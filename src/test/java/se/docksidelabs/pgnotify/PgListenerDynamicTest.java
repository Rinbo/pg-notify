package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Subscribing and unsubscribing while the listener is running. */
class PgListenerDynamicTest {

  private static final Duration POLL = Duration.ofMillis(100);
  private static final Duration STARTUP = Duration.ofSeconds(10);

  private final List<PgListener> listeners = new ArrayList<>();
  private final String appName = "pg-notify-dynamic-" + UUID.randomUUID();
  private Connection admin;

  @BeforeEach
  void connect() throws SQLException {
    admin = PostgresSupport.connect();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    listeners.forEach(PgListener::close);
    admin.close();
  }

  @Test
  void subscribesWhileRunningAndReceivesWithinOnePollTimeout() throws Exception {
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener = start(builder().listen(channel(), n -> {}));
    String late = channel();

    listener.listen(late, n -> received.add(n.payload()));
    awaitLastQuery("LISTEN " + ChannelNames.quote(late));
    PgNotifier.notify(admin, late, "hello");

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("hello");
  }

  @Test
  void unsubscribingTheLastHandlerIssuesUnlistenAndStopsDelivery() throws Exception {
    String kept = channel();
    String dropped = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener = start(builder().listen(kept, n -> received.add("kept")));
    Subscription sub = listener.listen(dropped, n -> received.add("dropped"));
    awaitLastQuery("LISTEN " + ChannelNames.quote(dropped));

    sub.close();
    awaitLastQuery("UNLISTEN " + ChannelNames.quote(dropped));
    PgNotifier.notify(admin, dropped, "x");
    PgNotifier.notify(admin, kept, "y");

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("kept");
    assertThat(received.poll(300, TimeUnit.MILLISECONDS)).isNull();
  }

  @Test
  void channelStaysSubscribedUntilItsLastHandlerIsRemoved() throws Exception {
    String channel = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener = start(builder().listen(channel(), n -> {}));
    Subscription first = listener.listen(channel, n -> received.add("first"));
    Subscription second = listener.listen(channel, n -> received.add("second"));
    awaitLastQuery("LISTEN " + ChannelNames.quote(channel));

    first.close();
    Thread.sleep(POLL.multipliedBy(3).toMillis());
    assertThat(lastQuery()).doesNotStartWith("UNLISTEN");
    PgNotifier.notify(admin, channel, "a");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("second");
    assertThat(received.poll(300, TimeUnit.MILLISECONDS)).isNull();

    second.close();
    awaitLastQuery("UNLISTEN " + ChannelNames.quote(channel));
    PgNotifier.notify(admin, channel, "b");
    assertThat(received.poll(300, TimeUnit.MILLISECONDS)).isNull();
  }

  @Test
  void closingASubscriptionTwiceRemovesOnlyItsOwnRegistration() throws Exception {
    String channel = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    NotificationHandler shared = n -> received.add(n.payload());
    PgListener listener = start(builder().listen(channel(), n -> {}));
    Subscription one = listener.listen(channel, shared);
    listener.listen(channel, shared);
    awaitLastQuery("LISTEN " + ChannelNames.quote(channel));

    one.close();
    one.close();
    PgNotifier.notify(admin, channel, "once");

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("once");
    assertThat(received.poll(300, TimeUnit.MILLISECONDS)).as("second registration intact").isNull();
  }

  @Test
  void subscriptionAddedWhileReconnectingIsSubscribedOnConnect() throws Exception {
    AtomicBoolean databaseUp = new AtomicBoolean(false);
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener =
        PgListener.builder(
                () -> {
                  if (!databaseUp.get()) {
                    throw new SQLException("simulated: database down");
                  }
                  return PostgresSupport.connect();
                })
            .pollTimeout(POLL)
            .backoff(Duration.ofMillis(20), Duration.ofMillis(100))
            .build();
    listeners.add(listener);
    listener.start();
    awaitState(listener, PgListener.State.RECONNECTING);
    String channel = channel();

    listener.listen(channel, n -> received.add(n.payload()));
    databaseUp.set(true);
    assertThat(listener.awaitListening(STARTUP)).isTrue();
    PgNotifier.notify(admin, channel, "after-connect");

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("after-connect");
  }

  @Test
  void listenAfterCloseThrowsAndSubscriptionCloseAfterCloseIsHarmless() throws Exception {
    PgListener listener = start(builder().listen(channel(), n -> {}));
    Subscription sub = listener.listen(channel(), n -> {});

    listener.close();

    assertThatIllegalStateException()
        .isThrownBy(() -> listener.listen(channel(), n -> {}))
        .withMessageContaining("closed");
    sub.close();
  }

  // ---- helpers ------------------------------------------------------------------------------

  private PgListener.Builder builder() {
    Properties props = PostgresSupport.properties();
    props.setProperty("ApplicationName", appName);
    return PgListener.builder(PostgresSupport.jdbcUrl(), props).pollTimeout(POLL);
  }

  private PgListener start(PgListener.Builder builder) throws InterruptedException {
    PgListener listener = builder.build();
    listeners.add(listener);
    listener.start();
    assertThat(listener.awaitListening(STARTUP)).as("listener started").isTrue();
    return listener;
  }

  private static String channel() {
    return "y_" + UUID.randomUUID().toString().replace("-", "");
  }

  private static void awaitState(PgListener listener, PgListener.State state)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (listener.state() != state && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(listener.state()).isEqualTo(state);
  }

  /** Last statement the listener session ran, as Postgres reports it. */
  private String lastQuery() throws SQLException {
    try (PreparedStatement ps =
        admin.prepareStatement("SELECT query FROM pg_stat_activity WHERE application_name = ?")) {
      ps.setString(1, appName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private void awaitLastQuery(String expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!expected.equals(lastQuery()) && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertThat(lastQuery()).isEqualTo(expected);
  }
}

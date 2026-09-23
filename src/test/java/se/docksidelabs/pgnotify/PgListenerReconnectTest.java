package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Reconnect, re-LISTEN, connection callbacks and the health check, against a real Postgres. */
class PgListenerReconnectTest {

  private static final Duration STARTUP = Duration.ofSeconds(10);

  /** Records every callback in order, plus the listener state seen inside onDisconnected. */
  private static final class RecordingListener implements ConnectionListener {
    final List<String> events = new CopyOnWriteArrayList<>();
    final BlockingQueue<ReconnectEvent> reconnects = new LinkedBlockingQueue<>();
    final BlockingQueue<Throwable> disconnects = new LinkedBlockingQueue<>();
    final AtomicInteger connected = new AtomicInteger();
    volatile PgListener listener;
    volatile PgListener.State stateInOnDisconnected;

    @Override
    public void onConnected() {
      connected.incrementAndGet();
      events.add("connected");
    }

    @Override
    public void onReconnected(ReconnectEvent event) {
      events.add("reconnected");
      reconnects.add(event);
    }

    @Override
    public void onDisconnected(Throwable cause) {
      events.add("disconnected");
      if (listener != null) {
        stateInOnDisconnected = listener.state();
      }
      disconnects.add(cause);
    }
  }

  private final List<PgListener> listeners = new ArrayList<>();
  private final String appName = "pg-notify-reconnect-" + UUID.randomUUID();
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
  void reconnectsAndReListensAfterBackendIsTerminated() throws Exception {
    String a = channel();
    String b = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    RecordingListener recorder = new RecordingListener();
    PgListener listener =
        builder()
            .connectionListener(recorder)
            .listen(a, n -> received.add(n.channel() + ":" + n.payload()))
            .listen(b, n -> received.add(n.channel() + ":" + n.payload()))
            .build();
    recorder.listener = listener;
    start(listener);
    int firstPid = backendPid();
    PgNotifier.notify(admin, a, "before");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(a + ":before");

    terminateBackend(firstPid);

    Throwable cause = recorder.disconnects.poll(10, TimeUnit.SECONDS);
    assertThat(cause).isInstanceOf(SQLException.class);
    assertThat(recorder.stateInOnDisconnected).isEqualTo(PgListener.State.RECONNECTING);
    assertThat(listener.awaitListening(STARTUP)).as("reconnected").isTrue();
    assertThat(backendPid()).isNotEqualTo(firstPid);

    ReconnectEvent event = recorder.reconnects.poll(5, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.attempts()).isGreaterThanOrEqualTo(1);
    assertThat(event.downtime()).isPositive();
    assertThat(event.disconnectedAt()).isBeforeOrEqualTo(event.reconnectedAt());
    assertThat(recorder.connected).hasValue(1);
    assertThat(recorder.events).startsWith("connected", "disconnected").endsWith("reconnected");

    PgNotifier.notify(admin, a, "after-a");
    PgNotifier.notify(admin, b, "after-b");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(a + ":after-a");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(b + ":after-b");
  }

  @Test
  void onReconnectShorthandReceivesTheEvent() throws Exception {
    BlockingQueue<ReconnectEvent> events = new LinkedBlockingQueue<>();
    PgListener listener = builder().onReconnect(events::add).listen(channel(), n -> {}).build();
    start(listener);

    terminateBackend(backendPid());

    assertThat(events.poll(10, TimeUnit.SECONDS)).isNotNull();
    assertThat(listener.awaitListening(STARTUP)).isTrue();
  }

  @Test
  void retriesAFailedInitialConnectionUntilTheDatabaseIsReachable() throws Exception {
    AtomicBoolean databaseUp = new AtomicBoolean(false);
    AtomicInteger attempts = new AtomicInteger();
    RecordingListener recorder = new RecordingListener();
    String channel = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener =
        PgListener.builder(
                () -> {
                  attempts.incrementAndGet();
                  if (!databaseUp.get()) {
                    throw new SQLException("simulated: database down");
                  }
                  return PostgresSupport.connect();
                })
            .backoff(Duration.ofMillis(20), Duration.ofMillis(100))
            .pollTimeout(Duration.ofMillis(100))
            .connectionListener(recorder)
            .listen(channel, n -> received.add(n.payload()))
            .build();
    recorder.listener = listener;
    listeners.add(listener);
    listener.start();

    assertThat(listener.awaitListening(Duration.ofMillis(500))).isFalse();
    assertThat(listener.state()).isEqualTo(PgListener.State.RECONNECTING);
    assertThat(recorder.disconnects.size()).as("one onDisconnected per attempt").isGreaterThan(1);
    assertThat(recorder.stateInOnDisconnected).isEqualTo(PgListener.State.RECONNECTING);

    databaseUp.set(true);

    assertThat(listener.awaitListening(STARTUP)).isTrue();
    assertThat(recorder.connected)
        .as("first success is onConnected, not onReconnected")
        .hasValue(1);
    assertThat(recorder.reconnects).isEmpty();
    assertThat(attempts.get()).isGreaterThan(1);
    PgNotifier.notify(admin, channel, "finally");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("finally");
  }

  @Test
  void closeDuringBackoffReturnsPromptly() throws Exception {
    PgListener listener =
        PgListener.builder(
                () -> {
                  throw new SQLException("simulated: always down");
                })
            .backoff(Duration.ofSeconds(30), Duration.ofSeconds(30))
            .listen(channel(), n -> {})
            .build();
    listeners.add(listener);
    listener.start();
    awaitState(listener, PgListener.State.RECONNECTING);

    long start = System.nanoTime();
    listener.close();
    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

    assertThat(elapsedMillis).isLessThan(2_000);
    assertThat(listener.state()).isEqualTo(PgListener.State.CLOSED);
    assertThat(
            Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().endsWith("-listener"))
                .map(Thread::getName))
        .isEmpty();
  }

  @Test
  void probesAnIdleConnectionWithSelectOne() throws Exception {
    PgListener listener =
        builder().healthCheckInterval(Duration.ofMillis(200)).listen(channel(), n -> {}).build();
    start(listener);
    assertThat(lastQuery()).startsWith("LISTEN");

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!"SELECT 1".equals(lastQuery()) && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }

    assertThat(lastQuery()).isEqualTo("SELECT 1");
    assertThat(listener.state()).isEqualTo(PgListener.State.LISTENING);
  }

  @Test
  void callbackExceptionsAreIsolated() throws Exception {
    BlockingQueue<String> order = new LinkedBlockingQueue<>();
    PgListener listener =
        builder()
            .connectionListener(
                new ConnectionListener() {
                  @Override
                  public void onConnected() {
                    throw new IllegalStateException("bad listener");
                  }
                })
            .connectionListener(
                new ConnectionListener() {
                  @Override
                  public void onConnected() {
                    order.add("second");
                  }
                })
            .listen(channel(), n -> {})
            .build();
    start(listener);

    assertThat(order.poll(5, TimeUnit.SECONDS)).isEqualTo("second");
  }

  // ---- helpers ------------------------------------------------------------------------------

  private PgListener.Builder builder() {
    Properties props = PostgresSupport.properties();
    props.setProperty("ApplicationName", appName);
    return PgListener.builder(PostgresSupport.jdbcUrl(), props)
        .pollTimeout(Duration.ofMillis(100))
        .backoff(Duration.ofMillis(20), Duration.ofMillis(200));
  }

  private void start(PgListener listener) throws InterruptedException {
    listeners.add(listener);
    listener.start();
    assertThat(listener.awaitListening(STARTUP)).as("listener started").isTrue();
  }

  private static String channel() {
    return "r_" + UUID.randomUUID().toString().replace("-", "");
  }

  private static void awaitState(PgListener listener, PgListener.State state)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (listener.state() != state && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(listener.state()).isEqualTo(state);
  }

  private int backendPid() throws SQLException {
    try (PreparedStatement ps =
        admin.prepareStatement("SELECT pid FROM pg_stat_activity WHERE application_name = ?")) {
      ps.setString(1, appName);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("listener session present").isTrue();
        int pid = rs.getInt(1);
        assertThat(rs.next()).as("exactly one listener session").isFalse();
        return pid;
      }
    }
  }

  private String lastQuery() throws SQLException {
    try (PreparedStatement ps =
        admin.prepareStatement("SELECT query FROM pg_stat_activity WHERE application_name = ?")) {
      ps.setString(1, appName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private void terminateBackend(int pid) throws SQLException {
    try (PreparedStatement ps = admin.prepareStatement("SELECT pg_terminate_backend(?)")) {
      ps.setInt(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next() && rs.getBoolean(1)).as("terminated").isTrue();
      }
    }
  }
}

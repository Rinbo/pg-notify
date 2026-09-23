package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PgListenerTest {

  private static final String APP_NAME = "pg-notify-test";
  private static final Duration STARTUP = Duration.ofSeconds(10);

  private final List<PgListener> listeners = new ArrayList<>();
  private final List<ExecutorService> executors = new ArrayList<>();
  private Connection publisher;

  @BeforeEach
  void connect() throws SQLException {
    publisher = PostgresSupport.connect();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    listeners.forEach(PgListener::close);
    executors.forEach(ExecutorService::shutdownNow);
    publisher.close();
  }

  @Test
  void deliversNotificationsToTheHandler() throws Exception {
    String channel = channel();
    BlockingQueue<Notification> received = new LinkedBlockingQueue<>();
    start(builder().listen(channel, received::add));

    PgNotifier.notify(publisher, channel, "hello");
    PgNotifier.notify(publisher, channel, null);

    Notification first = received.poll(5, TimeUnit.SECONDS);
    assertThat(first).isNotNull();
    assertThat(first.channel()).isEqualTo(channel);
    assertThat(first.payload()).isEqualTo("hello");
    assertThat(first.senderPid()).isEqualTo(backendPid(publisher));
    Notification second = received.poll(5, TimeUnit.SECONDS);
    assertThat(second).isNotNull();
    assertThat(second.payload()).isEmpty();
  }

  @Test
  void routesEachChannelToItsOwnHandlers() throws Exception {
    String a = channel();
    String b = channel();
    BlockingQueue<String> gotA = new LinkedBlockingQueue<>();
    BlockingQueue<String> gotB = new LinkedBlockingQueue<>();
    start(builder().listen(a, n -> gotA.add(n.payload())).listen(b, n -> gotB.add(n.payload())));

    PgNotifier.notify(publisher, a, "for-a");
    PgNotifier.notify(publisher, b, "for-b");

    assertThat(gotA.poll(5, TimeUnit.SECONDS)).isEqualTo("for-a");
    assertThat(gotB.poll(5, TimeUnit.SECONDS)).isEqualTo("for-b");
    assertThat(gotA.poll(300, TimeUnit.MILLISECONDS)).isNull();
    assertThat(gotB.poll(300, TimeUnit.MILLISECONDS)).isNull();
  }

  @Test
  void preservesOrderPerChannelWhileChannelsProceedIndependently() throws Exception {
    int count = 40;
    String slow = channel();
    String fast = channel();
    List<Integer> slowSeen = Collections.synchronizedList(new ArrayList<>());
    List<Integer> fastSeen = Collections.synchronizedList(new ArrayList<>());
    AtomicLong slowFinishedAt = new AtomicLong();
    AtomicLong fastFinishedAt = new AtomicLong();
    ExecutorService pool = pool(4);

    start(
        builder()
            .handlerExecutor(pool)
            .listen(
                slow,
                n -> {
                  Thread.sleep(15);
                  slowSeen.add(Integer.parseInt(n.payload()));
                  if (slowSeen.size() == count) {
                    slowFinishedAt.set(System.nanoTime());
                  }
                })
            .listen(
                fast,
                n -> {
                  fastSeen.add(Integer.parseInt(n.payload()));
                  if (fastSeen.size() == count) {
                    fastFinishedAt.set(System.nanoTime());
                  }
                }));

    publisher.setAutoCommit(false);
    for (int i = 0; i < count; i++) {
      PgNotifier.notify(publisher, slow, Integer.toString(i));
      PgNotifier.notify(publisher, fast, Integer.toString(i));
    }
    publisher.commit();

    awaitSize(slowSeen, count);
    awaitSize(fastSeen, count);
    List<Integer> expected = IntStream.range(0, count).boxed().toList();
    assertThat(slowSeen).containsExactlyElementsOf(expected);
    assertThat(fastSeen).containsExactlyElementsOf(expected);
    assertThat(fastFinishedAt.get())
        .as("fast channel finished before slow one")
        .isLessThan(slowFinishedAt.get());
  }

  @Test
  void isolatesHandlerFailures() throws Exception {
    String channel = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    start(
        builder()
            .listen(
                channel,
                n -> {
                  switch (n.payload()) {
                    case "boom" -> throw new IllegalStateException("boom");
                    case "err" -> throw new AssertionError("err");
                    default -> received.add(n.payload());
                  }
                }));

    for (String p : List.of("boom", "ok1", "err", "ok2")) {
      PgNotifier.notify(publisher, channel, p);
    }

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("ok1");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("ok2");
  }

  @Test
  void runsMultipleHandlersOnOneChannelInRegistrationOrder() throws Exception {
    String channel = channel();
    List<String> seen = Collections.synchronizedList(new ArrayList<>());
    start(
        builder()
            .listen(channel, n -> seen.add("h1:" + n.payload()))
            .listen(channel, n -> seen.add("h2:" + n.payload())));

    PgNotifier.notify(publisher, channel, "a");
    PgNotifier.notify(publisher, channel, "b");

    awaitSize(seen, 4);
    assertThat(seen).containsExactly("h1:a", "h2:a", "h1:b", "h2:b");
  }

  @Test
  void subscriptionClosedBeforeStartIsNotListened() throws Exception {
    String kept = channel();
    String dropped = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener = builder().listen(kept, n -> received.add(n.channel())).build();
    listeners.add(listener);
    Subscription sub = listener.listen(dropped, n -> received.add(n.channel()));
    sub.close();
    sub.close();
    listener.start();
    assertThat(listener.awaitListening(STARTUP)).isTrue();

    PgNotifier.notify(publisher, dropped, "x");
    PgNotifier.notify(publisher, kept, "y");

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(kept);
    assertThat(received.poll(300, TimeUnit.MILLISECONDS)).isNull();
  }

  @Test
  void startIsIdempotentAndCloseIsTerminal() throws Exception {
    PgListener listener = start(builder().listen(channel(), n -> {}));

    listener.start();
    assertThat(listener.state()).isEqualTo(PgListener.State.LISTENING);

    listener.close();
    assertThatIllegalStateException().isThrownBy(listener::start).withMessageContaining("closed");
    assertThatIllegalStateException()
        .isThrownBy(() -> listener.listen(channel(), n -> {}))
        .withMessageContaining("closed");
  }

  @Test
  void awaitListeningRequiresStart() {
    PgListener listener = builder().listen(channel(), n -> {}).build();
    listeners.add(listener);

    assertThatIllegalStateException().isThrownBy(() -> listener.awaitListening(STARTUP));
  }

  @Test
  void shutsDownCleanly() throws Exception {
    String channel = channel();
    PgListener listener = start(builder().listen(channel, n -> {}));
    awaitSessionCount(1);

    listener.close();

    assertThat(listener.state()).isEqualTo(PgListener.State.CLOSED);
    assertThat(listener.awaitListening(Duration.ofSeconds(1))).isFalse();
    assertThat(liveThreadsNamed("pg-notify-")).isEmpty();
    awaitSessionCount(0);

    listener.close(); // no-op
    assertThat(listener.state()).isEqualTo(PgListener.State.CLOSED);
  }

  @Test
  void closeReturnsWhileTheProviderIsStillOpening() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch opening = new CountDownLatch(1);
    ConnectionProvider stuck =
        () -> {
          opening.countDown();
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted", e);
          }
          return PostgresSupport.connect();
        };
    PgListener listener =
        PgListener.builder(stuck)
            .pollTimeout(Duration.ofMillis(100))
            .listen(channel(), n -> {})
            .build();
    listeners.add(listener);
    listener.start();
    assertThat(opening.await(5, TimeUnit.SECONDS)).isTrue();

    long before = System.nanoTime();
    listener.close();
    Duration took = Duration.ofNanos(System.nanoTime() - before);
    assertThat(took).as("close is bounded even though the provider hangs").isLessThan(STARTUP);
    assertThat(listener.state()).isEqualTo(PgListener.State.CLOSED);
    assertThat(liveThreadsNamed("pg-notify-"))
        .as("listener thread still in the provider")
        .isNotEmpty();

    release.countDown();
    awaitSessionCount(0);
    for (int i = 0; i < 50 && !liveThreadsNamed("pg-notify-").isEmpty(); i++) {
      Thread.sleep(100);
    }
    assertThat(liveThreadsNamed("pg-notify-"))
        .as("thread exits once the provider returns")
        .isEmpty();
  }

  @Test
  void closeFromInsideAHandlerDoesNotDeadlock() throws Exception {
    String channel = channel();
    PgListener[] holder = new PgListener[1];
    BlockingQueue<String> done = new LinkedBlockingQueue<>();
    holder[0] =
        builder()
            .listen(
                channel,
                n -> {
                  holder[0].close();
                  done.add("closed");
                })
            .build();
    listeners.add(holder[0]);
    holder[0].start();
    assertThat(holder[0].awaitListening(STARTUP)).isTrue();

    PgNotifier.notify(publisher, channel, "x");

    assertThat(done.poll(10, TimeUnit.SECONDS)).isEqualTo("closed");
    awaitSessionCount(0);
    assertThat(holder[0].state()).isEqualTo(PgListener.State.CLOSED);
  }

  @Test
  void doesNotShutDownASuppliedExecutor() throws Exception {
    ExecutorService pool = pool(1);
    PgListener listener = start(builder().handlerExecutor(pool).listen(channel(), n -> {}));

    listener.close();

    assertThat(pool.isShutdown()).isFalse();
  }

  @Test
  void aRejectingExecutorCostsTheNotificationButNotTheListener() throws Exception {
    String channel = channel();
    ExecutorService pool = pool(1);
    AtomicBoolean saturated = new AtomicBoolean(true);
    CountDownLatch rejected = new CountDownLatch(1);
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener =
        start(
            builder()
                .handlerExecutor(
                    task -> {
                      if (saturated.get()) {
                        rejected.countDown();
                        throw new RejectedExecutionException("simulated: executor saturated");
                      }
                      pool.execute(task);
                    })
                .listen(channel, n -> received.add(n.payload())));

    PgNotifier.notify(publisher, channel, "rejected");
    assertThat(rejected.await(5, TimeUnit.SECONDS)).as("executor saw the task").isTrue();
    saturated.set(false);
    PgNotifier.notify(publisher, channel, "delivered");

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("delivered");
    assertThat(received).isEmpty();
    assertThat(listener.state()).isEqualTo(PgListener.State.LISTENING);
  }

  @Test
  void worksWithAConnectionProvider() throws Exception {
    String channel = channel();
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    PgListener listener =
        PgListener.builder(PostgresSupport::connect)
            .pollTimeout(Duration.ofMillis(200))
            .listen(channel, n -> received.add(n.payload()))
            .build();
    listeners.add(listener);
    listener.start();
    assertThat(listener.awaitListening(STARTUP)).isTrue();

    PgNotifier.notify(publisher, channel, "via-supplier");

    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("via-supplier");
  }

  // ---- helpers ------------------------------------------------------------------------------

  private static PgListener.Builder builder() {
    Properties props = PostgresSupport.properties();
    props.setProperty("ApplicationName", APP_NAME);
    return PgListener.builder(PostgresSupport.jdbcUrl(), props).pollTimeout(Duration.ofMillis(200));
  }

  private PgListener start(PgListener.Builder builder) throws InterruptedException {
    PgListener listener = builder.build();
    listeners.add(listener);
    listener.start();
    assertThat(listener.awaitListening(STARTUP)).as("listener started").isTrue();
    return listener;
  }

  private ExecutorService pool(int threads) {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    executors.add(pool);
    return pool;
  }

  private static String channel() {
    return "c_" + UUID.randomUUID().toString().replace("-", "");
  }

  private static int backendPid(Connection c) throws SQLException {
    return c.unwrap(org.postgresql.PGConnection.class).getBackendPID();
  }

  private static void awaitSize(List<?> list, int size) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (list.size() < size && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(list).hasSize(size);
  }

  private static List<String> liveThreadsNamed(String prefix) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.isAlive() && t.getName().startsWith(prefix))
        .map(Thread::getName)
        .toList();
  }

  private int sessionCount() throws SQLException {
    try (PreparedStatement ps =
        publisher.prepareStatement(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = ?")) {
      ps.setString(1, APP_NAME);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private void awaitSessionCount(int expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (sessionCount() != expected && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertThat(sessionCount()).as("listener sessions in pg_stat_activity").isEqualTo(expected);
  }
}

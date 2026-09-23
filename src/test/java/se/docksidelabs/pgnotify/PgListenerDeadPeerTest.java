package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Timeout;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The failure Postgres cannot simulate: the peer goes silent without closing the socket. Nothing
 * arrives, nothing errors, and only the idle health check bounded by the network timeout can
 * notice. Toxiproxy's {@code timeout} toxic with a zero timeout holds all downstream data
 * indefinitely.
 */
class PgListenerDeadPeerTest {

  private static final Duration POLL = Duration.ofMillis(100);
  private static final Duration HEALTH_CHECK = Duration.ofMillis(500);
  private static final Duration NETWORK_TIMEOUT = Duration.ofSeconds(1);
  private static final Duration LOGIN_TIMEOUT = Duration.ofSeconds(2);

  private Connection admin;
  private PgListener listener;
  private Timeout toxic;

  @BeforeEach
  void connect() throws SQLException {
    admin = PostgresSupport.connect();
  }

  @AfterEach
  void cleanUp() throws Exception {
    if (toxic != null) {
      toxic.remove();
    }
    if (listener != null) {
      listener.close();
    }
    admin.close();
  }

  @Test
  void detectsASilentlyDeadPeerAndRecoversWhenTheNetworkReturns() throws Exception {
    String channel = "d_" + UUID.randomUUID().toString().replace("-", "");
    BlockingQueue<String> received = new LinkedBlockingQueue<>();
    BlockingQueue<Long> disconnectedAtNanos = new LinkedBlockingQueue<>();
    BlockingQueue<ReconnectEvent> reconnects = new LinkedBlockingQueue<>();
    AtomicInteger disconnects = new AtomicInteger();

    Properties props = PostgresSupport.properties();
    props.setProperty("loginTimeout", Long.toString(LOGIN_TIMEOUT.toSeconds()));
    listener =
        PgListener.builder(ToxiproxySupport.jdbcUrl(), props)
            .pollTimeout(POLL)
            .healthCheckInterval(HEALTH_CHECK)
            .networkTimeout(NETWORK_TIMEOUT)
            .backoff(Duration.ofMillis(100), Duration.ofMillis(300))
            .connectionListener(
                new ConnectionListener() {
                  @Override
                  public void onDisconnected(Throwable cause) {
                    disconnects.incrementAndGet();
                    disconnectedAtNanos.add(System.nanoTime());
                  }

                  @Override
                  public void onReconnected(ReconnectEvent event) {
                    reconnects.add(event);
                  }
                })
            .listen(channel, n -> received.add(n.payload()))
            .build();
    listener.start();
    assertThat(listener.awaitListening(Duration.ofSeconds(10))).isTrue();
    PgNotifier.notify(admin, channel, "through-proxy");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("through-proxy");

    long cutAt = System.nanoTime();
    toxic = ToxiproxySupport.proxy().toxics().timeout("silence", ToxicDirection.DOWNSTREAM, 0);

    Long noticedAt = disconnectedAtNanos.poll(10, TimeUnit.SECONDS);
    assertThat(noticedAt).as("dead peer noticed").isNotNull();
    Duration detection = Duration.ofNanos(noticedAt - cutAt);
    Duration bound = HEALTH_CHECK.plus(NETWORK_TIMEOUT).plus(POLL).plusSeconds(1);
    assertThat(detection)
        .as("detected within healthCheckInterval + networkTimeout")
        .isLessThan(bound);
    assertThat(listener.state()).isEqualTo(PgListener.State.RECONNECTING);

    // While the network is still broken, reconnect attempts hang until loginTimeout and fail.
    Thread.sleep(LOGIN_TIMEOUT.plusSeconds(1).toMillis());
    assertThat(listener.state()).isEqualTo(PgListener.State.RECONNECTING);
    assertThat(disconnects.get())
        .as("failed reconnect attempts also fire onDisconnected")
        .isGreaterThan(1);
    assertThat(reconnects).isEmpty();

    toxic.remove();
    toxic = null;

    assertThat(listener.awaitListening(Duration.ofSeconds(15))).as("recovered").isTrue();
    ReconnectEvent event = reconnects.poll(5, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.attempts()).isGreaterThan(1);
    assertThat(event.downtime()).isGreaterThan(LOGIN_TIMEOUT);
    PgNotifier.notify(admin, channel, "after-recovery");
    assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("after-recovery");
  }
}

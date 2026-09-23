package se.docksidelabs.pgnotify;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * A Toxiproxy in front of the shared Postgres, for tests that need to break the network in ways
 * Postgres itself cannot: silently dropping traffic, for example, rather than closing the socket.
 */
final class ToxiproxySupport {

  private static final int PROXY_PORT = 8666;

  private static final ToxiproxyContainer TOXIPROXY =
      new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.11.0")
          .withNetwork(PostgresSupport.NETWORK);

  private static final Proxy PROXY;

  static {
    TOXIPROXY.start();
    try {
      ToxiproxyClient client = new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort());
      PROXY =
          client.createProxy(
              "postgres", "0.0.0.0:" + PROXY_PORT, PostgresSupport.NETWORK_ALIAS + ":5432");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ToxiproxySupport() {}

  /** The proxy in front of Postgres; add toxics here. */
  static Proxy proxy() {
    return PROXY;
  }

  /** JDBC URL that reaches Postgres through the proxy. */
  static String jdbcUrl() {
    return "jdbc:postgresql://"
        + TOXIPROXY.getHost()
        + ":"
        + TOXIPROXY.getMappedPort(PROXY_PORT)
        + "/"
        + PostgresSupport.databaseName();
  }
}

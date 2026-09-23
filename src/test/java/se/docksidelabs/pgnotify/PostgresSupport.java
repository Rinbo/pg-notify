package se.docksidelabs.pgnotify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.testcontainers.containers.Network;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * One Postgres container shared by every test class in the JVM. Started on first use, stopped by
 * Testcontainers' Ryuk sidecar when the JVM exits.
 */
final class PostgresSupport {
  /** Shared so a proxy container can sit between a test and Postgres. */
  static final Network NETWORK = Network.newNetwork();

  /** Hostname of Postgres as seen by other containers on {@link #NETWORK}. */
  static final String NETWORK_ALIAS = "postgres";

  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:16-alpine")
          .withNetwork(NETWORK)
          .withNetworkAliases(NETWORK_ALIAS);

  static {
    POSTGRES.start();
  }

  private PostgresSupport() {}

  static String databaseName() {
    return POSTGRES.getDatabaseName();
  }

  static Connection connect() throws SQLException {
    return DriverManager.getConnection(jdbcUrl(), properties());
  }

  static String jdbcUrl() {
    return POSTGRES.getJdbcUrl();
  }

  static Properties properties() {
    Properties p = new Properties();
    p.setProperty("user", POSTGRES.getUsername());
    p.setProperty("password", POSTGRES.getPassword());
    return p;
  }
}

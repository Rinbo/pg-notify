package se.docksidelabs.pgnotify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * One Postgres container shared by every test class in the JVM. Started on first use, stopped by
 * Testcontainers' Ryuk sidecar when the JVM exits.
 */
final class PostgresSupport {
  private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

  static {
    POSTGRES.start();
  }

  private PostgresSupport() {}

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

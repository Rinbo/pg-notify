/*
 * Copyright 2026 Dockside Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.docksidelabs.pgnotify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opens the listener's dedicated connection and puts it in the state the listener needs: autocommit
 * on (Postgres holds notifications back while the receiving session is inside a transaction) and a
 * bounded network timeout so a dead peer cannot block a read forever.
 *
 * <p>In URL mode {@code tcpKeepAlive} and {@code ApplicationName} are set unless the caller's
 * properties already have them. In supplier mode the supplier is responsible for those.
 */
final class ConnectionFactory {

  private static final Logger log = LoggerFactory.getLogger(ConnectionFactory.class);

  @FunctionalInterface
  private interface Opener {
    Connection open() throws SQLException;
  }

  private final Opener opener;

  private ConnectionFactory(Opener opener) {
    this.opener = opener;
  }

  static ConnectionFactory forUrl(String jdbcUrl, Properties properties, String applicationName) {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Properties copy = new Properties();
    if (properties != null) {
      for (String name : properties.stringPropertyNames()) {
        copy.setProperty(name, properties.getProperty(name));
      }
    }
    copy.putIfAbsent("ApplicationName", applicationName);
    copy.putIfAbsent("tcpKeepAlive", "true");
    return new ConnectionFactory(() -> DriverManager.getConnection(jdbcUrl, copy));
  }

  static ConnectionFactory forSupplier(Supplier<Connection> supplier) {
    Objects.requireNonNull(supplier, "connectionSupplier");
    return new ConnectionFactory(
        () -> {
          Connection c;
          try {
            c = supplier.get();
          } catch (RuntimeException e) {
            throw new SQLException("connection supplier failed", e);
          }
          if (c == null) {
            throw new SQLException("connection supplier returned null");
          }
          return c;
        });
  }

  /** Opens and configures a connection. On any failure the connection is closed before rethrow. */
  Connection open(Duration networkTimeout) throws SQLException {
    Connection c = opener.open();
    try {
      if (c.isClosed()) {
        throw new SQLException("connection is already closed");
      }
      c.unwrap(PGConnection.class);
      c.setAutoCommit(true);
      c.setNetworkTimeout(Runnable::run, (int) networkTimeout.toMillis());
      return c;
    } catch (SQLException | RuntimeException e) {
      closeQuietly(c);
      throw e;
    }
  }

  static void closeQuietly(Connection c) {
    if (c == null) {
      return;
    }
    try {
      c.close();
    } catch (SQLException | RuntimeException e) {
      log.debug("Ignoring failure while closing connection", e);
    }
  }
}

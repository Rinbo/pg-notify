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
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Where the listener's raw connections come from: a JDBC URL with properties, or an application
 * supplier. Configuring the connection for listening is {@link ListenerSession}'s job.
 */
@FunctionalInterface
interface ConnectionFactory {

  String DEFAULT_APPLICATION_NAME = "pg-notify";

  /** Opens a new connection. Called once per connection attempt. */
  Connection open() throws SQLException;

  /**
   * Connects through {@link DriverManager}. Adds {@code tcpKeepAlive=true} and {@code
   * ApplicationName} unless the caller's properties already set them.
   */
  static ConnectionFactory forUrl(String jdbcUrl, Properties properties) {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Properties effective = new Properties();
    if (properties != null) {
      for (String name : properties.stringPropertyNames()) {
        effective.setProperty(name, properties.getProperty(name));
      }
    }
    effective.putIfAbsent("ApplicationName", DEFAULT_APPLICATION_NAME);
    effective.putIfAbsent("tcpKeepAlive", "true");
    return () -> DriverManager.getConnection(jdbcUrl, effective);
  }

  /**
   * Delegates to the supplier; a thrown {@link RuntimeException} or a {@code null} becomes a
   * failure.
   */
  static ConnectionFactory forSupplier(Supplier<Connection> supplier) {
    Objects.requireNonNull(supplier, "connectionSupplier");
    return () -> {
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
    };
  }
}

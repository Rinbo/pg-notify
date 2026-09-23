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

import java.sql.DriverManager;
import java.util.Objects;
import java.util.Properties;

/** The library's own {@link ConnectionProvider} implementations. */
final class ConnectionProviders {

  static final String DEFAULT_APPLICATION_NAME = "pg-notify";

  /** A hung handshake would stall the reconnect loop forever; pgjdbc's default is no limit. */
  static final String DEFAULT_LOGIN_TIMEOUT_SECONDS = "10";

  private ConnectionProviders() {}

  /**
   * Connects through {@link DriverManager}. Adds {@code tcpKeepAlive=true}, {@code ApplicationName}
   * and a {@code loginTimeout} unless the caller's properties already set them.
   */
  static ConnectionProvider forUrl(String jdbcUrl, Properties properties) {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Properties effective = new Properties();
    if (properties != null) {
      for (String name : properties.stringPropertyNames()) {
        effective.setProperty(name, properties.getProperty(name));
      }
    }
    effective.putIfAbsent("ApplicationName", DEFAULT_APPLICATION_NAME);
    effective.putIfAbsent("tcpKeepAlive", "true");
    effective.putIfAbsent("loginTimeout", DEFAULT_LOGIN_TIMEOUT_SECONDS);
    return () -> DriverManager.getConnection(jdbcUrl, effective);
  }
}

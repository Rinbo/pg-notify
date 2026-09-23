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
import java.sql.SQLException;

/**
 * Opens the dedicated connection a {@link PgListener} runs on.
 *
 * <p>Called once when the listener starts and again after every connection loss. Each call must
 * return a new pgjdbc connection that nothing else will use. The listener closes it when done.
 *
 * <p><b>Do not return a connection from your application's pool.</b> The listener keeps its
 * connection for its whole lifetime, so a pool would see a permanent leak, and any pool
 * housekeeping that recycles the connection silently drops the subscriptions. Open an unpooled
 * connection from the same coordinates instead:
 *
 * <pre>{@code
 * PgListener.builder(() -> DriverManager.getConnection(url, user, password))
 * }</pre>
 *
 * <p>The listener sets autocommit and a network timeout on the connection itself. Enabling TCP
 * keepalive ({@code tcpKeepAlive=true} in pgjdbc) is the provider's responsibility.
 */
@FunctionalInterface
public interface ConnectionProvider {

  /**
   * Opens a new connection.
   *
   * @throws SQLException if the connection cannot be opened; the listener treats this as a
   *     connection failure and retries according to its reconnect policy
   */
  Connection open() throws SQLException;
}

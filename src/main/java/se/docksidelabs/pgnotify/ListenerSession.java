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
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One connected listening session: the only class that talks JDBC on the listener's connection.
 *
 * <p>Opening puts the connection in the state listening needs: autocommit on, because Postgres
 * holds notifications back while the receiving session is inside a transaction, and a bounded
 * network timeout so a dead peer cannot block a read forever. All methods except {@link #abort()}
 * must be called from the thread that owns the session.
 */
final class ListenerSession implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ListenerSession.class);

  private final Connection connection;
  private final PGConnection pg;

  private ListenerSession(Connection connection, PGConnection pg) {
    this.connection = connection;
    this.pg = pg;
  }

  /** Opens and configures a session. On failure the connection is closed before the rethrow. */
  static ListenerSession open(ConnectionFactory factory, Duration networkTimeout)
      throws SQLException {
    Connection c = factory.open();
    try {
      if (c.isClosed()) {
        throw new SQLException("connection is already closed");
      }
      PGConnection pg = c.unwrap(PGConnection.class);
      c.setAutoCommit(true);
      c.setNetworkTimeout(Runnable::run, (int) networkTimeout.toMillis());
      return new ListenerSession(c, pg);
    } catch (SQLException | RuntimeException e) {
      closeQuietly(c);
      throw e;
    }
  }

  /** Issues {@code LISTEN} for each channel. Names must already be validated. */
  void listen(Collection<String> channels) throws SQLException {
    try (Statement st = connection.createStatement()) {
      for (String channel : channels) {
        st.execute("LISTEN " + ChannelNames.quote(channel));
      }
    }
  }

  /** Blocks up to {@code timeout} for notifications. Returns an empty list on timeout. */
  List<Notification> poll(Duration timeout) throws SQLException {
    PGNotification[] raw = pg.getNotifications((int) timeout.toMillis());
    if (raw == null || raw.length == 0) {
      return List.of();
    }
    List<Notification> out = new ArrayList<>(raw.length);
    for (PGNotification n : raw) {
      out.add(new Notification(n.getName(), n.getParameter(), n.getPID()));
    }
    return out;
  }

  int backendPid() {
    return pg.getBackendPID();
  }

  /** Closes the socket from any thread, unblocking a poll in progress on the owning thread. */
  void abort() {
    try {
      connection.abort(Runnable::run);
    } catch (SQLException | RuntimeException e) {
      log.debug("Ignoring failure while aborting connection", e);
    }
  }

  @Override
  public void close() {
    closeQuietly(connection);
  }

  private static void closeQuietly(Connection c) {
    try {
      c.close();
    } catch (SQLException | RuntimeException e) {
      log.debug("Ignoring failure while closing connection", e);
    }
  }
}

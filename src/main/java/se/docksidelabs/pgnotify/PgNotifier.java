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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Sends notifications with {@code SELECT pg_notify(?, ?)} on a connection the caller provides.
 *
 * <p>Because the statement runs on the caller's connection, it takes part in the caller's
 * transaction: Postgres delivers the notification when that transaction commits and discards it on
 * rollback. Run it in the same transaction as the change it announces, and listeners never see a
 * notification for data that was not committed.
 *
 * <p>Unlike the listener side, publishing works fine on pooled connections and behind PgBouncer in
 * any pooling mode, because nothing about it is session state.
 *
 * <p>Channel names are case-sensitive here and on the listener side. {@code notify(c, "Foo", p)}
 * reaches {@code PgListener.listen("Foo", ...)} but not an unquoted {@code LISTEN Foo} issued from
 * psql, which Postgres folds to {@code foo}.
 */
public final class PgNotifier {

  /**
   * The longest payload Postgres accepts, in bytes. The server requires the payload to be shorter
   * than 8000 bytes in the server encoding; this library measures UTF-8.
   */
  public static final int MAX_PAYLOAD_BYTES = 7999;

  private static final String SQL = "SELECT pg_notify(?, ?)";

  private PgNotifier() {
    throw new AssertionError("Not for instantiation");
  }

  /**
   * Sends a notification on {@code channel} using {@code connection}.
   *
   * <p>Within a single transaction Postgres collapses notifications with an identical channel and
   * payload into one delivery.
   *
   * @param connection the caller's connection; not closed or otherwise altered by this method
   * @param channel channel name: non-empty, at most 63 bytes of UTF-8, no control characters
   * @param payload payload, or {@code null} for an empty payload
   * @throws IllegalArgumentException if the channel name is invalid
   * @throws PayloadTooLargeException if the payload exceeds {@link #MAX_PAYLOAD_BYTES}
   * @throws SQLException if the statement fails
   */
  public static void notify(Connection connection, String channel, String payload)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");

    ChannelNames.validate(channel);
    String body = payload == null ? "" : payload;

    int bytes = body.getBytes(StandardCharsets.UTF_8).length;
    if (bytes > MAX_PAYLOAD_BYTES) {
      throw new PayloadTooLargeException(bytes, MAX_PAYLOAD_BYTES);
    }

    try (PreparedStatement ps = connection.prepareStatement(SQL)) {
      ps.setString(1, channel);
      ps.setString(2, body);
      ps.execute();
    }
  }
}

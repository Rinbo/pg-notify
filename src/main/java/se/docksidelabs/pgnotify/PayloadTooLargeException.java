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

import java.io.Serial;

/**
 * Thrown by {@link PgNotifier#notify} when a payload is too long for Postgres to accept.
 *
 * <p>Postgres requires a notification payload to be shorter than 8000 bytes in the server encoding.
 * This library measures the payload as UTF-8 and assumes the database uses a UTF-8 {@code
 * server_encoding}. The check happens before any SQL is sent, so the caller's transaction is
 * untouched.
 */
public final class PayloadTooLargeException extends IllegalArgumentException {

  @Serial private static final long serialVersionUID = 1L;

  private final int actualBytes;
  private final int maxBytes;

  PayloadTooLargeException(int actualBytes, int maxBytes) {
    super(
        "notification payload is "
            + actualBytes
            + " bytes of UTF-8; Postgres allows at most "
            + maxBytes);
    this.actualBytes = actualBytes;
    this.maxBytes = maxBytes;
  }

  /** Length of the rejected payload in bytes of UTF-8. */
  public int actualBytes() {
    return actualBytes;
  }

  /** The limit that was exceeded, {@link PgNotifier#MAX_PAYLOAD_BYTES}. */
  public int maxBytes() {
    return maxBytes;
  }
}

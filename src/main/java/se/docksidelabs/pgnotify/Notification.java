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

import java.util.Objects;

/**
 * One notification as received by a {@link PgListener}.
 *
 * @param channel the channel it arrived on, exactly as registered with {@link PgListener#listen}
 * @param payload the payload; never {@code null}, empty when the sender gave none
 * @param senderPid backend process id of the session that ran {@code NOTIFY}
 */
public record Notification(String channel, String payload, int senderPid) {

  /** Canonical constructor; normalises a {@code null} payload to the empty string. */
  public Notification {
    Objects.requireNonNull(channel, "channel");
    if (payload == null) {
      payload = "";
    }
  }
}

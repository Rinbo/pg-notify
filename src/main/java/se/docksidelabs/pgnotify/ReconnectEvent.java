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

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Describes a recovered connection, see {@link ConnectionListener#onReconnected}.
 *
 * @param disconnectedAt when the previous connection was lost
 * @param reconnectedAt when the new connection was subscribed to every channel
 * @param attempts how many connection attempts it took, at least 1
 */
public record ReconnectEvent(Instant disconnectedAt, Instant reconnectedAt, int attempts) {

  /** Validates the fields. */
  public ReconnectEvent {
    Objects.requireNonNull(disconnectedAt, "disconnectedAt");
    Objects.requireNonNull(reconnectedAt, "reconnectedAt");
    if (attempts < 1) {
      throw new IllegalArgumentException("attempts must be at least 1, was " + attempts);
    }
  }

  /** The window during which notifications were lost. */
  public Duration downtime() {
    return Duration.between(disconnectedAt, reconnectedAt);
  }
}

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
import java.util.Objects;

/**
 * Timing configuration for a listener. Immutable; validated on construction.
 *
 * @param pollTimeout how long one blocking read for notifications waits before the loop checks for
 *     shutdown
 * @param networkTimeout socket read timeout for everything on the connection except the poll
 * @param shutdownTimeout how long {@code close()} waits for queued handler work before interrupting
 */
record ListenerConfig(Duration pollTimeout, Duration networkTimeout, Duration shutdownTimeout) {

  static final ListenerConfig DEFAULTS =
      new ListenerConfig(Duration.ofSeconds(1), Duration.ofSeconds(10), Duration.ofSeconds(10));

  ListenerConfig {
    requireMillisRange(pollTimeout, "pollTimeout");
    requireMillisRange(networkTimeout, "networkTimeout");
    requireMillisRange(shutdownTimeout, "shutdownTimeout");
  }

  ListenerConfig withPollTimeout(Duration d) {
    return new ListenerConfig(d, networkTimeout, shutdownTimeout);
  }

  ListenerConfig withNetworkTimeout(Duration d) {
    return new ListenerConfig(pollTimeout, d, shutdownTimeout);
  }

  ListenerConfig withShutdownTimeout(Duration d) {
    return new ListenerConfig(pollTimeout, networkTimeout, d);
  }

  /** JDBC and pgjdbc take timeouts as {@code int} milliseconds, so that is the legal range. */
  private static void requireMillisRange(Duration d, String name) {
    Objects.requireNonNull(d, name);
    long millis = d.toMillis();
    if (millis <= 0 || millis > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          name + " must be between 1 ms and " + Integer.MAX_VALUE + " ms, was " + d);
    }
  }
}

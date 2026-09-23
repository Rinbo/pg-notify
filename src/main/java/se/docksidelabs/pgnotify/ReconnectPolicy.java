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
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with full jitter: the delay before attempt {@code n+1} is uniformly random in
 * {@code [0, min(max, initial * 2^(n-1))]}, where {@code n} is the number of consecutive failures.
 *
 * <p>Full jitter spreads reconnecting clients across the whole window, so a fleet that lost the
 * same database does not hammer it in lockstep when it comes back.
 */
final class ReconnectPolicy {

  private final long initialMillis;
  private final long maxMillis;
  private final RandomGenerator random;

  ReconnectPolicy(Duration initial, Duration max, RandomGenerator random) {
    this.initialMillis = initial.toMillis();
    this.maxMillis = max.toMillis();
    this.random = random;
  }

  /**
   * Delay to wait after {@code failures} consecutive failures.
   *
   * @param failures at least 1
   */
  Duration delay(int failures) {
    if (failures < 1) {
      throw new IllegalArgumentException("failures must be at least 1, was " + failures);
    }
    return Duration.ofMillis(random.nextLong(cap(failures) + 1));
  }

  /** {@code min(max, initial * 2^(failures-1))}, without overflowing. */
  long cap(int failures) {
    long cap = initialMillis;
    for (int i = 1; i < failures && cap < maxMillis; i++) {
      cap <<= 1;
    }
    return Math.min(cap, maxMillis);
  }
}

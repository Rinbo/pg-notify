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

/**
 * Validation and quoting of notification channel names.
 *
 * <p>{@code LISTEN} and {@code UNLISTEN} take an identifier, not a parameter, so the channel name
 * has to be spliced into SQL text. We always emit it as a quoted identifier with embedded double
 * quotes doubled, which makes any string safe to splice. Quoting also preserves the case, so a
 * channel registered here matches {@code pg_notify('Name', ...)} byte for byte.
 *
 * <p>Length is limited to 63 bytes of UTF-8 ({@code NAMEDATALEN - 1}). Postgres silently truncates
 * longer identifiers in {@code LISTEN}, which would subscribe to a different name than the one
 * passed to {@code pg_notify}, so we reject them up front.
 */
final class ChannelNames {

  /** {@code NAMEDATALEN - 1}: the longest identifier Postgres keeps intact. */
  static final int MAX_BYTES = 63;

  private ChannelNames() {
    throw new AssertionError("Not for instantiation");
  }

  /**
   * Validates a channel name.
   *
   * @return the same name for chaining
   * @throws IllegalArgumentException if the name is null, empty, longer than {@value #MAX_BYTES}
   *     bytes of UTF-8, contains control characters, or is not valid Unicode
   */
  static String validate(String name) {
    if (name == null) {
      throw new IllegalArgumentException("channel name must not be null");
    }

    if (name.isEmpty()) {
      throw new IllegalArgumentException("channel name must not be empty");
    }

    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isISOControl(c)) {
        throw new IllegalArgumentException(
            "channel name must not contain control characters (found U+"
                + String.format("%04X", (int) c)
                + " at index "
                + i
                + ")");
      }
    }

    if (!StandardCharsets.UTF_8.newEncoder().canEncode(name)) {
      throw new IllegalArgumentException("channel name is not valid Unicode (unpaired surrogate)");
    }

    int bytes = name.getBytes(StandardCharsets.UTF_8).length;
    if (bytes > MAX_BYTES) {
      throw new IllegalArgumentException(
          "channel name is "
              + bytes
              + " bytes of UTF-8; Postgres identifiers are limited to "
              + MAX_BYTES
              + " bytes and longer names are silently truncated");
    }

    return name;
  }

  /** Returns the name as a quoted SQL identifier. The name must already be validated. */
  static String quote(String validName) {
    StringBuilder sb = new StringBuilder(validName.length() + 2);
    sb.append('"');
    for (int i = 0; i < validName.length(); i++) {
      char c = validName.charAt(i);
      if (c == '"') {
        sb.append('"');
      }
      sb.append(c);
    }
    return sb.append('"').toString();
  }
}

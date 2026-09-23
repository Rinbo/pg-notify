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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Which handlers are registered on which channels.
 *
 * <p>Thread-safe. Every method is guarded by the instance monitor and every collection handed out
 * is a snapshot, so callers never observe a concurrent modification. Channel names are validated on
 * the way in, so anything stored here is safe to splice into {@code LISTEN}.
 */
final class HandlerRegistry {

  private final Map<String, List<NotificationHandler>> handlers = new LinkedHashMap<>();

  /**
   * Registers a handler.
   *
   * @return {@code true} if this is the first handler on the channel
   * @throws IllegalArgumentException if the channel name is invalid
   */
  synchronized boolean add(String channel, NotificationHandler handler) {
    ChannelNames.validate(channel);
    Objects.requireNonNull(handler, "handler");
    List<NotificationHandler> list = handlers.computeIfAbsent(channel, c -> new ArrayList<>());
    list.add(handler);
    return list.size() == 1;
  }

  /**
   * Removes a handler. Unknown handlers are ignored.
   *
   * @return {@code true} if the channel has no handlers left and was removed
   */
  synchronized boolean remove(String channel, NotificationHandler handler) {
    List<NotificationHandler> list = handlers.get(channel);
    if (list == null || !list.remove(handler) || !list.isEmpty()) {
      return false;
    }
    handlers.remove(channel);
    return true;
  }

  /** Handlers for a channel in registration order, or an empty list. Snapshot. */
  synchronized List<NotificationHandler> handlersFor(String channel) {
    List<NotificationHandler> list = handlers.get(channel);
    return list == null ? List.of() : List.copyOf(list);
  }

  /** Channels with at least one handler, in first-registration order. Snapshot. */
  synchronized Set<String> channels() {
    return Set.copyOf(handlers.keySet());
  }

  synchronized int channelCount() {
    return handlers.size();
  }
}

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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * {@code LISTEN}/{@code UNLISTEN} statements the listener thread still has to issue on the current
 * session, queued by {@link PgListener#listen} and {@link Subscription#close} from other threads.
 *
 * <p>The {@link HandlerRegistry} is the source of truth for which channels should be subscribed;
 * this queue only carries the deltas to the live session. On every (re)connect the loop discards it
 * and subscribes from the registry snapshot instead.
 */
final class ChannelChanges {

  enum Kind {
    LISTEN,
    UNLISTEN
  }

  record Change(Kind kind, String channel) {}

  private final Queue<Change> queue = new ConcurrentLinkedQueue<>();

  void listen(String channel) {
    queue.add(new Change(Kind.LISTEN, channel));
  }

  void unlisten(String channel) {
    queue.add(new Change(Kind.UNLISTEN, channel));
  }

  /** Removes and returns everything queued so far, in order. */
  List<Change> drain() {
    List<Change> out = new ArrayList<>();
    for (Change c = queue.poll(); c != null; c = queue.poll()) {
      out.add(c);
    }
    return out;
  }
}

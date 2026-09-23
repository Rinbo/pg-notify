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
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs handlers on a shared executor while keeping each channel strictly serial.
 *
 * <p>Each channel has its own queue. At most one task per channel is ever handed to the executor at
 * a time; when it finishes, the next one for that channel is submitted. Channels therefore proceed
 * independently on a multithreaded executor, and within a channel notifications are handled in
 * arrival order regardless of how many threads the executor has.
 *
 * <p>Queues are unbounded. Delivery is at-most-once anyway, and blocking the listener thread would
 * only move the backlog into Postgres' own notification queue. For the same reason an executor that
 * rejects work costs notifications, never the listener: the channel's queue is dropped and logged.
 */
final class SerialDispatcher {

  private static final Logger log = LoggerFactory.getLogger(SerialDispatcher.class);

  private final Executor executor;
  private final Map<String, ChannelQueue> queues = new ConcurrentHashMap<>();

  SerialDispatcher(Executor executor) {
    this.executor = executor;
  }

  /**
   * Queues {@code notification} for delivery to {@code handlers}, in list order. Never throws: if
   * the executor rejects the work, the channel's queued notifications are dropped with a warning.
   */
  void dispatch(Notification notification, List<NotificationHandler> handlers) {
    queues
        .computeIfAbsent(notification.channel(), ChannelQueue::new)
        .submit(() -> deliver(notification, handlers));
  }

  private static void deliver(Notification n, List<NotificationHandler> handlers) {
    for (NotificationHandler handler : handlers) {
      try {
        handler.handle(n);
      } catch (VirtualMachineError e) {
        throw e;
      } catch (Throwable t) {
        log.warn(
            "Handler {} failed on channel '{}' ({} byte payload); continuing",
            handler,
            n.channel(),
            n.payload().getBytes(StandardCharsets.UTF_8).length,
            t);
      }
    }
  }

  /** The SerialExecutor pattern from the {@link Executor} javadoc, one per channel. */
  private final class ChannelQueue {
    private final String channel;
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
    private boolean active;

    ChannelQueue(String channel) {
      this.channel = channel;
    }

    synchronized void submit(Runnable task) {
      tasks.add(task);
      if (!active) {
        scheduleNext();
      }
    }

    private synchronized void scheduleNext() {
      Runnable next = tasks.poll();
      if (next == null) {
        active = false;
        return;
      }
      active = true;
      try {
        executor.execute(
            () -> {
              try {
                next.run();
              } finally {
                scheduleNext();
              }
            });
      } catch (RejectedExecutionException e) {
        int dropped = tasks.size() + 1;
        tasks.clear();
        active = false;
        log.warn(
            "Handler executor rejected work on channel '{}'; dropped {} notification(s): {}",
            channel,
            dropped,
            e.toString());
      }
    }
  }
}

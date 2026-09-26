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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs handlers on a shared executor while keeping each channel strictly serial.
 *
 * <p>Each channel has its own queue. At most one task per channel is ever handed to the executor at
 * a time; when it finishes, the next one for that channel is submitted. Channels therefore proceed
 * independently on a multithreaded executor, and within a channel notifications are handled in
 * arrival order regardless of how many threads the executor has. A queue lives only while its
 * channel has work: once drained it is retired and removed, so channels that come and go do not
 * accumulate.
 *
 * <p>Queues are unbounded. Delivery is at-most-once anyway, and blocking the listener thread would
 * only move the backlog into Postgres' own notification queue. For the same reason an executor that
 * rejects work costs notifications, never the listener: the channel's queue is dropped and logged.
 * The executor is never called while a queue lock is held, so an executor that blocks in {@code
 * execute} cannot deadlock against a handler finishing. It can still stall the listener thread, as
 * can a {@code CallerRunsPolicy}, which makes the listener thread run the handler itself.
 */
final class SerialDispatcher {

  private static final Logger log = LoggerFactory.getLogger(SerialDispatcher.class);

  private final Executor executor;
  private final Map<String, ChannelQueue> queues = new ConcurrentHashMap<>();

  /** Signalled whenever a queue retires; see {@link #awaitIdle}. Taken after a queue lock. */
  private final Object idle = new Object();

  SerialDispatcher(Executor executor) {
    this.executor = executor;
  }

  /**
   * Queues {@code notification} for delivery to {@code handlers}, in list order. Never throws: if
   * the executor rejects the work, the channel's queued notifications are dropped with a warning.
   */
  void dispatch(Notification notification, List<NotificationHandler> handlers) {
    Runnable task = () -> deliver(notification, handlers);
    while (true) {
      ChannelQueue queue = queues.computeIfAbsent(notification.channel(), ChannelQueue::new);
      if (queue.submit(task)) {
        return;
      }
      // The queue drained and retired between the lookup and the submit; a fresh one takes over.
    }
  }

  /** Channels that currently have a queue, that is, work queued or running. */
  int activeChannels() {
    return queues.size();
  }

  /**
   * Waits until no channel has work queued or running, or the timeout elapses. Call it before
   * shutting the executor down: only one task per channel is ever inside the executor, and a shut
   * down executor rejects the rest. Meaningful once nothing dispatches any more.
   *
   * @return {@code true} if idle
   */
  boolean awaitIdle(Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    synchronized (idle) {
      while (!queues.isEmpty()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          return false;
        }
        TimeUnit.NANOSECONDS.timedWait(idle, remaining);
      }
      return true;
    }
  }

  private static void deliver(Notification n, List<NotificationHandler> handlers) {
    for (NotificationHandler handler : handlers) {
      try {
        handler.handle(n);
      } catch (InterruptedException e) {
        // The executor is shutting down: keep the flag so the thread can stop, skip the rest.
        Thread.currentThread().interrupt();
        log.warn(
            "Handler {} interrupted on channel '{}'; skipping remaining handlers",
            handler,
            n.channel());
        return;
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

  /**
   * The SerialExecutor pattern from the {@link Executor} javadoc, one per channel, with two
   * changes: the executor is called outside the lock, and the queue retires itself once idle.
   */
  private final class ChannelQueue {
    private final String channel;
    private final Object lock = new Object();

    /** Guarded by {@link #lock}. */
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

    /** Guarded by {@link #lock}. Whether a task for this channel is with the executor. */
    private boolean active;

    /** Guarded by {@link #lock}. Set once, when the queue is removed from the map. */
    private boolean retired;

    ChannelQueue(String channel) {
      this.channel = channel;
    }

    /** Returns {@code false} if this queue has retired and the caller must use a new one. */
    boolean submit(Runnable task) {
      synchronized (lock) {
        if (retired) {
          return false;
        }
        if (active) {
          tasks.add(task);
          return true;
        }
        active = true;
      }
      execute(task);
      return true;
    }

    /** Hands {@code task} to the executor. Called with {@code active} set and the lock released. */
    private void execute(Runnable task) {
      try {
        executor.execute(
            () -> {
              try {
                task.run();
              } finally {
                next();
              }
            });
      } catch (RejectedExecutionException e) {
        int dropped;
        synchronized (lock) {
          dropped = tasks.size() + 1;
          tasks.clear();
          retire();
        }
        log.warn(
            "Handler executor rejected work on channel '{}'; dropped {} notification(s): {}",
            channel,
            dropped,
            e.toString());
      }
    }

    private void next() {
      Runnable task;
      synchronized (lock) {
        task = tasks.poll();
        if (task == null) {
          retire();
          return;
        }
      }
      execute(task);
    }

    /** Leaves the map. Must hold {@link #lock}; nothing is queued or running afterwards. */
    private void retire() {
      active = false;
      retired = true;
      queues.remove(channel, this);
      synchronized (idle) {
        idle.notifyAll();
      }
    }
  }
}

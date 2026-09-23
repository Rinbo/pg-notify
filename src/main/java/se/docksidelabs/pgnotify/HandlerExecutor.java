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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The executor handlers run on, together with the ownership rule: an executor the listener created
 * is shut down by the listener; one supplied by the application is left alone.
 */
final class HandlerExecutor implements Executor {

  private static final Logger log = LoggerFactory.getLogger(HandlerExecutor.class);

  /**
   * Marks threads created by an owned executor so {@link #onHandlerThread()} needs no guesswork.
   */
  private static final class HandlerThread extends Thread {
    private final HandlerExecutor owner;

    HandlerThread(Runnable task, String name, HandlerExecutor owner) {
      super(task, name);
      this.owner = owner;
      setDaemon(true);
    }
  }

  private final Executor delegate;
  private final ExecutorService owned;

  private HandlerExecutor(Executor delegate, ExecutorService owned) {
    this.delegate = delegate;
    this.owned = owned;
  }

  /** A single daemon thread named {@code threadName}, created lazily, owned by the listener. */
  static HandlerExecutor owned(String threadName) {
    HandlerExecutor[] self = new HandlerExecutor[1];
    ExecutorService service =
        Executors.newSingleThreadExecutor(r -> new HandlerThread(r, threadName, self[0]));
    self[0] = new HandlerExecutor(service, service);
    return self[0];
  }

  /** Wraps an application-supplied executor. Never shut down by the listener. */
  static HandlerExecutor supplied(Executor executor) {
    return new HandlerExecutor(Objects.requireNonNull(executor, "executor"), null);
  }

  @Override
  public void execute(Runnable task) {
    delegate.execute(task);
  }

  /** {@code true} when called from a thread this executor owns. Always false when supplied. */
  boolean onHandlerThread() {
    return Thread.currentThread() instanceof HandlerThread t && t.owner == this;
  }

  /**
   * Shuts down an owned executor: lets queued work finish for up to {@code grace}, then interrupts.
   * Called from one of its own threads it only initiates the shutdown, since waiting would
   * deadlock. No-op for a supplied executor.
   */
  void shutdown(Duration grace) {
    if (owned == null) {
      return;
    }
    owned.shutdown();
    if (onHandlerThread()) {
      return;
    }
    try {
      if (!owned.awaitTermination(grace.toMillis(), TimeUnit.MILLISECONDS)) {
        log.warn("Handlers still running after {}; interrupting them", grace);
        owned.shutdownNow();
      }
    } catch (InterruptedException e) {
      owned.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}

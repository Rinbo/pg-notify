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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import se.docksidelabs.pgnotify.PgListener.State;

/**
 * The listener's state machine: holds the current {@link State}, applies transitions atomically,
 * and lets callers wait for a state.
 *
 * <p>{@link State#CLOSED} is terminal. Once reached, every other transition is refused and every
 * waiter is released.
 */
final class Lifecycle {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private State state = State.NEW;

  State state() {
    lock.lock();
    try {
      return state;
    } finally {
      lock.unlock();
    }
  }

  boolean isClosed() {
    return state() == State.CLOSED;
  }

  /**
   * Moves from {@code from} to {@code to}. Returns {@code false} if the state was not {@code from}.
   */
  boolean transition(State from, State to) {
    lock.lock();
    try {
      if (state != from || state == State.CLOSED) {
        return false;
      }
      set(to);
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Moves to {@code to} from any non-terminal state. Returns {@code false} if already closed. */
  boolean moveTo(State to) {
    lock.lock();
    try {
      if (state == State.CLOSED) {
        return false;
      }
      set(to);
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Moves to {@link State#CLOSED}. Returns {@code true} only for the call that did the closing. */
  boolean close() {
    return moveTo(State.CLOSED);
  }

  /**
   * Waits until the state is {@code target}, the lifecycle is closed, or the timeout elapses.
   *
   * @return {@code true} if the state is {@code target}
   */
  boolean await(State target, Duration timeout) throws InterruptedException {
    long remaining = timeout.toNanos();
    lock.lock();
    try {
      while (state != target && state != State.CLOSED) {
        if (remaining <= 0) {
          return false;
        }
        remaining = changed.awaitNanos(remaining);
      }
      return state == target;
    } finally {
      lock.unlock();
    }
  }

  private void set(State next) {
    state = next;
    changed.signalAll();
  }
}

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

/**
 * Observes the listener's connection: connected, lost, and reconnected.
 *
 * <p>The one method every application should implement is {@link #onReconnected}. Notifications
 * sent while the listener was disconnected are lost, and the application has to resynchronise
 * whatever the notifications were keeping fresh, for example by flushing a cache.
 *
 * <p>Callbacks run on the listener thread. {@code onConnected} and {@code onReconnected} run after
 * {@code LISTEN} has been issued for every channel, before the state becomes {@link
 * PgListener.State#LISTENING}, and before any notification from the new connection is dispatched;
 * so when {@link PgListener#awaitListening} returns {@code true} the callback has completed.
 * Handler tasks from the old connection that are still queued may run after it. Keep callbacks
 * short, or hand the work to another thread: while a callback runs nothing is read from Postgres.
 * Anything a callback throws is logged and ignored.
 */
public interface ConnectionListener {

  /** The first successful connection after {@link PgListener#start()}. */
  default void onConnected() {}

  /**
   * A later successful connection, after the previous one was lost. Resynchronise here.
   *
   * @param event when the connection was lost and regained, and how many attempts it took
   */
  default void onReconnected(ReconnectEvent event) {}

  /**
   * The connection was lost, or an attempt to connect failed. Called once per failure, so during a
   * long outage it fires on every retry.
   *
   * @param cause what went wrong
   */
  default void onDisconnected(Throwable cause) {}
}

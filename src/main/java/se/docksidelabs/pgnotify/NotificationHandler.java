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
 * Receives notifications for a channel.
 *
 * <p>Handlers run on the listener's handler executor, never on the thread that reads from Postgres,
 * so a slow handler delays only its own channel. Notifications on one channel are handed to its
 * handlers one at a time, in the order Postgres delivered them.
 *
 * <p>Anything a handler throws is caught and logged, and the next notification is delivered as
 * usual. A throwing handler never affects other handlers, other channels or the connection.
 */
@FunctionalInterface
public interface NotificationHandler {

  /**
   * Handles one notification.
   *
   * @param notification the notification; never {@code null}
   * @throws Exception any failure; it is logged and otherwise ignored
   */
  void handle(Notification notification) throws Exception;
}

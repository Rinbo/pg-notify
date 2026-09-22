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
 * A registered handler on a channel, returned by {@link PgListener#listen}.
 *
 * <p>Closing it removes the handler. When the last handler on a channel is removed the listener
 * stops listening on that channel. Closing twice is harmless.
 */
public interface Subscription extends AutoCloseable {

  /** Removes the handler. Idempotent. */
  @Override
  void close();
}

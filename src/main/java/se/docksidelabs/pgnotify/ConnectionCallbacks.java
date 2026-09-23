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

import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fans one connection event out to every registered {@link ConnectionListener}, isolating failures.
 */
final class ConnectionCallbacks {

  private static final Logger log = LoggerFactory.getLogger(ConnectionCallbacks.class);

  private final List<ConnectionListener> listeners;

  ConnectionCallbacks(List<ConnectionListener> listeners) {
    this.listeners = List.copyOf(listeners);
  }

  void connected() {
    fire("onConnected", ConnectionListener::onConnected);
  }

  void reconnected(ReconnectEvent event) {
    fire("onReconnected", l -> l.onReconnected(event));
  }

  void disconnected(Throwable cause) {
    fire("onDisconnected", l -> l.onDisconnected(cause));
  }

  private void fire(String name, Consumer<ConnectionListener> call) {
    for (ConnectionListener listener : listeners) {
      try {
        call.accept(listener);
      } catch (RuntimeException e) {
        log.warn("ConnectionListener {} threw from {}; ignoring", listener, name, e);
      }
    }
  }
}

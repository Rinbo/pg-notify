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

import java.sql.SQLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.docksidelabs.pgnotify.PgListener.State;

/**
 * What the listener thread does: open a session, subscribe, and poll until the listener is closed,
 * handing every notification to the dispatcher.
 *
 * <p>A lost connection currently ends the loop; reconnecting is the next milestone.
 */
final class ListenerLoop implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ListenerLoop.class);

  private final ListenerConfig config;
  private final ConnectionProvider connectionProvider;
  private final HandlerRegistry registry;
  private final SerialDispatcher dispatcher;
  private final Lifecycle lifecycle;

  /** The live session, published so {@link #abortSession()} can reach it from another thread. */
  private volatile ListenerSession session;

  ListenerLoop(
      ListenerConfig config,
      ConnectionProvider connectionProvider,
      HandlerRegistry registry,
      SerialDispatcher dispatcher,
      Lifecycle lifecycle) {
    this.config = config;
    this.connectionProvider = connectionProvider;
    this.registry = registry;
    this.dispatcher = dispatcher;
    this.lifecycle = lifecycle;
  }

  @Override
  public void run() {
    try (ListenerSession s = ListenerSession.open(connectionProvider, config.networkTimeout())) {
      session = s;
      s.listen(registry.channels());
      if (!lifecycle.transition(State.CONNECTING, State.LISTENING)) {
        return; // closed while we were connecting
      }
      log.info(
          "Listening on {} channel(s), backend pid {}", registry.channelCount(), s.backendPid());
      while (!lifecycle.isClosed()) {
        dispatch(s.poll(config.pollTimeout()));
      }
    } catch (SQLException e) {
      if (lifecycle.isClosed()) {
        log.debug("Connection closed during shutdown", e);
      } else {
        log.error("Listener connection failed; reconnect is not implemented yet", e);
      }
    } catch (RuntimeException e) {
      log.error("Listener loop failed", e);
    } finally {
      session = null;
      lifecycle.close();
    }
  }

  /** Unblocks a poll in progress by closing the socket. Safe from any thread. */
  void abortSession() {
    ListenerSession s = session;
    if (s != null) {
      s.abort();
    }
  }

  private void dispatch(List<Notification> batch) {
    for (Notification n : batch) {
      List<NotificationHandler> handlers = registry.handlersFor(n.channel());
      if (handlers.isEmpty()) {
        log.debug("No handler for channel '{}'; dropping notification", n.channel());
        continue;
      }
      dispatcher.dispatch(n, handlers);
    }
  }
}

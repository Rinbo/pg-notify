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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.docksidelabs.pgnotify.PgListener.State;

/**
 * What the listener thread does: connect, subscribe, poll until closed, and on any failure back off
 * and do it again. Never gives up; {@code close()} is the only exit.
 *
 * <pre>
 * loop until closed:
 *   open session, LISTEN every channel            -- failure: RECONNECTING, onDisconnected, backoff
 *   LISTENING; onConnected or onReconnected
 *   poll until closed; SELECT 1 when idle          -- failure: same as above
 * </pre>
 */
final class ListenerLoop implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ListenerLoop.class);

  private final ListenerConfig config;
  private final ConnectionProvider connectionProvider;
  private final HandlerRegistry registry;
  private final SerialDispatcher dispatcher;
  private final Lifecycle lifecycle;
  private final ConnectionCallbacks callbacks;
  private final ReconnectPolicy reconnectPolicy;

  /** The live session, published so {@link #abortSession()} can reach it from another thread. */
  private volatile ListenerSession session;

  /** Consecutive failed attempts since the last successful subscription. */
  private int failures;

  /** When the last good connection was lost; {@code null} while connected. */
  private Instant disconnectedAt;

  private boolean everConnected;

  ListenerLoop(
      ListenerConfig config,
      ConnectionProvider connectionProvider,
      HandlerRegistry registry,
      SerialDispatcher dispatcher,
      Lifecycle lifecycle,
      ConnectionCallbacks callbacks,
      ReconnectPolicy reconnectPolicy) {
    this.config = config;
    this.connectionProvider = connectionProvider;
    this.registry = registry;
    this.dispatcher = dispatcher;
    this.lifecycle = lifecycle;
    this.callbacks = callbacks;
    this.reconnectPolicy = reconnectPolicy;
  }

  @Override
  public void run() {
    try {
      while (!lifecycle.isClosed()) {
        try {
          connectAndPoll();
        } catch (SQLException e) {
          if (!backOff(e)) {
            return;
          }
        }
      }
    } catch (RuntimeException e) {
      log.error("Listener loop failed; listener is now closed", e);
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

  /** One connection's lifetime. Returns when closed; throws when the connection fails. */
  private void connectAndPoll() throws SQLException {
    try (ListenerSession s = ListenerSession.open(connectionProvider, config.networkTimeout())) {
      session = s;
      try {
        s.listen(registry.channels());
        if (lifecycle.isClosed()) {
          return;
        }
        onSubscribed(s); // callbacks complete before anyone waiting for LISTENING is released
        if (!lifecycle.moveTo(State.LISTENING)) {
          return;
        }
        pollUntilClosed(s);
      } catch (SQLException e) {
        s.abort(); // a broken socket must not stall the close that follows
        throw e;
      } finally {
        session = null;
      }
    }
  }

  private void onSubscribed(ListenerSession s) {
    int attempts = failures + 1;
    failures = 0;
    if (everConnected) {
      ReconnectEvent event = new ReconnectEvent(disconnectedAt, Instant.now(), attempts);
      log.info(
          "Reconnected to backend pid {} after {} attempt(s); notifications during the {} outage"
              + " were lost",
          s.backendPid(),
          attempts,
          event.downtime());
      callbacks.reconnected(event);
    } else {
      everConnected = true;
      log.info(
          "Listening on {} channel(s), backend pid {}", registry.channelCount(), s.backendPid());
      callbacks.connected();
    }
    disconnectedAt = null;
  }

  private void pollUntilClosed(ListenerSession s) throws SQLException {
    long idleSince = System.nanoTime();
    while (!lifecycle.isClosed()) {
      List<Notification> batch = s.poll(config.pollTimeout());
      if (!batch.isEmpty()) {
        idleSince = System.nanoTime();
        dispatch(batch);
      } else if (System.nanoTime() - idleSince >= config.healthCheckInterval().toNanos()) {
        s.healthCheck();
        idleSince = System.nanoTime();
      }
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

  /**
   * Records a failure, notifies, and waits out the backoff.
   *
   * @return {@code false} if the listener was closed, now or during the wait
   */
  private boolean backOff(SQLException cause) {
    if (lifecycle.isClosed()) {
      log.debug("Connection closed during shutdown", cause);
      return false;
    }
    failures++;
    if (disconnectedAt == null) {
      disconnectedAt = Instant.now();
    }
    lifecycle.moveTo(State.RECONNECTING);
    Duration delay = reconnectPolicy.delay(failures);
    log.warn("Connection attempt {} failed: {}; retrying in {}", failures, cause.toString(), delay);
    log.debug("Failure detail", cause);
    callbacks.disconnected(cause);
    try {
      return !lifecycle.await(State.CLOSED, delay);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}

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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for Postgres notifications on a dedicated connection and hands them to handlers.
 *
 * <h2>Delivery is at-most-once</h2>
 *
 * <p>Postgres does not persist notifications. Anything sent while this listener is not connected is
 * gone. If your application needs to be correct across a reconnect, for example a cache that must
 * not serve stale data, register a reconnect callback and resync there.
 *
 * <h2>Why a dedicated connection</h2>
 *
 * <p>{@code LISTEN} is session state, so the listener must own its connection for the whole time it
 * runs. A pooled connection is wrong for three reasons: the pool hands it to other borrowers who
 * then inherit the subscriptions and the queued notifications; the listener parks the connection in
 * a blocking read, which a pool treats as a leak or worse, uses concurrently; and Postgres holds
 * notifications back while the receiving session is inside a transaction, so the connection must
 * stay in autocommit and nobody else may run statements on it. Give the listener a JDBC URL and
 * properties, or a supplier that opens a fresh connection each time it is called.
 *
 * <p><b>PgBouncer in transaction or statement pooling mode breaks {@code LISTEN}</b>, because the
 * subscription lands on one server backend and later notifications arrive at another. Point the
 * listener at Postgres directly or at a session-mode pool. Publishing with {@link PgNotifier} works
 * behind any pooler.
 *
 * <h2>Threads</h2>
 *
 * <p>One daemon thread owns the connection and does nothing but read notifications. Handlers run on
 * a separate executor, by default a single owned daemon thread, so a slow handler never delays the
 * read loop. Notifications on one channel are delivered in order, one at a time, even on a
 * multi-threaded executor. Handler exceptions are logged and never affect delivery.
 *
 * <p>Channel names are case-sensitive and match {@code pg_notify} byte for byte; see {@link
 * PgNotifier} for the details.
 */
public final class PgListener implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PgListener.class);
  private static final AtomicInteger INSTANCES = new AtomicInteger();

  /** Lifecycle of a listener, see {@link #state()}. */
  public enum State {
    /** Built, not started. */
    NEW,
    /** {@link #start()} called; the listener thread is opening the connection. */
    CONNECTING,
    /** Connected and subscribed to every registered channel. */
    LISTENING,
    /** Connection lost; waiting to reconnect. */
    RECONNECTING,
    /** {@link #close()} called, or the listener stopped on its own. Terminal. */
    CLOSED
  }

  private final ConnectionFactory connectionFactory;
  private final Duration pollTimeout;
  private final Duration networkTimeout;
  private final Duration shutdownTimeout;
  private final ExecutorService ownedExecutor;
  private final SerialDispatcher dispatcher;
  private final Thread thread;
  private final String handlerThreadPrefix;

  /** Guards {@link #state}, {@link #closeRequested} and structural changes to {@link #handlers}. */
  private final Object lock = new Object();

  private final Map<String, CopyOnWriteArrayList<NotificationHandler>> handlers =
      new ConcurrentHashMap<>();
  private volatile State state = State.NEW;
  private boolean closeRequested;
  private volatile Connection connection;

  private PgListener(Builder b) {
    int id = INSTANCES.incrementAndGet();
    this.connectionFactory = b.connectionFactory();
    this.pollTimeout = b.pollTimeout;
    this.networkTimeout = b.networkTimeout;
    this.shutdownTimeout = b.shutdownTimeout;
    this.handlerThreadPrefix = "pg-notify-handler-" + id;
    Executor executor = b.handlerExecutor;
    if (executor == null) {
      this.ownedExecutor = Executors.newSingleThreadExecutor(r -> daemon(r, handlerThreadPrefix));
      executor = ownedExecutor;
    } else {
      this.ownedExecutor = null;
    }
    this.dispatcher = new SerialDispatcher(executor);
    b.handlers.forEach((channel, hs) -> handlers.put(channel, new CopyOnWriteArrayList<>(hs)));
    this.thread = daemon(this::run, "pg-notify-listener-" + id);
  }

  private static Thread daemon(Runnable r, String name) {
    Thread t = new Thread(r, name);
    t.setDaemon(true);
    return t;
  }

  /**
   * Starts building a listener that opens its own connection with {@link java.sql.DriverManager}.
   *
   * @param jdbcUrl a pgjdbc URL
   * @param properties connection properties such as {@code user} and {@code password}; may be
   *     {@code null}. {@code tcpKeepAlive=true} and {@code ApplicationName} are added unless
   *     present.
   */
  public static Builder builder(String jdbcUrl, Properties properties) {
    return new Builder(ConnectionFactory.forUrl(jdbcUrl, properties, Builder.DEFAULT_APP_NAME));
  }

  /**
   * Starts building a listener that obtains its connection from {@code connectionSupplier}.
   *
   * <p>The supplier must return a new, unshared pgjdbc connection each time it is called; it is
   * called again after every connection loss. Never return a pooled connection; see the class
   * documentation for why. The listener sets autocommit and a network timeout itself but relies on
   * the supplier for TCP keepalive.
   */
  public static Builder builder(Supplier<Connection> connectionSupplier) {
    return new Builder(ConnectionFactory.forSupplier(connectionSupplier));
  }

  /**
   * Starts the listener thread and returns immediately. Idempotent.
   *
   * <p>The connection is opened on the listener thread. Use {@link #awaitListening} to block until
   * the listener is subscribed, for example during application startup.
   *
   * @throws IllegalStateException if the listener has been closed
   */
  public void start() {
    synchronized (lock) {
      if (state == State.CLOSED) {
        throw new IllegalStateException("listener is closed");
      }
      if (state != State.NEW) {
        return;
      }
      setState(State.CONNECTING);
    }
    thread.start();
  }

  /**
   * Blocks until the listener is subscribed to all channels, it is closed, or the timeout elapses.
   *
   * @return {@code true} if the listener is in {@link State#LISTENING}
   * @throws IllegalStateException if {@link #start()} has not been called
   */
  public boolean awaitListening(Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    synchronized (lock) {
      if (state == State.NEW) {
        throw new IllegalStateException("listener has not been started");
      }
      while (state != State.LISTENING && state != State.CLOSED) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          return false;
        }
        TimeUnit.NANOSECONDS.timedWait(lock, remaining);
      }
      return state == State.LISTENING;
    }
  }

  /**
   * Registers a handler for a channel.
   *
   * <p>Several handlers may share a channel; they run in registration order. The channel name is
   * validated now: non-empty, at most 63 bytes of UTF-8, no control characters.
   *
   * @return a subscription that removes the handler when closed
   * @throws IllegalArgumentException if the channel name is invalid
   * @throws IllegalStateException if the listener is closed, or has already been started (dynamic
   *     subscriptions are not supported yet)
   */
  public Subscription listen(String channel, NotificationHandler handler) {
    ChannelNames.validate(channel);
    Objects.requireNonNull(handler, "handler");
    synchronized (lock) {
      requireNotStarted("subscribing");
      handlers.computeIfAbsent(channel, c -> new CopyOnWriteArrayList<>()).add(handler);
    }
    return () -> unsubscribe(channel, handler);
  }

  private void unsubscribe(String channel, NotificationHandler handler) {
    synchronized (lock) {
      if (state == State.CLOSED) {
        return;
      }
      requireNotStarted("unsubscribing");
      List<NotificationHandler> list = handlers.get(channel);
      if (list != null && list.remove(handler) && list.isEmpty()) {
        handlers.remove(channel);
      }
    }
  }

  private void requireNotStarted(String what) {
    if (state == State.CLOSED) {
      throw new IllegalStateException("listener is closed");
    }
    if (state != State.NEW) {
      throw new IllegalStateException(what + " after start() is not supported yet");
    }
  }

  /** Current lifecycle state. */
  public State state() {
    return state;
  }

  /**
   * Stops the listener and releases its resources. Idempotent; safe to call from a handler.
   *
   * <p>Waits for the listener thread to exit, then shuts down the owned handler executor, allowing
   * queued handler work up to the configured shutdown timeout before interrupting it. A
   * user-supplied executor is left running.
   */
  @Override
  public void close() {
    boolean started;
    synchronized (lock) {
      if (closeRequested) {
        return;
      }
      closeRequested = true;
      started = state != State.NEW;
      setState(State.CLOSED);
    }
    if (started && Thread.currentThread() != thread) {
      thread.interrupt();
      joinQuietly(pollTimeout.plusSeconds(2));
      if (thread.isAlive()) {
        log.debug("Listener thread did not exit after poll timeout; aborting connection");
        abortConnection();
        joinQuietly(Duration.ofSeconds(5));
      }
    }
    if (ownedExecutor != null) {
      shutdownOwnedExecutor();
    }
  }

  private void shutdownOwnedExecutor() {
    ownedExecutor.shutdown();
    if (Thread.currentThread().getName().startsWith(handlerThreadPrefix)) {
      return; // called from a handler: cannot wait for ourselves
    }
    try {
      if (!ownedExecutor.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        log.warn("Handlers still running after {}; interrupting", shutdownTimeout);
        ownedExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      ownedExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private void joinQuietly(Duration timeout) {
    try {
      thread.join(timeout.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void abortConnection() {
    Connection c = connection;
    if (c == null) {
      return;
    }
    try {
      c.abort(Runnable::run);
    } catch (SQLException | RuntimeException e) {
      log.debug("Ignoring failure while aborting connection", e);
    }
  }

  private void setState(State next) {
    synchronized (lock) {
      if (state == State.CLOSED && next != State.CLOSED) {
        return;
      }
      state = next;
      lock.notifyAll();
    }
  }

  private boolean isCloseRequested() {
    synchronized (lock) {
      return closeRequested;
    }
  }

  // ---- listener thread ---------------------------------------------------------------------

  private void run() {
    Connection conn = null;
    try {
      conn = connectionFactory.open(networkTimeout);
      connection = conn;
      PGConnection pg = conn.unwrap(PGConnection.class);
      subscribeAll(conn);
      setState(State.LISTENING);
      log.info("Listening on {} channel(s), backend pid {}", handlers.size(), pg.getBackendPID());
      poll(pg);
    } catch (SQLException e) {
      if (isCloseRequested()) {
        log.debug("Connection closed during shutdown", e);
      } else {
        log.error("Listener connection failed; reconnect is not implemented yet", e);
      }
    } catch (RuntimeException e) {
      log.error("Listener thread failed", e);
    } finally {
      connection = null;
      ConnectionFactory.closeQuietly(conn);
      setState(State.CLOSED);
    }
  }

  private void subscribeAll(Connection conn) throws SQLException {
    try (Statement st = conn.createStatement()) {
      for (String channel : handlers.keySet()) {
        st.execute("LISTEN " + ChannelNames.quote(channel));
      }
    }
  }

  private void poll(PGConnection pg) throws SQLException {
    int timeoutMillis = (int) pollTimeout.toMillis();
    while (!isCloseRequested()) {
      PGNotification[] batch = pg.getNotifications(timeoutMillis);
      if (batch == null) {
        continue;
      }
      for (PGNotification raw : batch) {
        List<NotificationHandler> hs = handlers.get(raw.getName());
        if (hs == null || hs.isEmpty()) {
          log.debug("No handler for channel '{}'; dropping notification", raw.getName());
          continue;
        }
        dispatcher.dispatch(new Notification(raw.getName(), raw.getParameter(), raw.getPID()), hs);
      }
    }
  }

  // ---- builder -----------------------------------------------------------------------------

  /** Configures and creates a {@link PgListener}. Not thread-safe. */
  public static final class Builder {

    static final String DEFAULT_APP_NAME = "pg-notify";

    private final ConnectionFactory connectionFactory;
    private final Map<String, List<NotificationHandler>> handlers = new LinkedHashMap<>();
    private Executor handlerExecutor;
    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration networkTimeout = Duration.ofSeconds(10);
    private Duration shutdownTimeout = Duration.ofSeconds(10);

    private Builder(ConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
    }

    private ConnectionFactory connectionFactory() {
      return connectionFactory;
    }

    /** Registers a handler; same rules as {@link PgListener#listen}. */
    public Builder listen(String channel, NotificationHandler handler) {
      ChannelNames.validate(channel);
      Objects.requireNonNull(handler, "handler");
      handlers.computeIfAbsent(channel, c -> new ArrayList<>()).add(handler);
      return this;
    }

    /**
     * Executor that runs handlers. Default: one daemon thread owned and shut down by the listener.
     * A supplied executor is never shut down by the listener. Per-channel ordering is preserved on
     * any executor.
     */
    public Builder handlerExecutor(Executor executor) {
      this.handlerExecutor = Objects.requireNonNull(executor, "executor");
      return this;
    }

    /**
     * How long each blocking read for notifications waits before the listener thread checks for
     * shutdown. Bounds how long {@link #close()} takes. Default 1 second.
     */
    public Builder pollTimeout(Duration pollTimeout) {
      this.pollTimeout = requirePositiveMillis(pollTimeout, "pollTimeout");
      return this;
    }

    /**
     * Socket read timeout applied to the listener connection for everything except the notification
     * poll. Default 10 seconds.
     */
    public Builder networkTimeout(Duration networkTimeout) {
      this.networkTimeout = requirePositiveMillis(networkTimeout, "networkTimeout");
      return this;
    }

    /**
     * How long {@link PgListener#close()} waits for queued handler work on the owned executor
     * before interrupting it. Default 10 seconds.
     */
    public Builder shutdownTimeout(Duration shutdownTimeout) {
      this.shutdownTimeout = requirePositiveMillis(shutdownTimeout, "shutdownTimeout");
      return this;
    }

    private static Duration requirePositiveMillis(Duration d, String name) {
      Objects.requireNonNull(d, name);
      long millis = d.toMillis();
      if (millis <= 0 || millis > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            name + " must be between 1 ms and " + Integer.MAX_VALUE + " ms, was " + d);
      }
      return d;
    }

    /** Creates the listener. It is not started. */
    public PgListener build() {
      return new PgListener(this);
    }
  }
}

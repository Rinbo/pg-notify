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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Listens for Postgres notifications on a dedicated connection and hands them to handlers.
 *
 * <p>Delivery is at-most-once: notifications sent while the listener is not connected are lost.
 * Read the {@linkplain se.docksidelabs.pgnotify package documentation} before using this class; it
 * explains that trade-off, why the connection must not come from a pool, and how PgBouncer
 * interacts with {@code LISTEN}.
 *
 * <p>Typical use:
 *
 * <pre>{@code
 * try (PgListener listener = PgListener.builder(url, props)
 *         .listen("orders", n -> cache.invalidate(n.payload()))
 *         .build()) {
 *   listener.start();
 *   listener.awaitListening(Duration.ofSeconds(10));
 *   ...
 * }
 * }</pre>
 *
 * <p>All methods are thread-safe. {@link #start()} and {@link #close()} are idempotent.
 */
public final class PgListener implements AutoCloseable {

  /** Lifecycle of a listener, see {@link #state()}. */
  public enum State {
    /** Built, not started. */
    NEW,
    /** {@link #start()} called; the listener thread is opening the connection. */
    CONNECTING,
    /** Connected and subscribed to every registered channel. */
    LISTENING,
    /** Connection lost or a connection attempt failed; retrying with backoff. */
    RECONNECTING,
    /** {@link #close()} called, or the listener stopped on its own. Terminal. */
    CLOSED
  }

  private final String name;
  private final ListenerConfig config;
  private final ConnectionProvider connectionProvider;
  private final HandlerRegistry registry;
  private final HandlerExecutor executor;
  private final ConnectionCallbacks callbacks;
  private final Lifecycle lifecycle = new Lifecycle();

  /** Set by {@link #start()}; guarded by {@code this}. */
  private ListenerLoop loop;

  private Thread thread;

  private PgListener(
      String name,
      ListenerConfig config,
      ConnectionProvider connectionProvider,
      HandlerRegistry registry,
      HandlerExecutor executor,
      ConnectionCallbacks callbacks) {
    this.name = name;
    this.config = config;
    this.connectionProvider = connectionProvider;
    this.registry = registry;
    this.executor = executor;
    this.callbacks = callbacks;
  }

  /**
   * Starts building a listener that opens its own connection with {@link java.sql.DriverManager}.
   *
   * @param jdbcUrl a pgjdbc URL
   * @param properties connection properties such as {@code user} and {@code password}; may be
   *     {@code null}. {@code tcpKeepAlive=true} and {@code ApplicationName=pg-notify} are added
   *     unless present.
   */
  public static Builder builder(String jdbcUrl, Properties properties) {
    return new Builder(ConnectionProviders.forUrl(jdbcUrl, properties));
  }

  /**
   * Starts building a listener that opens its connection through {@code connectionProvider}.
   *
   * <p>This is the overload to use when the application already knows how to reach its database,
   * for example a Spring Boot service with a configured {@code DataSource}. <b>Do not pass the pool
   * itself</b> ({@code dataSource::getConnection} on HikariCP or any other pool): the listener
   * keeps its connection for life, which a pool treats as a leak, and pool housekeeping would drop
   * the subscriptions. Open an unpooled connection from the same coordinates:
   *
   * <pre>{@code
   * PgListener.builder(() -> DriverManager.getConnection(url, user, password))
   * }</pre>
   *
   * <p>The provider is called again after every connection loss. See {@link ConnectionProvider} for
   * what it must guarantee.
   */
  public static Builder builder(ConnectionProvider connectionProvider) {
    return new Builder(Objects.requireNonNull(connectionProvider, "connectionProvider"));
  }

  /**
   * Starts the listener thread and returns immediately. Idempotent.
   *
   * <p>The connection is opened on the listener thread. A failed first connection is retried with
   * backoff like any other; the listener never gives up until {@link #close()}. Use {@link
   * #awaitListening} to block until the listener is subscribed, for example during application
   * startup, if you would rather fail fast.
   *
   * @throws IllegalStateException if the listener has been closed
   */
  public synchronized void start() {
    if (lifecycle.isClosed()) {
      throw new IllegalStateException("listener is closed");
    }
    if (!lifecycle.transition(State.NEW, State.CONNECTING)) {
      return;
    }
    loop =
        new ListenerLoop(
            config,
            connectionProvider,
            registry,
            new SerialDispatcher(executor),
            lifecycle,
            callbacks,
            new ReconnectPolicy(
                config.backoffInitial(), config.backoffMax(), ThreadLocalRandom.current()));
    thread = new Thread(loop, name + "-listener");
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Blocks until the listener is subscribed to all channels, it is closed, or the timeout elapses.
   *
   * @return {@code true} if the listener is in {@link State#LISTENING}
   * @throws IllegalStateException if {@link #start()} has not been called
   */
  public boolean awaitListening(Duration timeout) throws InterruptedException {
    if (lifecycle.state() == State.NEW) {
      throw new IllegalStateException("listener has not been started");
    }
    return lifecycle.await(State.LISTENING, timeout);
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
  public synchronized Subscription listen(String channel, NotificationHandler handler) {
    requireNotStarted("subscribing");
    registry.add(channel, handler);
    return () -> unsubscribe(channel, handler);
  }

  private synchronized void unsubscribe(String channel, NotificationHandler handler) {
    if (lifecycle.isClosed()) {
      return;
    }
    requireNotStarted("unsubscribing");
    registry.remove(channel, handler);
  }

  private void requireNotStarted(String what) {
    State current = lifecycle.state();
    if (current == State.CLOSED) {
      throw new IllegalStateException("listener is closed");
    }
    if (current != State.NEW) {
      throw new IllegalStateException(what + " after start() is not supported yet");
    }
  }

  /** Current lifecycle state. */
  public State state() {
    return lifecycle.state();
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
    if (!lifecycle.close()) {
      return;
    }
    Thread t;
    ListenerLoop l;
    synchronized (this) {
      t = thread;
      l = loop;
    }
    if (t != null && t != Thread.currentThread()) {
      join(t, config.pollTimeout().plusSeconds(2));
      if (t.isAlive()) {
        l.abortSession();
        join(t, Duration.ofSeconds(5));
      }
    }
    executor.shutdown(config.shutdownTimeout());
  }

  private static void join(Thread t, Duration timeout) {
    try {
      t.join(timeout.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Configures and creates a {@link PgListener}. Not thread-safe; one builder per listener. */
  public static final class Builder {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private record Registration(String channel, NotificationHandler handler) {}

    private final ConnectionProvider connectionProvider;
    private final List<Registration> registrations = new ArrayList<>();
    private final List<ConnectionListener> connectionListeners = new ArrayList<>();
    private ListenerConfig config = ListenerConfig.DEFAULTS;
    private Executor handlerExecutor;

    private Builder(ConnectionProvider connectionProvider) {
      this.connectionProvider = connectionProvider;
    }

    /** Registers a handler; same rules as {@link PgListener#listen}. */
    public Builder listen(String channel, NotificationHandler handler) {
      ChannelNames.validate(channel);
      Objects.requireNonNull(handler, "handler");
      registrations.add(new Registration(channel, handler));
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
     * Called when the connection comes up, goes down, or comes back. May be called more than once;
     * listeners are notified in registration order.
     *
     * <p>Every application should handle {@link ConnectionListener#onReconnected}: notifications
     * sent during the outage are lost, so resynchronise there.
     */
    public Builder connectionListener(ConnectionListener listener) {
      connectionListeners.add(Objects.requireNonNull(listener, "listener"));
      return this;
    }

    /**
     * Shorthand for a {@link ConnectionListener} that only handles {@link
     * ConnectionListener#onReconnected}. This is the callback every application should register:
     * notifications sent during the outage are lost, so resynchronise here, for example by flushing
     * the cache the notifications keep fresh.
     */
    public Builder onReconnect(Consumer<ReconnectEvent> callback) {
      Objects.requireNonNull(callback, "callback");
      return connectionListener(
          new ConnectionListener() {
            @Override
            public void onReconnected(ReconnectEvent event) {
              callback.accept(event);
            }
          });
    }

    /**
     * How long the connection may be silent before the listener sends {@code SELECT 1} to check it
     * is alive. Incoming notifications count as traffic and defer the check. A dead peer is
     * detected within about this interval plus the network timeout. Default 30 seconds.
     */
    public Builder healthCheckInterval(Duration interval) {
      config = config.withHealthCheckInterval(interval);
      return this;
    }

    /**
     * Reconnect backoff. The delay before attempt {@code n+1} is random between zero and {@code
     * min(max, initial * 2^(n-1))}. Default 500 ms to 30 seconds.
     */
    public Builder backoff(Duration initial, Duration max) {
      config = config.withBackoff(initial, max);
      return this;
    }

    /**
     * How long each blocking read for notifications waits before the listener thread checks for
     * shutdown. Bounds how long {@link PgListener#close()} takes. Default 1 second.
     */
    public Builder pollTimeout(Duration pollTimeout) {
      config = config.withPollTimeout(pollTimeout);
      return this;
    }

    /**
     * Socket read timeout applied to the listener connection for everything except the notification
     * poll. Default 10 seconds.
     */
    public Builder networkTimeout(Duration networkTimeout) {
      config = config.withNetworkTimeout(networkTimeout);
      return this;
    }

    /**
     * How long {@link PgListener#close()} waits for queued handler work on the owned executor
     * before interrupting it. Default 10 seconds.
     */
    public Builder shutdownTimeout(Duration shutdownTimeout) {
      config = config.withShutdownTimeout(shutdownTimeout);
      return this;
    }

    /** Creates the listener. It is not started. */
    public PgListener build() {
      String name = "pg-notify-" + COUNTER.incrementAndGet();
      HandlerRegistry registry = new HandlerRegistry();
      registrations.forEach(r -> registry.add(r.channel(), r.handler()));
      HandlerExecutor executor =
          handlerExecutor == null
              ? HandlerExecutor.owned(name + "-handler")
              : HandlerExecutor.supplied(handlerExecutor);
      return new PgListener(
          name,
          config,
          connectionProvider,
          registry,
          executor,
          new ConnectionCallbacks(connectionListeners));
    }
  }
}

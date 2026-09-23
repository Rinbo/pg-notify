# pg-notify

Postgres `LISTEN`/`NOTIFY` for the JVM, done carefully. If your service already has Postgres, you can
signal between instances, for example to invalidate caches, without adding a message broker.

The two SQL statements are trivial. The library exists for the parts around them that are easy to get
wrong: a dedicated connection the listener owns, reconnect with backoff and re-`LISTEN`, detection of
silently dead connections, safe channel quoting, per-channel ordered dispatch off the listener thread,
and a publisher that rides your own transaction.

Runtime dependencies: pgjdbc and slf4j-api. Java 17+. Apache-2.0.

## Read this first: delivery is at-most-once

Postgres does not persist notifications. Anything sent while your listener is not connected is gone,
and the listener cannot tell you what it missed. Every outage, however short, is a gap.

So every application must handle the reconnect:

```java
PgListener listener = PgListener.builder(url, props)
    .listen("products", n -> cache.invalidate(n.payload()))
    .onReconnect(event -> cache.invalidateAll())   // <- the one callback you must think about
    .build();
```

`onReconnect` runs after `LISTEN` has been re-issued and before any notification from the new
connection is dispatched. The `ReconnectEvent` tells you the downtime window and the number of
attempts. Resynchronise whatever the notifications were keeping fresh. If you need durability, write to
a table and use notifications only as a wake-up.

## Quick start

Not yet on Maven Central. Build locally with `mvn install` and depend on:

```xml
<dependency>
  <groupId>se.docksidelabs</groupId>
  <artifactId>pg-notify</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Listen:

```java
Properties props = new Properties();
props.setProperty("user", "app");
props.setProperty("password", "secret");

try (PgListener listener = PgListener.builder("jdbc:postgresql://db/app", props)
        .listen("products", n -> cache.invalidate(n.payload()))
        .onReconnect(event -> cache.invalidateAll())
        .build()) {
  listener.start();                                     // non-blocking; connects on its own thread
  listener.awaitListening(Duration.ofSeconds(10));      // optional startup gate
  // ... run your service ...
}                                                       // close() stops the thread and the connection
```

Publish, inside the transaction that made the change:

```java
try (Connection c = dataSource.getConnection()) {       // your normal, pooled connection
  c.setAutoCommit(false);
  update(c, productId);
  PgNotifier.notify(c, "products", productId);          // delivered on commit, dropped on rollback
  c.commit();
}
```

Handlers receive a `Notification` with `channel()`, `payload()` and `senderPid()`. Subscriptions can be
added and removed while the listener runs; `listen()` returns a `Subscription` whose `close()` removes
the handler and, if it was the last one on the channel, issues `UNLISTEN`.

## Where the listener's connection comes from

The listener needs one connection of its own, held for its whole lifetime. Two ways to give it one:

```java
// 1. Let the library open it through DriverManager
PgListener.builder(jdbcUrl, properties)

// 2. Open it yourself, for example from the coordinates your app already has
PgListener.builder(() -> DriverManager.getConnection(url, user, password))
```

**Do not hand the listener a pooled connection.** `LISTEN` is session state, so the listener must keep
its connection for as long as it runs. A pool sees that as a leak, and any housekeeping that recycles
the connection silently drops the subscriptions. Postgres also holds notifications back while the
receiving session is inside a transaction, so the connection must stay in autocommit and nothing else
may run statements on it. In a Spring Boot service, `dataSource::getConnection` is HikariCP: use option
2 with the same URL, username and password the pool was configured from (`DataSourceProperties`).

In option 1 the library adds `tcpKeepAlive=true`, `ApplicationName=pg-notify` and `loginTimeout=10`
unless your properties set them. In option 2 those are your call; the listener still sets autocommit
and a network timeout itself.

**PgBouncer in transaction or statement pooling mode breaks `LISTEN`.** Each transaction may land on a
different server backend, so the `LISTEN` is registered on one and later notifications arrive at
another. Point the listener at Postgres directly or at a PgBouncer pool in session mode. Publishing
works behind any pooler, because `pg_notify` runs inside your normal transaction.

## Channel names and payloads

Channel names are matched byte for byte and are **case-sensitive**. The listener always quotes the
identifier, so `listen("Orders")` receives `pg_notify('Orders', ...)` but not an unquoted `NOTIFY Orders`
typed into psql, which Postgres folds to `orders`. Use lowercase names unless you have a reason not to.

Names must be non-empty, at most 63 bytes of UTF-8 and free of control characters. Anything else is
rejected with `IllegalArgumentException` before any SQL runs. Postgres silently truncates longer
identifiers, which would subscribe you to the wrong name, so the limit is enforced up front.

Payloads must be shorter than 8000 bytes; `PgNotifier.notify` throws `PayloadTooLargeException` at
8000 bytes of UTF-8 or more, before touching your transaction. The check assumes a UTF-8
`server_encoding`. A `null` payload is sent as the empty string. Within one transaction Postgres
collapses notifications with identical channel and payload into a single delivery, so send an
identifier rather than a bare "changed" if you need to count.

## Threads, ordering and failures

Each listener has one daemon thread that owns the connection and does nothing but read
notifications. Handlers run on a separate executor, by default a single daemon thread the listener
owns, so a slow handler never delays the read loop.

- **Per-channel ordering** holds on any executor. Notifications on one channel are handled one at a
  time in the order Postgres delivered them, even if you supply a thread pool. Different channels
  proceed independently on a pool.
- **Multiple handlers** on one channel run in registration order for each notification.
- **Handler exceptions**, including `Error`s, are logged at WARN and ignored. The next notification is
  delivered as usual and the connection is unaffected.
- **Queues are unbounded.** Delivery is already at-most-once, and blocking the reader would only move
  the backlog into Postgres' own notification queue.
- `close()` waits for the listener thread, then lets queued handler work finish for up to
  `shutdownTimeout` before interrupting it. A supplied executor is never shut down by the library.

## Reconnect and dead-connection detection

The listener never gives up; `close()` is the only exit. On any failure it moves to `RECONNECTING`,
calls `onDisconnected`, waits a random delay between zero and the current cap (full jitter, cap
doubling from `initial` up to `max`), and tries again. The first success after `start()` calls
`onConnected`; every later one calls `onReconnected`.

A connection can die without any packet reaching you. Once the connection has been idle for
`healthCheckInterval` the listener sends `SELECT 1`, bounded by `networkTimeout`, so a dead peer is
noticed within roughly the sum of the two. Incoming notifications count as traffic and defer the check.

| Builder option | Default | Meaning |
|---|---|---|
| `pollTimeout` | 1 s | Blocking-read length; bounds how long `close()` and a new `listen()` take to act |
| `healthCheckInterval` | 30 s | Idle time before a `SELECT 1` probe |
| `networkTimeout` | 10 s | Socket read timeout for everything except the poll |
| `backoff(initial, max)` | 500 ms, 30 s | Reconnect delay caps |
| `shutdownTimeout` | 10 s | Grace for queued handler work on `close()` |
| `handlerExecutor` | owned single thread | Where handlers run |

`state()` reports `NEW`, `CONNECTING`, `LISTENING`, `RECONNECTING` or `CLOSED`. `awaitListening(timeout)`
blocks until `LISTENING`, `CLOSED` or the timeout, and returns whether the listener is listening.

## Logging

Everything logs under `se.docksidelabs.pgnotify` via slf4j. INFO on connect and reconnect (the
reconnect line includes the downtime), WARN on each failed attempt and on handler exceptions, DEBUG
for the rest. Bring your own binding.

## Contributing

Formatting is enforced by [Spotless](https://github.com/diffplug/spotless) with
[google-java-format](https://github.com/google/google-java-format). `mvn verify` fails on unformatted
code; run `mvn spotless:apply` to fix it. IntelliJ users can install the google-java-format plugin so the
IDE and the build agree.

Tests run against a real Postgres (and a Toxiproxy for the dead-peer case) through Testcontainers, so
Docker is required for `mvn verify`. There are no mocks of the JDBC driver, and there will not be.

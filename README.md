# pg-notify

Postgres `LISTEN`/`NOTIFY` for the JVM, done carefully. If your service already has Postgres, its
instances can signal each other, for example to invalidate caches, without adding a message broker.

Runtime dependencies: pgjdbc and slf4j-api. Requires Java 17 or later. Licensed under Apache-2.0.

## How it works

Postgres has a built-in pub/sub. Any database session can subscribe to a named **channel** with
`LISTEN`, and any transaction can send a short string, the **payload**, to a channel with
`pg_notify(channel, payload)`. When that transaction commits, Postgres hands the string to every
session currently listening on the channel. Nothing is stored: a session that is not listening at that
moment never hears about it.

That makes two sides of your application:

- **Publishing** is one extra statement in the transaction that changes the data. It runs on whatever
  connection that transaction already uses, pooled or not, and is delivered only if the transaction
  commits.
- **Listening** needs one connection that does nothing else, kept open for the life of the service,
  because the subscription lives in that connection's session. If the connection dies, so does the
  subscription, and everything sent in the meantime is lost.

The two statements are trivial. The library does the parts around the listening side that are easy to
get wrong: owning that one connection, noticing when it has silently died, reconnecting with backoff
and subscribing again, telling you what window you missed, running your handlers off the connection
thread in order, and quoting channel names so they cannot be mistaken for SQL.

### What it is for

- **Cache invalidation.** Your service runs as several instances, each with its own in-memory cache.
  One instance updates a row, perhaps because it consumed a queue message that only it received, and
  the others keep serving the old value until their cache entry expires. A notification tells every
  instance to drop that entry now.
- **Waking job workers.** Workers poll a jobs table with `SKIP LOCKED`; a notification on insert wakes
  them at once, and the poll interval becomes a safety net. The table is the truth, the notification
  is the alarm clock.
- **Pushing live updates.** A row changes, and the instance holding that user's WebSocket or SSE
  connection hears about it and pushes, without every instance polling.
- **Immediate revocation.** A session is revoked or a key rotated, and every instance drops it from its
  local lookup now rather than at the next TTL.
- **Publishing from a trigger.** A database trigger calls `pg_notify`, so writes from any application,
  or a DBA in psql, notify too.

## Read this first: delivery is at-most-once

Anything sent while your listener is not connected is gone, and the listener cannot tell you what it
missed. Every outage, however short, is a gap. So every application must handle the reconnect:

```java
PgListener listener = PgListener.builder(url, props)
    .listen("products", notification -> cache.invalidate(notification.payload()))
    .onReconnect(event -> cache.invalidateAll())   // <- the one callback you must think about
    .build();
```

`onReconnect` runs after the listener has subscribed again and before any notification from the new
connection is dispatched. The `ReconnectEvent` gives you `downtime()`, which starts at the last moment
the old connection was known to be alive and therefore errs on the long side, and the number of
attempts. Resynchronise whatever the notifications were keeping fresh.

If you cannot afford to miss anything, do not let the notification carry the fact: write it to a table
and let the notification say only "there is something new, go look". The consumer reads the table on
every notification and on every reconnect, so a lost notification costs latency, not data.

Outages usually come from the listener's connection being dropped; see
[The listener's connection](#the-listeners-connection) for how to give it a proper one and
[Why listener connections die](#why-listener-connections-die-and-what-happens-then) for what happens
when it goes.

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
        .listen("products", notification -> cache.invalidate(notification.payload()))
        .onReconnect(event -> cache.invalidateAll())
        .build()) {
  listener.start();                                     // non-blocking; connects on its own thread
  listener.awaitListening(Duration.ofSeconds(10));      // optional startup gate
  // ... run your service ...
}                                                       // close() stops the thread and the connection
```

Publish, inside the transaction that made the change, on that transaction's connection:

```java
// plain JDBC
try (Connection c = dataSource.getConnection()) {       // your normal, pooled connection
  c.setAutoCommit(false);
  update(c, productId);
  PgNotifier.notify(c, "products", productId);          // delivered on commit, dropped on rollback
  c.commit();
}

// Jdbi: the handle's connection is the transaction's connection
jdbi.useTransaction(handle -> {
  handle.createUpdate("update products set price = :p where id = :id")
      .bind("p", price).bind("id", productId).execute();
  PgNotifier.notify(handle.getConnection(), "products", productId);
});

// Spring Data JPA with Hibernate: doWork runs on the connection the transaction is using
@Transactional
public void reprice(UUID productId, BigDecimal price) {
  Product product = products.findById(productId).orElseThrow();
  product.setPrice(price);
  entityManager.unwrap(Session.class)
      .doWork(connection -> PgNotifier.notify(connection, "products", productId.toString()));
}
```

With `JdbcTemplate` in a `@Transactional` method, `DataSourceUtils.getConnection(dataSource)` is the
transaction's connection. `PgNotifier.notify` borrows the connection for one statement and never closes
it.

## Handlers

A handler is one method, `handle(Notification)`, registered per channel. The library issues the
`LISTEN`s and routes each notification to the handlers of its channel, so you never switch on the
channel name yourself. `Notification` has `channel()`, `payload()` and `senderPid()`.

```java
PgListener listener = PgListener.builder(url, props)
    .listen("orders", notification -> orderCache.evict(UUID.fromString(notification.payload())))
    .listen("customers", notification -> customerCache.evict(notification.payload()))
    .listen("config", notification -> settings.reload())   // payload unused
    .onReconnect(event -> { orderCache.clear(); customerCache.clear(); settings.reload(); })
    .build();
```

- **One channel is handled in order.** Two `orders` notifications run one after the other, in the order
  Postgres delivered them, on any executor.
- **Channels are independent.** `orders` and `customers` can run at the same time if you supply an
  executor with more than one thread (`handlerExecutor`). The default is a single thread the listener
  owns, so everything runs one at a time, still in order per channel.
- **Handlers run off the connection thread.** A slow handler delays its own channel, never the
  connection. Handler queues are unbounded; blocking the reader would only move the backlog into
  Postgres.
- **A handler that throws** is logged at WARN and the next notification proceeds. Only
  `VirtualMachineError`s propagate. The connection is unaffected either way.
- **Several handlers on one channel** run in registration order for each notification.

Subscribing after `start()` works the same way and can be undone:

```java
Subscription sub = listener.listen("tenant_" + tenantId, notification -> tenantCache.evict(tenantId));
sub.close();   // removes the handler; UNLISTEN once the channel has no handlers left
```

Postgres has no wildcard `LISTEN`: a channel per entity type is the norm, a channel per entity is for
cases like the tenant above. The payload is a string with no type, so the usual shape is an id you look
up, or nothing at all when "reload" is the whole message.

## Sending a record as JSON

The payload is a string, so a structured message is whatever you serialise into it. With Jackson and
a record:

```java
record PriceChanged(UUID productId, BigDecimal price) {}

ObjectMapper mapper = new ObjectMapper();

// publish
PgNotifier.notify(connection, "prices", mapper.writeValueAsString(new PriceChanged(id, price)));

// listen
.listen("prices", notification -> {
  PriceChanged change = mapper.readValue(notification.payload(), PriceChanged.class);
  priceCache.put(change.productId(), change.price());
})
```

If `readValue` throws, the library treats it like any other exception from a handler: it logs a
WARN with the channel and payload length, drops that one notification, and carries on with the next.
Nothing is retried and the connection is unaffected. Three things to keep in mind:

- **Size is enforced, not just advised.** `PgNotifier.notify` throws `PayloadTooLargeException` at
  8000 bytes of UTF-8, before your transaction is touched. Records grow over time, so prefer an id plus
  a lookup when the data is large, or must be current when it is read rather than when it was sent.
- **Old instances read new payloads** during a rolling deploy. Only add fields, never rename or remove
  them, and configure the mapper to ignore unknown properties so an old instance skips what it does not
  know instead of failing.
- **The wire has no type.** Anything can be sent to a channel from a trigger, psql or another language.
  The record is a convention between the code you control, not a guarantee.

The library stops at strings on purpose: since the wire has no type, the compiler can only check code
that shares a constant, and that constant is a few lines of your own code with your own mapper:

```java
record Channel<T>(String name, Class<T> type) {
  void notify(Connection c, T value) throws SQLException, JsonProcessingException {
    PgNotifier.notify(c, name, mapper.writeValueAsString(value));
  }
  Subscription listen(PgListener listener, Consumer<T> handler) {
    return listener.listen(name, n -> handler.accept(mapper.readValue(n.payload(), type)));
  }
}

static final Channel<PriceChanged> PRICES = new Channel<>("prices", PriceChanged.class);

PRICES.notify(connection, new PriceChanged(id, price));
PRICES.listen(listener, change -> priceCache.put(change.productId(), change.price()));
```

## Connection callbacks

A `PgListener` owns one connection, and a `ConnectionListener` reports that connection's life. It is
per listener, not per handler or channel:

```java
.connectionListener(new ConnectionListener() {
  @Override public void onConnected() { ... }                        // first successful subscribe
  @Override public void onReconnected(ReconnectEvent event) { ... }  // every later one; resync here
  @Override public void onDisconnected(Throwable cause) { ... }      // lost, or an attempt failed
})
```

`onReconnect(...)` on the builder is shorthand for a listener that only implements `onReconnected`.
Callbacks run on the connection thread before polling resumes, so keep them short. `onDisconnected`
fires once per failed attempt during an outage, so expect several in a row.

## The listener's connection

The listener needs one connection of its own, kept for its whole lifetime. Two ways to give it one:

```java
// 1. Let the library open it through DriverManager
PgListener.builder(jdbcUrl, properties)

// 2. Open it yourself; called on start and after every loss
PgListener.builder(() -> DriverManager.getConnection(url, user, password))
```

Option 1 adds `tcpKeepAlive=true`, `ApplicationName=pg-notify` (so you can find the session in
`pg_stat_activity`) and `loginTimeout=10` unless your properties set them. Option 2 is for when opening
a connection is more than URL plus password: rotated passwords, IAM tokens, custom SSL. The provider is
called on every reconnect, so it can fetch fresh credentials each time.

**Never a pooled connection.** A pool lends connections out and expects them back; the listener keeps
its forever. A HikariCP `DataSource` cannot give you a dedicated connection at all. Take the
coordinates from it instead, in Spring Boot from `DataSourceProperties`:

```java
props.setProperty("user", ds.determineUsername());
props.setProperty("password", ds.determinePassword());
PgListener.builder(ds.determineUrl(), props)
```

Holding one Hikari connection forever does run, but it occupies a pool slot for good, trips leak
detection, and fights the pool on every reconnect. Three lines of properties are cheaper.

**Poolers in front of Postgres: PgBouncer, RDS Proxy and the like.** Three cases:

| Side | Through the pooler | Works |
|---|---|---|
| Publisher | any mode | yes, `pg_notify` is part of your normal transaction |
| Listener | session pooling mode | yes |
| Listener | transaction or statement pooling mode | **no**: each transaction may land on a different server connection, so the `LISTEN` registers on one and notifications arrive at another. The listener sees nothing, or a random subset, from day one |

If your application pool goes through such a pooler, route the listener's one connection directly to
Postgres. It is one connection, so it costs nothing to send it a different way.

## Why listener connections die, and what happens then

A listening connection is the most idle connection you own: it sends nothing for hours unless a
notification happens to come through. Idle connections are what NAT gateways, firewalls and shared
database hosts reap. Typical causes: a middlebox dropping the TCP mapping silently after some minutes
of silence; `idle_session_timeout` or `idle_in_transaction_session_timeout` set by a DBA; failovers,
patching, or a manual kill.

The listener handles all of them the same way. Once the connection has been idle for
`healthCheckInterval` it sends `SELECT 1`, bounded by `networkTimeout`, which both keeps middleboxes
happy and notices a dead peer within roughly the sum of the two. On any failure it moves to
`RECONNECTING`, calls `onDisconnected`, waits a random delay between zero and the current cap (full
jitter, cap doubling from `initial` up to `max`), and tries again. It never gives up; `close()` is the
only exit. Every such episode is still a window of lost notifications, which is what `onReconnect` is
for.

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
`close()` waits for the connection thread, then lets queued handler work finish for up to
`shutdownTimeout` before interrupting it. A supplied executor is never shut down by the library.

## Channel names and payloads

Channel names are matched byte for byte and are **case-sensitive**. The listener always quotes the
identifier, so `listen("Orders")` receives `pg_notify('Orders', ...)` but not an unquoted `NOTIFY Orders`
typed into psql, which Postgres folds to `orders`. Use lowercase names unless you have a reason not to.

Names must be non-empty, at most 63 bytes of UTF-8 and free of control characters; anything else is
rejected with `IllegalArgumentException` before any SQL runs. Postgres silently truncates longer
identifiers, which would subscribe you to the wrong name.

Payloads must be shorter than 8000 bytes; `PgNotifier.notify` throws `PayloadTooLargeException` at
8000 bytes of UTF-8 or more, before touching your transaction. A `null` payload is sent as the empty
string. Within one transaction Postgres collapses notifications with identical channel and payload
into a single delivery, so send an identifier rather than a bare "changed" if you need to count.

## Logging

Everything logs under `se.docksidelabs.pgnotify` via slf4j. INFO on connect and reconnect (the
reconnect line includes the downtime), WARN on each failed attempt and on handler exceptions, DEBUG
for the rest. Bring your own binding.

## Contributing

Formatting is enforced by [Spotless](https://github.com/diffplug/spotless) with
[google-java-format](https://github.com/google/google-java-format), which needs JDK 21+ to run; on
JDK 17 the check is skipped. `mvn verify` fails on unformatted code; run `mvn spotless:apply` to fix it.

Tests run against a real Postgres (and a Toxiproxy for the dead-peer case) through Testcontainers, so
Docker is required for `mvn verify`. There are no mocks of the JDBC driver, and there will not be.

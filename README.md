# pg-notify

Lightweight Postgres `LISTEN`/`NOTIFY` for the JVM. If your service already has Postgres, you can do
cache-invalidation style signalling between instances without adding a message broker.

The library handles the parts that are easy to get wrong: a dedicated connection the listener owns,
reconnect with exponential backoff and re-`LISTEN`, dead-connection detection, safe channel quoting,
per-channel ordered dispatch off the listener thread, and a publisher that rides your own transaction.

**Delivery is at-most-once.** Notifications sent while the listener is disconnected are lost. Register an
`onReconnect` callback and resync there (for a cache: flush it).

Status: pre-release, under active development. Not yet published to Maven Central.

Requirements: Java 17+, pgjdbc, slf4j-api. Licensed under Apache-2.0.

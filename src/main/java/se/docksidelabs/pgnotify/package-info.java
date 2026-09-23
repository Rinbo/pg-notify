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

/**
 * Postgres {@code LISTEN}/{@code NOTIFY} for services that already have Postgres and want
 * signalling between instances without a message broker.
 *
 * <p>Two entry points: {@link se.docksidelabs.pgnotify.PgListener} receives notifications on a
 * dedicated connection, and {@link se.docksidelabs.pgnotify.PgNotifier} sends them on a connection
 * you provide, inside your own transaction.
 *
 * <h2>Delivery is at-most-once</h2>
 *
 * <p>Postgres does not persist notifications. Anything sent while a listener is not connected is
 * gone, and the listener cannot tell you what it missed. If the application must be correct across
 * a reconnect, for example a cache that must not serve stale entries, register a reconnect callback
 * and resynchronise there. If you need durability, write to a table and use notifications only as a
 * wake-up.
 *
 * <h2>Why the listener needs its own connection</h2>
 *
 * <p>{@code LISTEN} is session state, so the listener must own its connection for as long as it
 * runs. A pooled connection is wrong for three reasons: the pool hands it to other borrowers, who
 * inherit the subscriptions and the queued notifications; the listener parks the connection in a
 * blocking read, which a pool treats as a leak or, worse, uses concurrently; and Postgres holds
 * notifications back while the receiving session is inside a transaction, so the connection must
 * stay in autocommit and nobody else may run statements on it. Give the listener a JDBC URL and
 * properties, or a supplier that opens a fresh connection each time it is called.
 *
 * <h2>PgBouncer</h2>
 *
 * <p>PgBouncer in transaction or statement pooling mode breaks {@code LISTEN}: the subscription
 * lands on one server backend and later notifications arrive at another. Point the listener at
 * Postgres directly or at a session-mode pool. Publishing works behind any pooler.
 *
 * <h2>Channel names</h2>
 *
 * <p>Names are matched byte for byte and are case-sensitive. The listener always quotes the
 * identifier, so {@code listen("Orders")} receives {@code pg_notify('Orders', ...)} but not an
 * unquoted {@code NOTIFY Orders} typed into psql, which Postgres folds to {@code orders}. Names
 * must be non-empty, at most 63 bytes of UTF-8 and free of control characters.
 *
 * <h2>Threads</h2>
 *
 * <p>Each listener has one daemon thread that owns the connection and only reads notifications.
 * Handlers run on a separate executor, by default one daemon thread owned by the listener, so a
 * slow handler never delays the read loop. Notifications on one channel are delivered in order, one
 * at a time, even on a multi-threaded executor. Handler exceptions are logged and never affect
 * delivery.
 */
package se.docksidelabs.pgnotify;

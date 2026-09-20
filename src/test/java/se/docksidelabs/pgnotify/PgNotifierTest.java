package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * Exercises the publisher against a real Postgres, observing delivery through a raw pgjdbc LISTEN so
 * these tests do not depend on the listener half of the library.
 */
class PgNotifierTest {

    private Connection publisher;
    private Connection subscriber;
    private String channel;

    @BeforeEach
    void connect() throws SQLException {
        publisher = PostgresSupport.connect();
        subscriber = PostgresSupport.connect();
        channel = "t_" + UUID.randomUUID().toString().replace("-", "");
        listen(channel);
    }

    @AfterEach
    void disconnect() throws SQLException {
        publisher.close();
        subscriber.close();
    }

    @Test
    void deliversOnAutocommit() throws SQLException {
        PgNotifier.notify(publisher, channel, "hello");

        List<PGNotification> got = await(1);
        assertThat(got).hasSize(1);
        assertThat(got.get(0).getName()).isEqualTo(channel);
        assertThat(got.get(0).getParameter()).isEqualTo("hello");
        assertThat(got.get(0).getPID()).isEqualTo(publisher.unwrap(PGConnection.class).getBackendPID());
    }

    @Test
    void deliversOnCommitNotBefore() throws SQLException {
        publisher.setAutoCommit(false);
        PgNotifier.notify(publisher, channel, "pending");

        assertThat(poll(300)).isEmpty();

        publisher.commit();
        assertThat(await(1)).extracting(PGNotification::getParameter).containsExactly("pending");
    }

    @Test
    void droppedOnRollback() throws SQLException {
        publisher.setAutoCommit(false);
        PgNotifier.notify(publisher, channel, "doomed");
        publisher.rollback();

        assertThat(poll(300)).isEmpty();
    }

    @Test
    void nullPayloadIsSentAsEmptyString() throws SQLException {
        PgNotifier.notify(publisher, channel, null);

        assertThat(await(1)).extracting(PGNotification::getParameter).containsExactly("");
    }

    @Test
    void acceptsPayloadOf7999Bytes() throws SQLException {
        String payload = "x".repeat(PgNotifier.MAX_PAYLOAD_BYTES);
        PgNotifier.notify(publisher, channel, payload);

        assertThat(await(1)).extracting(PGNotification::getParameter).containsExactly(payload);
    }

    @Test
    void rejectsPayloadOf8000BytesBeforeAnySql() throws SQLException {
        publisher.setAutoCommit(false);
        String payload = "x".repeat(PgNotifier.MAX_PAYLOAD_BYTES + 1);

        assertThatThrownBy(() -> PgNotifier.notify(publisher, channel, payload))
                .isInstanceOf(PayloadTooLargeException.class)
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(e -> {
                    PayloadTooLargeException ptl = (PayloadTooLargeException) e;
                    assertThat(ptl.actualBytes()).isEqualTo(8000);
                    assertThat(ptl.maxBytes()).isEqualTo(7999);
                });

        // Transaction is still usable: nothing was sent to the server.
        try (Statement st = publisher.createStatement()) {
            st.execute("SELECT 1");
        }
        publisher.commit();
        assertThat(poll(300)).isEmpty();
    }

    @Test
    void payloadLimitIsMeasuredInUtf8Bytes() throws SQLException {
        String fits = "å".repeat(3999); // 7998 bytes
        PgNotifier.notify(publisher, channel, fits);
        assertThat(await(1)).extracting(PGNotification::getParameter).containsExactly(fits);

        String tooLong = "å".repeat(4000); // 8000 bytes
        assertThatThrownBy(() -> PgNotifier.notify(publisher, channel, tooLong))
                .isInstanceOf(PayloadTooLargeException.class);
    }

    @Test
    void rejectsInvalidChannelName() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PgNotifier.notify(publisher, "", "x"));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PgNotifier.notify(publisher, "y".repeat(64), "x"));
    }

    @Test
    void channelNamesAreCaseSensitiveAndQuotingPreservesCase() throws SQLException {
        String mixed = "Mixed_" + channel;
        listen(mixed);

        PgNotifier.notify(publisher, mixed, "to-mixed");
        assertThat(await(1)).extracting(PGNotification::getName).containsExactly(mixed);

        PgNotifier.notify(publisher, mixed.toLowerCase(), "to-lower");
        assertThat(poll(300)).isEmpty();
    }

    @Test
    void quotedListenHandlesHostileNames() throws SQLException {
        String hostile = "x\"; DROP TABLE nothing; --";
        listen(hostile);

        PgNotifier.notify(publisher, hostile, "safe");
        assertThat(await(1)).extracting(PGNotification::getName).containsExactly(hostile);
    }

    private void listen(String name) throws SQLException {
        try (Statement st = subscriber.createStatement()) {
            st.execute("LISTEN " + ChannelNames.quote(ChannelNames.validate(name)));
        }
    }

    private List<PGNotification> await(int count) throws SQLException {
        List<PGNotification> all = new ArrayList<>();
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (all.size() < count && System.nanoTime() < deadline) {
            all.addAll(poll(200));
        }
        return all;
    }

    private List<PGNotification> poll(int millis) throws SQLException {
        PGNotification[] ns = subscriber.unwrap(PGConnection.class).getNotifications(millis);
        List<PGNotification> out = new ArrayList<>();
        if (ns != null) {
            for (PGNotification n : ns) {
                out.add(n);
            }
        }
        return out;
    }
}

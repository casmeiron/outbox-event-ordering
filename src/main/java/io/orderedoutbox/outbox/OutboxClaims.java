package io.orderedoutbox.outbox;

import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.hibernate.Session;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Short transactions that lease outbox rows and later mark them published.
 * Kafka I/O happens outside these transactions. A crash after the broker ack
 * and before the mark republishes the row once the lease expires; consumers
 * treat that second copy as a duplicate.
 */
@ApplicationScoped
public class OutboxClaims {

    private static final String CLAIM_COLUMNS = """
            SELECT id, event_id, aggregate_id, aggregate_version, event_type, payload
            FROM outbox_event
            """;

    @Inject
    EntityManager entityManager;

    public List<ClaimedOutbox> claimNaive(int limit, int workerSlot) {
        String sql = CLAIM_COLUMNS + """
                WHERE published_at IS NULL
                  AND (claimed_by IS NULL OR claimed_at < NOW() - make_interval(secs => ?))
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """;
        return QuarkusTransaction.requiringNew().call(() -> jdbc(connection -> {
            List<ClaimedOutbox> claimed = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, leaseSeconds());
                statement.setInt(2, limit);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        claimed.add(lease(connection, rows, workerSlot));
                    }
                }
            }
            return claimed;
        }));
    }

    /**
     * Oldest unpublished version of each aggregate this worker owns. A higher
     * version is not leased while a lower one is still unpublished, including
     * when that lower one is already leased by an in-flight send.
     */
    public List<ClaimedOutbox> claimSafe(int workerId, int workers, int limit) {
        String sql = """
                SELECT e.id, e.event_id, e.aggregate_id, e.aggregate_version, e.event_type, e.payload
                FROM outbox_event e
                WHERE e.published_at IS NULL
                  AND (e.claimed_by IS NULL OR e.claimed_at < NOW() - make_interval(secs => ?))
                  AND MOD(e.shard, ?) = ?
                  AND e.aggregate_version = (
                      SELECT MIN(n.aggregate_version)
                      FROM outbox_event n
                      WHERE n.aggregate_id = e.aggregate_id
                        AND n.published_at IS NULL
                  )
                ORDER BY e.aggregate_id
                LIMIT ?
                FOR UPDATE OF e SKIP LOCKED
                """;
        return QuarkusTransaction.requiringNew().call(() -> jdbc(connection -> {
            List<ClaimedOutbox> claimed = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, leaseSeconds());
                statement.setInt(2, workers);
                statement.setInt(3, workerId);
                statement.setInt(4, limit);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        claimed.add(lease(connection, rows, workerId));
                    }
                }
            }
            return claimed;
        }));
    }

    public void markPublished(ClaimedOutbox claimed, String kafkaKey, OutboxMode mode) {
        Instant now = Instant.now();
        QuarkusTransaction.requiringNew().run(() -> jdbc(connection -> {
            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE outbox_event
                    SET published_at = ?, claimed_by = NULL
                    WHERE id = ? AND claimed_by = ? AND published_at IS NULL
                    """)) {
                update.setTimestamp(1, Timestamp.from(now));
                update.setLong(2, claimed.id());
                update.setString(3, claimed.claimToken());
                if (update.executeUpdate() != 1) {
                    throw new IllegalStateException("lost the outbox lease for row " + claimed.id());
                }
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO relay_log
                        (aggregate_id, aggregate_version, kafka_key, mode, worker_slot, published_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """)) {
                insert.setObject(1, claimed.aggregateId());
                insert.setLong(2, claimed.aggregateVersion());
                insert.setString(3, kafkaKey);
                insert.setString(4, mode.name());
                insert.setInt(5, claimed.workerSlot());
                insert.setTimestamp(6, Timestamp.from(now));
                insert.executeUpdate();
            }
            return null;
        }));
    }

    private ClaimedOutbox lease(Connection connection, ResultSet rows, int workerSlot) throws SQLException {
        long id = rows.getLong("id");
        String token = workerSlot + "-" + UUID.randomUUID();
        try (PreparedStatement update = connection.prepareStatement("""
                UPDATE outbox_event
                SET claimed_by = ?, claimed_at = ?
                WHERE id = ?
                """)) {
            update.setString(1, token);
            update.setTimestamp(2, Timestamp.from(Instant.now()));
            update.setLong(3, id);
            update.executeUpdate();
        }
        return new ClaimedOutbox(
                id,
                rows.getObject("event_id", UUID.class),
                rows.getObject("aggregate_id", UUID.class),
                rows.getLong("aggregate_version"),
                rows.getString("event_type"),
                rows.getString("payload"),
                token,
                workerSlot);
    }

    private volatile long claimLeaseSeconds = 60;

    @Inject
    void setLease(OutboxSettings settings) {
        this.claimLeaseSeconds = Math.max(1, settings.claimLease().toSeconds());
    }

    private int leaseSeconds() {
        return (int) Math.max(1, claimLeaseSeconds);
    }

    private <T> T jdbc(SqlWork<T> work) {
        Session session = entityManager.unwrap(Session.class);
        return session.doReturningWork(connection -> {
            try {
                return work.run(connection);
            } catch (SQLException exception) {
                throw new IllegalStateException("outbox SQL failed", exception);
            }
        });
    }

    @FunctionalInterface
    private interface SqlWork<T> {
        T run(Connection connection) throws SQLException;
    }
}

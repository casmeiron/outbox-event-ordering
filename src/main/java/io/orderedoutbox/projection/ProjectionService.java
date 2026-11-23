package io.orderedoutbox.projection;

import io.orderedoutbox.domain.EventPayload;
import io.orderedoutbox.domain.OrderStatuses;
import io.orderedoutbox.messaging.EventPayloadCodec;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.hibernate.Session;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;

/**
 * Applies events to the order projection. Safe mode uses {@link VersionGate}:
 * the next version is applied, duplicates are ignored, and a gap is parked
 * until the missing version arrives and the contiguous suffix can be drained.
 * Naive mode overwrites the row with whatever arrived, which is how state
 * moves backwards.
 */
@ApplicationScoped
public class ProjectionService {

    private static final Logger LOG = Logger.getLogger(ProjectionService.class);

    @Inject
    EntityManager entityManager;

    @Inject
    VersionGate versions;

    @Inject
    EventPayloadCodec codec;

    public void applyBlindly(EventPayload event) {
        QuarkusTransaction.requiringNew().run(() -> applyBlindlyInTransaction(event));
    }

    public void applySafely(EventPayload event) {
        QuarkusTransaction.requiringNew().run(() -> applySafelyInTransaction(event));
    }

    private void applyBlindlyInTransaction(EventPayload event) {
        OrderProjection projection = lock(event.aggregateId());
        boolean backwards = projection.appliedVersion > 0 && event.aggregateVersion() < projection.appliedVersion;
        copyBusinessFields(projection, event);
        projection.status = OrderStatuses.after(event.type());
        projection.appliedVersion = event.aggregateVersion();
        projection.regressed = projection.regressed || backwards;
        projection.updatedAt = Instant.now();
        appendLog(projection, event);
        if (backwards) {
            LOG.infof("naive apply moved %s backwards to v%d (%s)",
                    event.aggregateId(), event.aggregateVersion(), event.type());
        }
    }

    private void applySafelyInTransaction(EventPayload event) {
        OrderProjection projection = lock(event.aggregateId());
        switch (versions.decide(projection.appliedVersion, event.aggregateVersion())) {
            case ApplyDecision.Accept ignored -> {
                applyForward(projection, event);
                drain(projection);
            }
            case ApplyDecision.Ignore ignored -> LOG.debugf(
                    "ignored duplicate or stale %s v%d (applied=%d)",
                    event.aggregateId(), event.aggregateVersion(), projection.appliedVersion);
            case ApplyDecision.Park park -> {
                park(event);
                LOG.infof("parked %s v%d; next expected version is %d",
                        event.aggregateId(), event.aggregateVersion(), park.expectedVersion());
            }
        }
    }

    private void drain(OrderProjection projection) {
        while (true) {
            long next = projection.appliedVersion + 1;
            ParkedEvent parked = entityManager.find(ParkedEvent.class, new ParkedEventId(projection.orderId, next));
            if (parked == null) {
                return;
            }
            EventPayload payload = codec.decode(parked.payload);
            entityManager.remove(parked);
            applyForward(projection, payload);
            LOG.infof("drained parked %s v%d", projection.orderId, payload.aggregateVersion());
        }
    }

    private void applyForward(OrderProjection projection, EventPayload event) {
        long expected = projection.appliedVersion + 1;
        if (event.aggregateVersion() != expected) {
            throw new IllegalStateException("refusing to apply v" + event.aggregateVersion()
                    + " of " + event.aggregateId() + " because v" + expected + " is next");
        }
        copyBusinessFields(projection, event);
        projection.status = OrderStatuses.after(event.type());
        projection.appliedVersion = event.aggregateVersion();
        projection.updatedAt = Instant.now();
        appendLog(projection, event);
    }

    private void park(EventPayload event) {
        ParkedEventId id = new ParkedEventId(event.aggregateId(), event.aggregateVersion());
        if (entityManager.find(ParkedEvent.class, id) != null) {
            return;
        }
        ParkedEvent parked = new ParkedEvent();
        parked.id = id;
        parked.eventType = event.type();
        parked.payload = codec.encode(event);
        parked.parkedAt = Instant.now();
        entityManager.persist(parked);
    }

    private void copyBusinessFields(OrderProjection projection, EventPayload event) {
        if (event.customerRef() != null) {
            projection.customerRef = event.customerRef();
        }
        if (event.paymentRef() != null) {
            projection.paymentRef = event.paymentRef();
        }
        if (event.trackingNumber() != null) {
            projection.trackingNumber = event.trackingNumber();
        }
    }

    private void appendLog(OrderProjection projection, EventPayload event) {
        ProjectionLogEntry entry = new ProjectionLogEntry();
        entry.orderId = projection.orderId;
        entry.aggregateVersion = event.aggregateVersion();
        entry.eventType = event.type();
        entry.statusAfter = projection.status.name();
        entry.appliedAt = Instant.now();
        entityManager.persist(entry);
    }

    private OrderProjection lock(UUID orderId) {
        long key = orderId.getMostSignificantBits() ^ orderId.getLeastSignificantBits();
        Session session = entityManager.unwrap(Session.class);
        session.doWork(connection -> lockAggregate(connection, key));
        OrderProjection projection = entityManager.find(OrderProjection.class, orderId);
        if (projection == null) {
            projection = new OrderProjection();
            projection.orderId = orderId;
            projection.appliedVersion = 0;
            projection.regressed = false;
            projection.updatedAt = Instant.now();
            entityManager.persist(projection);
            entityManager.flush();
        }
        return projection;
    }

    private static void lockAggregate(Connection connection, long key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
            statement.setLong(1, key);
            statement.execute();
        }
    }
}

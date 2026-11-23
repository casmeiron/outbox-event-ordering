package io.orderedoutbox.query;

import io.orderedoutbox.api.ApiException;
import io.orderedoutbox.api.Views.AppliedEvent;
import io.orderedoutbox.api.Views.OrderView;
import io.orderedoutbox.api.Views.ProjectionView;
import io.orderedoutbox.api.Views.Summary;
import io.orderedoutbox.api.Views.WrittenEvent;
import io.orderedoutbox.domain.Order;
import io.orderedoutbox.outbox.OutboxEvent;
import io.orderedoutbox.outbox.OutboxSettings;
import io.orderedoutbox.outbox.RelayLogEntry;
import io.orderedoutbox.projection.OrderProjection;
import io.orderedoutbox.projection.ParkedEvent;
import io.orderedoutbox.projection.ProjectionLogEntry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class OrderQuery {

    @Inject
    OutboxSettings settings;

    @Transactional
    public OrderView inspect(UUID orderId) {
        Order order = Order.findById(orderId);
        if (order == null) {
            throw new ApiException.NotFound("order " + orderId + " was not found");
        }
        ProjectionView projection = projection(orderId);
        List<WrittenEvent> written = writtenEvents(orderId);
        List<RelayLogEntry> relay = RelayLogEntry.list("aggregateId = ?1 order by id", orderId);
        return new OrderView(
                order.id,
                order.customerRef,
                order.status.name(),
                order.version,
                projection.status(),
                projection.appliedVersion(),
                projection.regressed(),
                paymentRef(orderId),
                trackingNumber(orderId),
                written,
                relay.stream().map(entry -> entry.aggregateVersion).toList(),
                relay.stream().map(entry -> entry.kafkaKey).toList(),
                appliedEvents(orderId),
                projection.parkedVersions(),
                OutboxEvent.count("aggregateId = ?1 and publishedAt is null", orderId));
    }

    @Transactional
    public ProjectionView projection(UUID orderId) {
        OrderProjection projection = OrderProjection.findById(orderId);
        List<ProjectionLogEntry> applied = ProjectionLogEntry.list("orderId = ?1 order by id", orderId);
        List<ParkedEvent> parked = ParkedEvent.list("id.orderId = ?1 order by id.aggregateVersion", orderId);
        if (projection == null) {
            return new ProjectionView(null, 0, false, List.of(), parked.stream().map(row -> row.id.aggregateVersion).toList());
        }
        return new ProjectionView(
                projection.status == null ? null : projection.status.name(),
                projection.appliedVersion,
                projection.regressed,
                applied.stream().map(entry -> entry.aggregateVersion).toList(),
                parked.stream().map(row -> row.id.aggregateVersion).toList());
    }

    @Transactional
    public String payload(UUID orderId, long version) {
        OutboxEvent event = OutboxEvent.find("aggregateId = ?1 and aggregateVersion = ?2", orderId, version).firstResult();
        if (event == null) {
            throw new ApiException.NotFound("outbox event " + orderId + " v" + version + " was not found");
        }
        return event.payload;
    }

    @Transactional
    public Summary summary() {
        return new Summary(
                settings.mode().name().toLowerCase(),
                settings.paused(),
                Order.count(),
                OutboxEvent.count("publishedAt is null"),
                OrderProjection.count("regressed = true"));
    }

    private List<WrittenEvent> writtenEvents(UUID orderId) {
        List<OutboxEvent> rows = OutboxEvent.list("aggregateId = ?1 order by aggregateVersion", orderId);
        return rows.stream()
                .map(row -> new WrittenEvent(row.aggregateVersion, row.eventType, row.sourceCommitTime, row.publishedAt != null))
                .toList();
    }

    private List<AppliedEvent> appliedEvents(UUID orderId) {
        List<ProjectionLogEntry> rows = ProjectionLogEntry.list("orderId = ?1 order by id", orderId);
        return rows.stream()
                .map(row -> new AppliedEvent(row.aggregateVersion, row.eventType, row.statusAfter))
                .toList();
    }

    private String paymentRef(UUID orderId) {
        OrderProjection projection = OrderProjection.findById(orderId);
        return projection == null ? null : projection.paymentRef;
    }

    private String trackingNumber(UUID orderId) {
        OrderProjection projection = OrderProjection.findById(orderId);
        return projection == null ? null : projection.trackingNumber;
    }
}

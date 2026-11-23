package io.orderedoutbox.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.orderedoutbox.domain.EventPayload;
import io.orderedoutbox.domain.EventPayloads;
import io.orderedoutbox.domain.Order;
import io.orderedoutbox.domain.OrderEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import java.time.Instant;
import java.util.UUID;

/** Writes the outbox row in the caller's transaction, which also changed the order. */
@ApplicationScoped
public class OutboxAppender {

    @Inject
    EntityManager entityManager;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    OutboxSettings settings;

    public void append(Order order, OrderEvent event) {
        Instant sourceCommitTime = Instant.now();
        UUID eventId = UUID.randomUUID();
        EventPayload payload = EventPayloads.from(event, eventId, sourceCommitTime);
        OutboxEvent row = new OutboxEvent();
        row.eventId = eventId;
        row.aggregateId = order.id;
        row.aggregateVersion = event.version();
        row.eventType = payload.type();
        row.payload = encode(payload);
        row.shard = AggregateShard.of(order.id, settings.shards());
        row.recordedAt = event.occurredAt();
        row.sourceCommitTime = sourceCommitTime;
        entityManager.persist(row);
    }

    private String encode(EventPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("could not encode outbox payload", exception);
        }
    }
}

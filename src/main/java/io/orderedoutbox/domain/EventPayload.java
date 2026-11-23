package io.orderedoutbox.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Wire format for an outbox row and the Kafka record. {@code sourceCommitTime}
 * is the application clock inside the transaction that wrote the event. It is
 * there for downstream analytics. The aggregate version is what consumers use
 * to order the stream.
 */
public record EventPayload(
        UUID eventId,
        UUID aggregateId,
        long aggregateVersion,
        String type,
        Instant occurredAt,
        Instant sourceCommitTime,
        String customerRef,
        String paymentRef,
        String trackingNumber) {
}

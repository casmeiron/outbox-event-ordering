package io.orderedoutbox.domain;

import java.time.Instant;
import java.util.UUID;

public final class EventPayloads {

    private EventPayloads() {
    }

    public static EventPayload from(OrderEvent event, UUID eventId, Instant sourceCommitTime) {
        return switch (event) {
            case OrderCreated created -> new EventPayload(
                    eventId,
                    created.orderId(),
                    created.version(),
                    "OrderCreated",
                    created.occurredAt(),
                    sourceCommitTime,
                    created.customerRef(),
                    null,
                    null);
            case PaymentConfirmed paid -> new EventPayload(
                    eventId,
                    paid.orderId(),
                    paid.version(),
                    "PaymentConfirmed",
                    paid.occurredAt(),
                    sourceCommitTime,
                    null,
                    paid.paymentRef(),
                    null);
            case OrderShipped shipped -> new EventPayload(
                    eventId,
                    shipped.orderId(),
                    shipped.version(),
                    "OrderShipped",
                    shipped.occurredAt(),
                    sourceCommitTime,
                    null,
                    null,
                    shipped.trackingNumber());
        };
    }
}

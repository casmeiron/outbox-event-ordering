package io.orderedoutbox.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Business facts for one order. The version is the aggregate sequence, assigned
 * when the order row changes, not the outbox identity column.
 */
public sealed interface OrderEvent permits OrderCreated, PaymentConfirmed, OrderShipped {

    UUID orderId();

    long version();

    Instant occurredAt();
}

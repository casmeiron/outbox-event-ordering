package io.orderedoutbox.domain;

import java.time.Instant;
import java.util.UUID;

public record PaymentConfirmed(UUID orderId, long version, Instant occurredAt, String paymentRef) implements OrderEvent {
}

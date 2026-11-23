package io.orderedoutbox.domain;

import java.time.Instant;
import java.util.UUID;

public record OrderShipped(UUID orderId, long version, Instant occurredAt, String trackingNumber) implements OrderEvent {
}

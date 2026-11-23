package io.orderedoutbox.domain;

import java.time.Instant;
import java.util.UUID;

public record OrderCreated(UUID orderId, long version, Instant occurredAt, String customerRef) implements OrderEvent {
}

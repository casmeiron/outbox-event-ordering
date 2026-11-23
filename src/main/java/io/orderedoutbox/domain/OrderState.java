package io.orderedoutbox.domain;

import java.time.Instant;
import java.util.UUID;

public record OrderState(UUID id, String customerRef, OrderStatus status, long version, Instant updatedAt) {

    public static OrderState from(Order order) {
        return new OrderState(order.id, order.customerRef, order.status, order.version, order.updatedAt);
    }
}

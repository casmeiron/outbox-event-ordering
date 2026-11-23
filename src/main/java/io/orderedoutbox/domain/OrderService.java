package io.orderedoutbox.domain;

import io.orderedoutbox.api.ApiException;
import io.orderedoutbox.outbox.OutboxAppender;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class OrderService {

    @Inject
    OutboxAppender outbox;

    @Transactional
    public OrderState create(String customerRef) {
        if (customerRef == null || customerRef.isBlank()) {
            throw new ApiException.BadRequest("customerRef is required");
        }
        Instant now = Instant.now();
        Order order = new Order();
        order.id = UUID.randomUUID();
        order.customerRef = customerRef.trim();
        order.status = OrderStatus.CREATED;
        order.version = 1;
        order.createdAt = now;
        order.updatedAt = now;
        order.persist();
        outbox.append(order, new OrderCreated(order.id, order.version, now, order.customerRef));
        return OrderState.from(order);
    }

    @Transactional
    public OrderState confirmPayment(UUID orderId, String paymentRef) {
        if (paymentRef == null || paymentRef.isBlank()) {
            throw new ApiException.BadRequest("paymentRef is required");
        }
        Order order = require(orderId);
        if (order.status != OrderStatus.CREATED) {
            throw new ApiException.Conflict("order " + orderId + " is " + order.status + ", payment requires CREATED");
        }
        Instant now = Instant.now();
        order.version = order.version + 1;
        order.status = OrderStatus.PAYMENT_CONFIRMED;
        order.updatedAt = now;
        outbox.append(order, new PaymentConfirmed(order.id, order.version, now, paymentRef.trim()));
        return OrderState.from(order);
    }

    @Transactional
    public OrderState ship(UUID orderId, String trackingNumber) {
        if (trackingNumber == null || trackingNumber.isBlank()) {
            throw new ApiException.BadRequest("trackingNumber is required");
        }
        Order order = require(orderId);
        if (order.status != OrderStatus.PAYMENT_CONFIRMED) {
            throw new ApiException.Conflict(
                    "order " + orderId + " is " + order.status + ", shipment requires PAYMENT_CONFIRMED");
        }
        Instant now = Instant.now();
        order.version = order.version + 1;
        order.status = OrderStatus.SHIPPED;
        order.updatedAt = now;
        outbox.append(order, new OrderShipped(order.id, order.version, now, trackingNumber.trim()));
        return OrderState.from(order);
    }

    private Order require(UUID orderId) {
        Order order = Order.findById(orderId);
        if (order == null) {
            throw new ApiException.NotFound("order " + orderId + " was not found");
        }
        return order;
    }
}

package io.orderedoutbox.demo;

import io.orderedoutbox.api.ApiException;
import io.orderedoutbox.domain.OrderService;
import io.orderedoutbox.domain.OrderState;
import io.orderedoutbox.outbox.OutboxRelay;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Writes every event for a batch of orders before publishers are allowed to
 * run, so the relay sees a full history and the ordering mode is what decides
 * what the consumer observes.
 */
@ApplicationScoped
public class DemoService {

    @Inject
    OutboxRelay relay;

    @Inject
    OrderService orderService;

    public List<UUID> burst(int orders, String customerPrefix) {
        if (orders < 1 || orders > 500) {
            throw new ApiException.BadRequest("orders must be between 1 and 500");
        }
        String prefix = customerPrefix == null || customerPrefix.isBlank() ? "demo" : customerPrefix.trim();
        relay.pauseAndAwait();
        try {
            List<UUID> ids = new ArrayList<>();
            for (int i = 0; i < orders; i++) {
                OrderState created = orderService.create(prefix + "-" + i);
                orderService.confirmPayment(created.id(), "pay-" + i);
                orderService.ship(created.id(), "track-" + i);
                ids.add(created.id());
            }
            return ids;
        } finally {
            relay.resume();
        }
    }
}

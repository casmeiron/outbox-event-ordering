package io.orderedoutbox.messaging;

import io.orderedoutbox.domain.EventPayload;
import io.orderedoutbox.outbox.OutboxMode;
import io.orderedoutbox.outbox.OutboxSettings;
import io.orderedoutbox.projection.ProjectionService;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class OrderEventConsumer {

    @Inject
    EventPayloadCodec codec;

    @Inject
    OutboxSettings settings;

    @Inject
    ProjectionService projection;

    @Inject
    NaiveAsyncApplier naiveAsync;

    @Inject
    ConsumptionCounter counter;

    @Incoming("order-events-in")
    @Blocking
    public void consume(String payload) {
        EventPayload event = codec.decode(payload);
        if (settings.mode() == OutboxMode.NAIVE) {
            if (settings.naiveAsyncConsumer()) {
                naiveAsync.accept(event);
            } else {
                projection.applyBlindly(event);
            }
        } else {
            projection.applySafely(event);
        }
        counter.increment();
    }
}

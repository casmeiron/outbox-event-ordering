package io.orderedoutbox.messaging;

import io.orderedoutbox.domain.EventPayload;
import io.orderedoutbox.outbox.OutboxSettings;
import io.orderedoutbox.outbox.VersionDelay;
import io.orderedoutbox.projection.ProjectionService;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Detaches the business effect from Kafka's delivery order. The consumer acks
 * as soon as the task is scheduled. Lower versions sleep longer, so a batch
 * that arrived in order can still be applied backwards.
 */
@ApplicationScoped
public class NaiveAsyncApplier {

    private static final Logger LOG = Logger.getLogger(NaiveAsyncApplier.class);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4, runnable -> {
        Thread thread = new Thread(runnable, "naive-apply");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicInteger inFlight = new AtomicInteger();

    @Inject
    ProjectionService projection;

    @Inject
    OutboxSettings settings;

    public void accept(EventPayload event) {
        long steps = VersionDelay.steps(event.aggregateVersion(), settings.pipelineDepth());
        long delayMs = steps * Math.max(0, settings.naiveConsumerStepDelay().toMillis());
        inFlight.incrementAndGet();
        scheduler.schedule(() -> {
            try {
                projection.applyBlindly(event);
            } catch (RuntimeException exception) {
                LOG.errorf(exception, "naive async apply failed for %s v%d",
                        event.aggregateId(), event.aggregateVersion());
            } finally {
                inFlight.decrementAndGet();
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    public int inFlight() {
        return inFlight.get();
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdown();
    }
}

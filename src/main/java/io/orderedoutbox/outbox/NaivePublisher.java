package io.orderedoutbox.outbox;

import io.orderedoutbox.messaging.OrderEventSender;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox by generated id and publishes whatever it claimed, with no
 * aggregate affinity. When {@code outbox.naive.stall-lower-versions} is on,
 * each in-flight send waits until a higher version of the same aggregate in
 * the same batch has been acknowledged. That stall is the reproducible form of
 * a slow worker: PaymentConfirmed reaches the broker while OrderCreated is
 * still in flight. The record key is random, and every naive record is pinned
 * to one partition so the broker keeps that already-wrong send order.
 */
@ApplicationScoped
public class NaivePublisher {

    private static final Logger LOG = Logger.getLogger(NaivePublisher.class);

    private final Object claimMonitor = new Object();
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    @Inject
    OutboxClaims claims;

    @Inject
    OutboxSettings settings;

    @Inject
    OrderEventSender sender;

    @PreDestroy
    void shutdown() {
        pool.close();
    }

    public int publishTick(int workerSlot) {
        List<ClaimedOutbox> claimed;
        synchronized (claimMonitor) {
            claimed = claims.claimNaive(settings.naiveTickSize(), workerSlot);
        }
        if (claimed.isEmpty()) {
            return 0;
        }
        publishAll(claimed);
        return claimed.size();
    }

    private void publishAll(List<ClaimedOutbox> claimed) {
        Map<UUID, NavigableMap<Long, CountDownLatch>> gates = new HashMap<>();
        for (ClaimedOutbox event : claimed) {
            gates.computeIfAbsent(event.aggregateId(), ignored -> new TreeMap<>())
                    .put(event.aggregateVersion(), new CountDownLatch(1));
        }
        for (NavigableMap<Long, CountDownLatch> versions : gates.values()) {
            if (settings.stallLowerVersions()) {
                versions.lastEntry().getValue().countDown();
            } else {
                versions.values().forEach(CountDownLatch::countDown);
            }
        }

        List<Future<?>> futures = new ArrayList<>();
        for (ClaimedOutbox event : claimed) {
            futures.add(pool.submit(() -> publishOne(event, gates.get(event.aggregateId()))));
        }
        List<Throwable> failures = new ArrayList<>();
        for (Future<?> future : futures) {
            try {
                future.get(45, TimeUnit.SECONDS);
            } catch (Exception exception) {
                failures.add(exception);
            }
        }
        if (!failures.isEmpty()) {
            IllegalStateException failed = new IllegalStateException(
                    "naive publish failed: " + failures.get(0).getMessage(), failures.get(0));
            failures.stream().skip(1).forEach(failed::addSuppressed);
            throw failed;
        }
    }

    private Void publishOne(ClaimedOutbox event, NavigableMap<Long, CountDownLatch> versions) {
        CountDownLatch gate = versions.get(event.aggregateVersion());
        try {
            if (!gate.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting to publish "
                        + event.aggregateId() + " v" + event.aggregateVersion());
            }
            String key = UUID.randomUUID().toString();
            sender.send(key, event.payload(), OrderEventSender.NAIVE_PARTITION);
            claims.markPublished(event, key, OutboxMode.NAIVE);
            LOG.infof("published mode=NAIVE worker=%d aggregate=%s version=%d key=%s",
                    event.workerSlot(), event.aggregateId(), event.aggregateVersion(), key);
            return null;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted publishing " + event.aggregateId(), exception);
        } finally {
            Map.Entry<Long, CountDownLatch> lower = versions.lowerEntry(event.aggregateVersion());
            if (lower != null) {
                lower.getValue().countDown();
            }
        }
    }
}

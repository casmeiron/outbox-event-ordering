package io.orderedoutbox.outbox;

import io.orderedoutbox.messaging.OrderEventSender;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * One worker owns an aggregate for as long as the worker count stays put,
 * because ownership is {@code shard % workers}. A tick leases only the oldest
 * unpublished version of each owned aggregate, sends it with the aggregate id
 * as the Kafka key, marks it, and only then becomes willing to lease the next
 * version. Parallelism is across aggregates, never inside one.
 */
@ApplicationScoped
public class SafePublisher {

    private static final Logger LOG = Logger.getLogger(SafePublisher.class);

    @Inject
    OutboxClaims claims;

    @Inject
    OutboxSettings settings;

    @Inject
    OrderEventSender sender;

    public int publishTick(int workerId) {
        List<ClaimedOutbox> claimed = claims.claimSafe(workerId, settings.workers(), settings.safeBatchSize());
        Set<UUID> seen = new HashSet<>();
        int sent = 0;
        for (ClaimedOutbox event : claimed) {
            if (!seen.add(event.aggregateId())) {
                throw new IllegalStateException("safe relay claimed two in-flight rows for " + event.aggregateId());
            }
            String key = event.aggregateId().toString();
            sender.send(key, event.payload(), null);
            claims.markPublished(event, key, OutboxMode.SAFE);
            LOG.infof("published mode=SAFE worker=%d aggregate=%s version=%d key=%s",
                    workerId, event.aggregateId(), event.aggregateVersion(), key);
            sent++;
        }
        return sent;
    }
}

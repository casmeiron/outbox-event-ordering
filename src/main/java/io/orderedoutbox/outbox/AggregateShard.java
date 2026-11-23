package io.orderedoutbox.outbox;

import java.util.UUID;

/**
 * Stable shard for an aggregate. {@link UUID#hashCode()} is specified, so the
 * same id always lands on the same shard in every JVM. Workers own
 * {@code shard % workerCount}.
 */
public final class AggregateShard {

    private AggregateShard() {
    }

    public static int of(UUID aggregateId, int shards) {
        if (aggregateId == null) {
            throw new IllegalArgumentException("aggregate id is required");
        }
        if (shards <= 0) {
            throw new IllegalArgumentException("shard count must be positive");
        }
        return Math.floorMod(aggregateId.hashCode(), shards);
    }
}

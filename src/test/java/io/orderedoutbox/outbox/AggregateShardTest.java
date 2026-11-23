package io.orderedoutbox.outbox;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateShardTest {

    @Test
    void shardIsStableAndInsideTheRing() {
        UUID id = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        int shard = AggregateShard.of(id, 32);
        assertEquals(shard, AggregateShard.of(id, 32));
        assertTrue(shard >= 0 && shard < 32);
    }

    @Test
    void differentAggregatesCanLandOnDifferentShards() {
        int shards = 32;
        boolean differed = false;
        int first = AggregateShard.of(UUID.randomUUID(), shards);
        for (int i = 0; i < 64; i++) {
            if (AggregateShard.of(UUID.randomUUID(), shards) != first) {
                differed = true;
                break;
            }
        }
        assertTrue(differed);
    }

    @Test
    void rejectsAnEmptyRing() {
        assertThrows(IllegalArgumentException.class, () -> AggregateShard.of(UUID.randomUUID(), 0));
    }
}

package io.orderedoutbox.outbox;

import java.util.UUID;

/** A row this process has leased. The token is what allows the lease owner to mark it published. */
public record ClaimedOutbox(
        long id,
        UUID eventId,
        UUID aggregateId,
        long aggregateVersion,
        String eventType,
        String payload,
        String claimToken,
        int workerSlot) {
}

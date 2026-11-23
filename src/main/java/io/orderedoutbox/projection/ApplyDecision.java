package io.orderedoutbox.projection;

/**
 * What a consumer should do with one event, given the version it has already applied.
 */
public sealed interface ApplyDecision permits ApplyDecision.Accept, ApplyDecision.Ignore, ApplyDecision.Park {

    /** {@code incoming == applied + 1}. Apply it, then drain any parked suffix. */
    record Accept() implements ApplyDecision {
    }

    /** Duplicate or already superseded. Delivery is at-least-once; this is the idempotent path. */
    record Ignore(String reason) implements ApplyDecision {
    }

    /** {@code incoming > applied + 1}. Remember it and keep consuming other aggregates. */
    record Park(long expectedVersion) implements ApplyDecision {
    }
}

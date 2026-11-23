package io.orderedoutbox.outbox;

/**
 * How long a naive consumer waits before applying an event. Lower versions wait
 * longer, so a pool that accepts events in business order still finishes them
 * backwards. That is the async-processing half of the ordering bug.
 */
public final class VersionDelay {

    private VersionDelay() {
    }

    public static long steps(long aggregateVersion, int pipelineDepth) {
        if (aggregateVersion <= 0) {
            throw new IllegalArgumentException("aggregate version must be positive");
        }
        if (pipelineDepth <= 0) {
            throw new IllegalArgumentException("pipeline depth must be positive");
        }
        return Math.max(0, (long) pipelineDepth - aggregateVersion + 1);
    }
}

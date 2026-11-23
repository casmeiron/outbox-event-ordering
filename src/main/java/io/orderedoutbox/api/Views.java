package io.orderedoutbox.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class Views {

    private Views() {
    }

    public record OrderView(
            UUID id,
            String customerRef,
            String status,
            long version,
            String projectionStatus,
            long appliedVersion,
            boolean regressed,
            String paymentRef,
            String trackingNumber,
            List<WrittenEvent> written,
            List<Long> relayVersions,
            List<String> relayKeys,
            List<AppliedEvent> applied,
            List<Long> parkedVersions,
            long pendingOutbox) {
    }

    public record WrittenEvent(long version, String type, Instant sourceCommitTime, boolean published) {
    }

    public record AppliedEvent(long version, String type, String statusAfter) {
    }

    public record ProjectionView(
            String status,
            long appliedVersion,
            boolean regressed,
            List<Long> appliedVersions,
            List<Long> parkedVersions) {
    }

    public record Summary(String mode, boolean paused, long orders, long pendingOutbox, long regressedProjections) {
    }

    public record SettingsView(
            String mode,
            boolean paused,
            int workers,
            int shards,
            boolean stallLowerVersions,
            boolean naiveAsyncConsumer) {
    }

    public record BurstRequest(int orders, String customerPrefix) {
    }

    public record BurstResponse(List<UUID> orderIds) {
    }

    public record ModeRequest(String mode) {
    }
}

package io.orderedoutbox.outbox;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Startup configuration plus the two knobs the demo flips at runtime: mode and pause.
 */
@ApplicationScoped
public class OutboxSettings {

    private final OutboxConfig config;
    private final AtomicReference<OutboxMode> mode;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    @Inject
    public OutboxSettings(OutboxConfig config) {
        if (config.workers() <= 0 || config.shards() <= 0) {
            throw new IllegalArgumentException("outbox.workers and outbox.shards must be positive");
        }
        if (config.pipelineDepth() <= 0) {
            throw new IllegalArgumentException("outbox.pipeline-depth must be positive");
        }
        if (config.naive().tickSize() <= 0 || config.safe().batchSize() <= 0) {
            throw new IllegalArgumentException("outbox batch sizes must be positive");
        }
        this.config = config;
        this.mode = new AtomicReference<>(config.mode());
    }

    public OutboxMode mode() {
        return mode.get();
    }

    public void mode(OutboxMode mode) {
        this.mode.set(mode);
    }

    public boolean paused() {
        return paused.get();
    }

    public void pause() {
        paused.set(true);
    }

    public void resume() {
        paused.set(false);
    }

    public int shards() {
        return config.shards();
    }

    public int workers() {
        return config.workers();
    }

    public Duration pollInterval() {
        return config.pollInterval();
    }

    public Duration claimLease() {
        return config.claimLease();
    }

    public int pipelineDepth() {
        return config.pipelineDepth();
    }

    public int naiveTickSize() {
        return config.naive().tickSize();
    }

    public boolean stallLowerVersions() {
        return config.naive().stallLowerVersions();
    }

    public boolean naiveAsyncConsumer() {
        return config.naive().asyncConsumer();
    }

    public Duration naiveConsumerStepDelay() {
        return config.naive().consumerStepDelay();
    }

    public int safeBatchSize() {
        return config.safe().batchSize();
    }
}

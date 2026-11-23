package io.orderedoutbox.outbox;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;

import java.time.Duration;

@ConfigMapping(prefix = "outbox")
public interface OutboxConfig {

    @WithConverter(OutboxModeConverter.class)
    OutboxMode mode();

    int shards();

    int workers();

    Duration pollInterval();

    Duration claimLease();

    int pipelineDepth();

    Naive naive();

    Safe safe();

    Publisher publisher();

    interface Naive {
        int tickSize();

        boolean stallLowerVersions();

        boolean asyncConsumer();

        Duration consumerStepDelay();
    }

    interface Safe {
        int batchSize();
    }

    interface Publisher {
        boolean autoStart();
    }
}

package io.orderedoutbox.messaging;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class OrderEventSender {

    /** Naive publishes share one partition so broker order matches send order. */
    public static final int NAIVE_PARTITION = 0;

    @Inject
    @Channel("order-events-out")
    MutinyEmitter<String> emitter;

    public void send(String key, String payload, Integer partition) {
        OutgoingKafkaRecordMetadata.OutgoingKafkaRecordMetadataBuilder<String> metadata =
                OutgoingKafkaRecordMetadata.<String>builder().withKey(key);
        if (partition != null) {
            metadata = metadata.withPartition(partition);
        }
        emitter.sendMessageAndAwait(Message.of(payload).addMetadata(metadata.build()));
    }
}

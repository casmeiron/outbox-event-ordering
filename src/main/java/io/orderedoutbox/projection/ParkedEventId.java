package io.orderedoutbox.projection;

import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class ParkedEventId implements Serializable {

    public UUID orderId;

    public long aggregateVersion;

    public ParkedEventId() {
    }

    public ParkedEventId(UUID orderId, long aggregateVersion) {
        this.orderId = orderId;
        this.aggregateVersion = aggregateVersion;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ParkedEventId that)) {
            return false;
        }
        return aggregateVersion == that.aggregateVersion && Objects.equals(orderId, that.orderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, aggregateVersion);
    }
}

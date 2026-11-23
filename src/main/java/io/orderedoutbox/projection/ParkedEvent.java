package io.orderedoutbox.projection;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "parked_event")
public class ParkedEvent extends PanacheEntityBase {

    @EmbeddedId
    public ParkedEventId id;

    @Column(nullable = false)
    public String eventType;

    @Column(nullable = false, columnDefinition = "text")
    public String payload;

    @Column(nullable = false)
    public Instant parkedAt;
}

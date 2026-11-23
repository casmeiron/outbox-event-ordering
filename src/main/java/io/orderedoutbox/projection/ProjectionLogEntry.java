package io.orderedoutbox.projection;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "projection_log")
public class ProjectionLogEntry extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(nullable = false)
    public UUID orderId;

    @Column(nullable = false)
    public long aggregateVersion;

    @Column(nullable = false)
    public String eventType;

    @Column(nullable = false)
    public String statusAfter;

    @Column(nullable = false)
    public Instant appliedAt;
}

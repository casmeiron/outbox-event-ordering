package io.orderedoutbox.outbox;

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
@Table(name = "outbox_event")
public class OutboxEvent extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(nullable = false)
    public UUID eventId;

    @Column(nullable = false)
    public UUID aggregateId;

    @Column(nullable = false)
    public long aggregateVersion;

    @Column(nullable = false)
    public String eventType;

    @Column(nullable = false, columnDefinition = "text")
    public String payload;

    @Column(nullable = false)
    public int shard;

    @Column(nullable = false)
    public Instant recordedAt;

    @Column(nullable = false)
    public Instant sourceCommitTime;

    public Instant publishedAt;

    public String claimedBy;

    public Instant claimedAt;
}

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
@Table(name = "relay_log")
public class RelayLogEntry extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(nullable = false)
    public UUID aggregateId;

    @Column(nullable = false)
    public long aggregateVersion;

    @Column(nullable = false)
    public String kafkaKey;

    @Column(nullable = false)
    public String mode;

    @Column(nullable = false)
    public int workerSlot;

    @Column(nullable = false)
    public Instant publishedAt;
}

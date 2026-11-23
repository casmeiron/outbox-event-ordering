package io.orderedoutbox.projection;

import io.orderedoutbox.domain.OrderStatus;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "order_projection")
public class OrderProjection extends PanacheEntityBase {

    @Id
    public UUID orderId;

    public String customerRef;

    @Enumerated(EnumType.STRING)
    public OrderStatus status;

    public long appliedVersion;

    public String paymentRef;

    public String trackingNumber;

    public boolean regressed;

    public Instant updatedAt;
}

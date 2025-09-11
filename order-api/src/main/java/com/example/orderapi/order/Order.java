package com.example.orderapi.order;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "orders")
public class Order {
    @Id
    @GeneratedValue
    private UUID id;

    private String customerId;
    private String courierId;
    private String city;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    @Column(nullable = false)
    private BigDecimal price;

    private Instant createdAt;
    private Instant updatedAt;
    private Instant promisedAt;

    @Transient
    private String eventType;
}

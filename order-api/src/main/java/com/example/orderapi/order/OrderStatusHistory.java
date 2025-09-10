package com.example.orderapi.order;


import jakarta.persistence.*;
import lombok.*;


import java.time.Instant;
import java.util.UUID;


@Entity
@Table(name = "order_status_history")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OrderStatusHistory {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;


    @Column(name = "order_id", nullable = false)
    private UUID orderId;


    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;


    @Column(name = "event_time", nullable = false)
    private Instant eventTime;
}
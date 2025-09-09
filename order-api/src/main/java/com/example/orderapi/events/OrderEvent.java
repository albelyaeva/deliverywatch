package com.example.orderapi.events;


import com.example.orderapi.order.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;


public record OrderEvent(
        String schema,
        String eventType,
        UUID orderId,
        OrderStatus status,
        UUID customerId,
        UUID courierId,
        String city,
        Instant createdAt,
        Instant promisedAt,
        Instant eventTime,
        BigDecimal price
) {}
package com.example.orderapi.order.dto;

import com.example.orderapi.order.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String customerId,
        String courierId,
        String city,
        OrderStatus status,
        BigDecimal price,
        Instant createdAt,
        Instant promisedAt,
        Instant updatedAt
) {}


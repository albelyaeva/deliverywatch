package com.example.orderapi.events;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderEvent(
        String schema,
        String eventType,
        String orderId,
        String status,
        String customerId,
        String courierId,
        String city,
        Instant createdAt,
        Instant promisedAt,
        Instant eventTime,
        BigDecimal price
) {}

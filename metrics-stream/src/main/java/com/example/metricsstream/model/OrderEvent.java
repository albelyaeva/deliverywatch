package com.example.metricsstream.model;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;


@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderEvent(
        String schema,
        String eventType,
        UUID orderId,
        String status,
        UUID customerId,
        UUID courierId,
        String city,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant createdAt,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant promisedAt,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant eventTime,
        BigDecimal price
) {}
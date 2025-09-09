package com.example.orderapi.order.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreateRequest(
        @NotNull UUID customerId,
        @NotBlank String city,
        @NotNull BigDecimal price,
        @NotNull Instant promisedAt
) {}

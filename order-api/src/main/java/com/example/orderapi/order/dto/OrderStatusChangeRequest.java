package com.example.orderapi.order.dto;

import com.example.orderapi.order.OrderStatus;
import jakarta.validation.constraints.NotNull;

public record OrderStatusChangeRequest(
        @NotNull OrderStatus status
) {}

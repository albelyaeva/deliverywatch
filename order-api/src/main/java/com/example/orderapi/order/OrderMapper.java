package com.example.orderapi.order;


import com.example.orderapi.order.dto.OrderResponse;
import org.mapstruct.Mapper;
import org.springframework.stereotype.Component;


@Component
public class OrderMapper {
    public OrderResponse toResponse(Order o) {
        return new OrderResponse(
                o.getId(),
                o.getCustomerId(),
                o.getCourierId(),
                o.getCity(),
                o.getStatus(),
                o.getPrice(),
                o.getCreatedAt(),
                o.getPromisedAt(),
                o.getUpdatedAt()
        );
    }
}
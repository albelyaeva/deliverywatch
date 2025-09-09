package com.example.orderapi.order;


import com.example.orderapi.order.dto.OrderResponse;
import org.mapstruct.Mapper;


@Mapper(componentModel = "spring")
public interface OrderMapper {
    OrderResponse toResponse(Order o);
}
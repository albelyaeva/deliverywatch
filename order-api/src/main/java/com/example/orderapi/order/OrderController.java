package com.example.orderapi.order;


import com.example.orderapi.order.dto.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;


import java.net.URI;
import java.util.UUID;


@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService service;
    private final OrderMapper mapper;


    @PostMapping
    public ResponseEntity<OrderResponse> create(@RequestBody @Validated OrderCreateRequest req) {
        var saved = service.create(req);
        return ResponseEntity.created(URI.create("/orders/"+saved.getId()))
                .body(mapper.toResponse(saved));
    }


    @PostMapping("/{id}/status")
    public OrderResponse changeStatus(@PathVariable UUID id, @RequestBody @Validated OrderStatusChangeRequest req) {
        return mapper.toResponse(service.changeStatus(id, req));
    }
}
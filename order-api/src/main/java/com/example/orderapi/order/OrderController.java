package com.example.orderapi.order;

import com.example.orderapi.kafka.OrderEvents;
import com.example.orderapi.order.dto.OrderCreateRequest;
import com.example.orderapi.order.dto.OrderResponse;
import com.example.orderapi.order.dto.OrderStatusChangeRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService service;
    private final OrderMapper mapper;
    private final OrderEvents events;

    @PostMapping
    public ResponseEntity<OrderResponse> create(@RequestBody @Validated OrderCreateRequest req) {
        log.info("HTTP POST /orders req={}", req);
        var saved = service.create(req);

        var resp = mapper.toResponse(saved);
        events.publish(resp, saved.getId().toString());

        log.info("ORDER CREATED id={} status={} city={}", saved.getId(), resp.status(), resp.city());
        return ResponseEntity
                .created(URI.create("/orders/" + saved.getId()))
                .body(resp);
    }

    @PostMapping("/{id}/status")
    public OrderResponse changeStatus(@PathVariable("id") UUID id,
                                      @RequestBody @Validated OrderStatusChangeRequest req) {
        log.info("HTTP POST /orders/{}/status newStatus={}", id, req.status());
        var updated = service.changeStatus(id, req);

        var resp = mapper.toResponse(updated);
        events.publish(resp, updated.getId().toString());

        log.info("ORDER STATUS CHANGED id={} -> {}", id, resp.status());
        return resp;
    }
}

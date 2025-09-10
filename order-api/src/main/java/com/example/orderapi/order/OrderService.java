package com.example.orderapi.order;

import com.example.orderapi.events.OrderEvent;
import com.example.orderapi.kafka.OrderEvents;
import com.example.orderapi.order.dto.OrderCreateRequest;
import com.example.orderapi.order.dto.OrderStatusChangeRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository repo;
    private final OrderStatusHistoryRepository histRepo;
    private final OrderEvents events;

    @Transactional
    public Order create(OrderCreateRequest req) {
        var now = Instant.now();
        var o = Order.builder()
                .customerId(String.valueOf(req.customerId()))
                .city(req.city())
                .price(req.price())
                .status(OrderStatus.PLACED)
                .createdAt(now)
                .updatedAt(now)
                .promisedAt(req.promisedAt())
                .build();
        o = repo.save(o);
        histRepo.save(OrderStatusHistory.builder()
                .orderId(o.getId())
                .status(OrderStatus.PLACED)
                .eventTime(now)
                .build());

        events.publish(new OrderEvent(
                "delivery.order.events.v1",
                "ORDER_CREATED",
                o.getId().toString(),
                o.getStatus().name(),
                o.getCustomerId(),
                o.getCourierId(),
                o.getCity(),
                o.getCreatedAt(),
                o.getPromisedAt(),
                now,
                o.getPrice()
        ), o.getId().toString());

        return o;
    }

    @Transactional
    public Order changeStatus(UUID orderId, OrderStatusChangeRequest req) {
        var o = repo.findById(orderId).orElseThrow();
        o.setStatus(req.status());
        o.setUpdatedAt(Instant.now());

        var saved = repo.save(o);

        histRepo.save(OrderStatusHistory.builder()
                .orderId(saved.getId())
                .status(req.status())
                .eventTime(saved.getUpdatedAt())
                .build());

        events.publish(new OrderEvent(
                "delivery.order.events.v1",
                "STATUS_CHANGED",
                o.getId().toString(),
                o.getStatus().name(),
                o.getCustomerId(),
                o.getCourierId(),
                o.getCity(),
                o.getCreatedAt(),
                o.getPromisedAt(),
                saved.getUpdatedAt(),
                o.getPrice()
        ), o.getId().toString());

        return saved;
    }
}

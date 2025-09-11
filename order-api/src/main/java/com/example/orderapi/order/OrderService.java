package com.example.orderapi.order;

import com.example.orderapi.order.dto.OrderCreateRequest;
import com.example.orderapi.order.dto.OrderStatusChangeRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository repo;
    private final OrderStatusHistoryRepository histRepo;
    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    public Order create(OrderCreateRequest req) {
        var now = Instant.now();
        var order = Order.builder()
                .customerId(String.valueOf(req.customerId()))
                .city(req.city())
                .price(req.price())
                .status(OrderStatus.PLACED)
                .createdAt(now)
                .updatedAt(now)
                .promisedAt(req.promisedAt())
                .build();

        order = repo.save(order);

        histRepo.save(OrderStatusHistory.builder()
                .orderId(order.getId())
                .status(OrderStatus.PLACED)
                .eventTime(now)
                .build());

        order.setEventType("ORDER_CREATED");
        eventPublisher.publishEvent(order);

        log.info("Order created: id={}, city={}, status={}", order.getId(), order.getCity(), order.getStatus());
        return order;
    }

    @Transactional
    public Order changeStatus(UUID orderId, OrderStatusChangeRequest req) {
        var order = repo.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

        order.setStatus(req.status());
        order.setUpdatedAt(Instant.now());

        var saved = repo.save(order);

        histRepo.save(OrderStatusHistory.builder()
                .orderId(saved.getId())
                .status(req.status())
                .eventTime(saved.getUpdatedAt())
                .build());

        // Добавляем метаданные для типа события
        saved.setEventType("STATUS_CHANGED");
        eventPublisher.publishEvent(saved);

        log.info("Order status changed: id={}, status={}", orderId, req.status());
        return saved;
    }
}
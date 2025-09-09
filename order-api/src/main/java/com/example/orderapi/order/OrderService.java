package com.example.orderapi.order;


import com.example.orderapi.events.OrderEvent;
import com.example.orderapi.order.dto.OrderCreateRequest;
import com.example.orderapi.order.dto.OrderStatusChangeRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


import java.time.Instant;


@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository repo;
    private final OrderStatusHistoryRepository histRepo;
    private final ApplicationEventPublisher events;


    @Transactional
    public Order create(OrderCreateRequest req) {
        var now = Instant.now();
        var o = Order.builder()
                .customerId(req.customerId())
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
                .eventTime(now).build());
        events.publishEvent(new OrderEvent(
                "delivery.order.events.v1",
                "ORDER_CREATED",
                o.getId(), o.getStatus(), o.getCustomerId(), o.getCourierId(), o.getCity(),
                o.getCreatedAt(), o.getPromisedAt(), now, o.getPrice()
        ));
        return o;
    }


    @Transactional
    public Order changeStatus(java.util.UUID orderId, OrderStatusChangeRequest req) {
        var o = repo.findById(orderId).orElseThrow();
        o.setStatus(req.status());
        o.setUpdatedAt(Instant.now());
        var saved = repo.save(o);
        histRepo.save(OrderStatusHistory.builder()
                .orderId(saved.getId())
                .status(req.status())
                .eventTime(saved.getUpdatedAt()).build());
        events.publishEvent(new OrderEvent(
                "delivery.order.events.v1",
                "STATUS_CHANGED",
                saved.getId(), saved.getStatus(), saved.getCustomerId(), saved.getCourierId(), saved.getCity(),
                saved.getCreatedAt(), saved.getPromisedAt(), saved.getUpdatedAt(), saved.getPrice()
        ));
        return saved;
    }
}
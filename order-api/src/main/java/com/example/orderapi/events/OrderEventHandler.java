package com.example.orderapi.events;

import com.example.orderapi.kafka.OrderEvents;
import com.example.orderapi.order.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventHandler {

    private final OrderEvents orderEvents;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Async
    public void handleOrder(Order order) {
        var orderEvent = new OrderEvent(
                "delivery.order.events.v1",
                order.getEventType(),
                order.getId().toString(),
                order.getStatus().name(),
                order.getCustomerId(),
                order.getCourierId(),
                order.getCity(),
                order.getCreatedAt(),
                order.getPromisedAt(),
                order.getUpdatedAt(),
                order.getPrice()
        );

        orderEvents.publishWithRetry(orderEvent, order.getId().toString());
        log.info("Published {} event for order: {}", order.getEventType(), order.getId());
    }
}

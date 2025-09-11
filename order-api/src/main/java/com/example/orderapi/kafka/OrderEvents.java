package com.example.orderapi.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEvents {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    @Value("${app.kafka.topic:orders}")
    private String topic;

    @Retryable(maxAttempts = 3, backoff = @Backoff(delay = 1000))
    public void publishWithRetry(Object event, String key) {
        try {
            String payload = mapper.writeValueAsString(event);

            var future = kafka.send(topic, key, payload);
            var result = future.get(); // Блокируем для retry логики

            var metadata = result.getRecordMetadata();
            log.info("PUBLISHED topic={} key={} partition={} offset={}",
                    topic, key, metadata.partition(), metadata.offset());

        } catch (Exception e) {
            log.error("Failed to publish event: key={}", key, e);
            throw new RuntimeException("Event publishing failed", e);
        }
    }

    public void publish(Object event, String key) {
        publishWithRetry(event, key);
    }
}
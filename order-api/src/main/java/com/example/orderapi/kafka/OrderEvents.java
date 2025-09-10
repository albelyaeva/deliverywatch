package com.example.orderapi.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEvents {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    @Value("${app.kafka.topic:orders}")
    private String topic;

    public void publish(Object event, String key) {
        try {
            String payload = mapper.writeValueAsString(event);

            CompletableFuture<SendResult<String, String>> future = kafka.send(topic, key, payload);

            future.whenComplete((sr, ex) -> {
                if (ex != null) {
                    log.error("KAFKA PUBLISH FAILED topic={} key={} err={}", topic, key, ex.toString());
                } else {
                    RecordMetadata rm = sr.getRecordMetadata();
                    log.info("PUBLISHED topic={} key={} partition={} offset={}",
                            topic, key, rm.partition(), rm.offset());
                }
            });

        } catch (Exception e) {
            log.error("Kafka serialize failed", e);
        }
    }
}

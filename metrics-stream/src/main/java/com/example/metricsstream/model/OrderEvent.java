package com.example.metricsstream.model;


import com.example.metricsstream.streams.Stats;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;


@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderEvent(
        String schema,
        String eventType,
        UUID orderId,
        String status,
        UUID customerId,
        UUID courierId,
        String city,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant createdAt,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant promisedAt,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant eventTime,
        BigDecimal price
) {
    private Serde<Stats> createStatsSerde() {
        JsonSerde<Stats> serde = new JsonSerde<>(Stats.class);
        Map<String, Object> config = Map.of(
                JsonDeserializer.TRUSTED_PACKAGES, "*"
        );
        serde.configure(config, false);
        return serde;
    }
}
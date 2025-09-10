package com.example.metricsstream.streams;

import com.example.metricsstream.repository.MetricsWriter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class StreamsTopology {

    private final MetricsWriter writer;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    @Value("${app.kafka.input-topic}")
    private String inputTopic;

    @Value("${app.windows.size-minutes:1}")
    private int windowSizeMinutes;

    @Value("${app.alerts.sla-threshold:90.0}")
    private double slaThreshold;

    public record OrderEvt(
            String schema,
            String eventType,
            String orderId,
            String status,
            String customerId,
            String courierId,
            String city,
            Instant createdAt,
            Instant promisedAt,
            Instant eventTime,
            Double price
    ) {}

    private record CitySec(String city, Long seconds) {}

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Bean
    public KafkaStreams kafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();

        KStream<String, String> raw = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));

        KStream<String, OrderEvt> events = raw
                .peek((k, v) -> log.info("STREAM IN raw key={} value={}", k, v))
                .mapValues(v -> {
                    try {
                        return MAPPER.readValue(v, OrderEvt.class);
                    } catch (Exception e) {
                        log.warn("Bad event JSON, skipping: {}", v, e);
                        return null;
                    }
                })
                .filter((k, e) -> e != null)
                // ставим ключом orderId (если он есть)
                .selectKey((k, e) -> e.orderId() != null ? e.orderId() : k)
                .peek((k, e) -> log.info("STREAM PARSED key={} type={} status={} city={}", k, e.eventType(), e.status(), e.city()));

        TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowSizeMinutes));

        // --- SLA (DELIVERED vs on-time) ---
        KStream<String, OrderEvt> statusChanged = events
                .filter((k, e) -> "STATUS_CHANGED".equals(e.eventType()));

        KTable<Windowed<String>, Long> deliveredTotal = statusChanged
                .filter((k, e) -> "DELIVERED".equals(e.status()))
                .map((k, e) -> KeyValue.pair(e.city(), 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(windows)
                .count();

        KTable<Windowed<String>, Long> deliveredOnTime = statusChanged
                .filter((k, e) -> "DELIVERED".equals(e.status())
                        && e.promisedAt() != null
                        && e.eventTime() != null
                        && !e.promisedAt().isBefore(e.eventTime()))
                .map((k, e) -> KeyValue.pair(e.city(), 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(windows)
                .count();

        KTable<Windowed<String>, Double> slaPercent = deliveredOnTime.join(
                deliveredTotal,
                (onTime, total) -> (total == null || total == 0) ? 100.0 : (onTime * 100.0) / total
        );

        slaPercent.toStream().foreach((winCity, sla) -> {
            String city = winCity.key();
            var start = winCity.window().startTime();
            var end = winCity.window().endTime();
            writer.upsertSla(start, end, city, sla);
            if (sla != null && sla < slaThreshold) {
                writer.insertAlert(end, "LOW_SLA", city, sla, "SLA below threshold " + slaThreshold + "%");
            }
            log.info("SLA UPSERT city={} window=[{},{}] sla={}", city, start, end, sla);
        });

        // --- delivery time (avg, p95) ---
        // таблица createdAt по orderId
        KTable<String, Long> createdAtTable = events
                .filter((k, e) -> "ORDER_CREATED".equals(e.eventType()) && e.createdAt() != null)
                .mapValues(e -> e.createdAt().toEpochMilli())
                .toTable(Materialized.with(stringSerde, Serdes.Long()));

        // на DELIVERED считаем секунды и мапим на ключ city
        KStream<String, Long> durationsByCity = events
                .filter((k, e) -> "STATUS_CHANGED".equals(e.eventType())
                        && "DELIVERED".equals(e.status())
                        && e.eventTime() != null)
                .leftJoin(createdAtTable,
                        (delivered, createdAtMs) -> {
                            if (createdAtMs == null) return new CitySec(delivered.city(), null);
                            long seconds = Math.max(0, (delivered.eventTime().toEpochMilli() - createdAtMs) / 1000);
                            return new CitySec(delivered.city(), seconds);
                        })
                .filter((k, citySec) -> citySec != null && citySec.seconds != null)
                .map((orderId, citySec) -> KeyValue.pair(citySec.city, citySec.seconds));

        TimeWindowedKStream<String, Long> winDur =
                durationsByCity.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                        .windowedBy(windows);
        // простой агрегатор статистики
        var statsSerde = Serdes.String(); // не нужен кастомный serde — будем писать в БД из foreach
        var aggSerde = new JsonSerde<>(Agg.class);
        aggSerde.deserializer().configure(Map.of(
                JsonDeserializer.TRUSTED_PACKAGES, "*"
        ), false);

        KTable<Windowed<String>, Agg> stats = winDur.aggregate(
                Agg::new,
                (city, seconds, agg) -> agg.add(seconds),
                Materialized.with(Serdes.String(), aggSerde)   // <-- важно!
        );

        KTable<Windowed<String>, Long> sumSeconds =
                winDur.reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()));

        KTable<Windowed<String>, Long> cnt =
                winDur.count(Materialized.with(Serdes.String(), Serdes.Long()));

        stats.toStream().foreach((winCity, st) -> {
            if (st == null) return;
            String city = winCity.key();
            var start = winCity.window().startTime();
            var end = winCity.window().endTime();
            int avgSec = (int) Math.round(st.avg());
            int p95Sec = st.p95();
            writer.upsertDeliveryTime(start, end, city, avgSec, p95Sec);
            log.info("DT UPSERT city={} window=[{},{}] avg={}s p95={}s", city, start, end, avgSec, p95Sec);
        });

        // стартуем Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(streamsProps()));
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        return streams;
    }

    // простой агрегатор для p95/avg
    static class Agg {
        long count;
        long sum;
        // грубая p95 без хранения всего — можно заменить на TDigest или хистограмму
        long max; // временно; если нужен честный p95 — соберите гистограмму

        Agg add(long sec) {
            count++;
            sum += sec;
            if (sec > max) max = sec;
            return this;
        }
        double avg() { return count == 0 ? 0 : (double) sum / count; }
        int p95() { return (int) max; } // упрощённо: возьмём максимум как суррогат p95 для демо
    }

    private Map<String, Object> streamsProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deliverywatch-metrics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstreams/metrics");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        // читать с «начала», если стейт пустой
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}

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
import org.springframework.kafka.support.serializer.JsonSerializer;

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
    ) {
    }

    private record CitySec(String city, Long seconds) {
    }

    public static record Agg(long count, long sum, long max) {
        public Agg() {
            this(0, 0, 0);
        }

        public Agg add(long sec) {
            return new Agg(count + 1, sum + sec, Math.max(max, sec));
        }

        public double avg() {
            return count == 0 ? 0 : (double) sum / count;
        }

        public int p95() {
            return (int) max;
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Bean
    public KafkaStreams kafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();

        // Создаем serdes
        Serde<String> stringSerde = Serdes.String();
        Serde<OrderEvt> orderEvtSerde = createOrderEvtSerde();

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
                // Используем правильный serde для repartitioning
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
                        },
                        Joined.with(stringSerde, orderEvtSerde, Serdes.Long())) // ДОБАВЛЕНО!
                .filter((k, citySec) -> citySec != null && citySec.seconds != null)
                .map((orderId, citySec) -> KeyValue.pair(citySec.city, citySec.seconds));

        TimeWindowedKStream<String, Long> winDur =
                durationsByCity.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                        .windowedBy(windows);

        // Правильно настроенный serde для агрегации
        var aggSerde = createAggSerde();

        KTable<Windowed<String>, Agg> stats = winDur.aggregate(
                Agg::new,
                (city, seconds, agg) -> agg.add(seconds),
                Materialized.with(Serdes.String(), aggSerde)
        );

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

    private Serde<OrderEvt> createOrderEvtSerde() {
        JsonSerde<OrderEvt> serde = new JsonSerde<>(OrderEvt.class);
        Map<String, Object> config = Map.of(
                JsonDeserializer.TRUSTED_PACKAGES, "*"
        );
        serde.configure(config, false);
        return serde;
    }

    private Serde<Agg> createAggSerde() {
        JsonSerde<Agg> serde = new JsonSerde<>(Agg.class);
        Map<String, Object> config = Map.of(
                JsonDeserializer.TRUSTED_PACKAGES, "*",
                JsonSerializer.ADD_TYPE_INFO_HEADERS, false
        );
        serde.configure(config, false);
        return serde;
    }

    private Map<String, Object> streamsProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deliverywatch-metrics-v4"); // Изменил версию
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstreams/metrics-v4"); // Новый путь
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}
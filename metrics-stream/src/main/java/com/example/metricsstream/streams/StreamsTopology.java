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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

    public static record Agg(long count, long sum, long[] samples) {
        private static final int MAX_SAMPLES = 1000;
        private static final Random RANDOM = new Random();

        public Agg() {
            this(0, 0, new long[0]);
        }

        public Agg add(long sec) {
            long[] newSamples;

            if (samples.length < MAX_SAMPLES) {
                newSamples = Arrays.copyOf(samples, samples.length + 1);
                newSamples[samples.length] = sec;
            } else {
                newSamples = samples.clone();
                int replaceIndex = RANDOM.nextInt(MAX_SAMPLES);
                newSamples[replaceIndex] = sec;
            }

            return new Agg(count + 1, sum + sec, newSamples);
        }

        public double avg() {
            return count == 0 ? 0 : (double) sum / count;
        }

        public int p95() {
            if (samples.length == 0) return 0;

            long[] sorted = samples.clone();
            Arrays.sort(sorted);
            int index = (int) Math.ceil(0.95 * samples.length) - 1;
            return (int) sorted[Math.max(0, Math.min(index, samples.length - 1))];
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Bean
    public KafkaStreams kafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<OrderEvt> orderEvtSerde = createOrderEvtSerde();

        KStream<String, String> raw = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));

        KStream<String, OrderEvt> events = raw
                .peek((k, v) -> log.debug("STREAM IN raw key={} value={}", k, v)) // debug вместо info для производительности
                .mapValues(v -> {
                    try {
                        return MAPPER.readValue(v, OrderEvt.class);
                    } catch (Exception e) {
                        log.warn("Bad event JSON, skipping: {}", v, e);
                        return null;
                    }
                })
                .filter((k, e) -> e != null)
                .selectKey((k, e) -> e.orderId() != null ? e.orderId() : k)
                .peek((k, e) -> log.debug("STREAM PARSED key={} type={} status={} city={}", k, e.eventType(), e.status(), e.city()));

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

            try {
                writer.upsertSla(start, end, city, sla);
                if (sla != null && sla < slaThreshold) {
                    writer.insertAlert(end, "LOW_SLA", city, sla, "SLA below threshold " + slaThreshold + "%");
                }
                log.info("SLA UPSERT city={} window=[{},{}] sla={}", city, start, end, sla);
            } catch (Exception e) {
                log.error("Failed to write SLA metrics for city={}", city, e);
            }
        });

        // --- delivery time (avg, p95) ---
        KTable<String, Long> createdAtTable = events
                .filter((k, e) -> "ORDER_CREATED".equals(e.eventType()) && e.createdAt() != null)
                .mapValues(e -> e.createdAt().toEpochMilli())
                .toTable(Materialized.with(stringSerde, Serdes.Long()));

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
                        Joined.with(stringSerde, orderEvtSerde, Serdes.Long()))
                .filter((k, citySec) -> citySec != null && citySec.seconds != null)
                .map((orderId, citySec) -> KeyValue.pair(citySec.city, citySec.seconds));

        TimeWindowedKStream<String, Long> winDur =
                durationsByCity.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                        .windowedBy(windows);

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

            try {
                writer.upsertDeliveryTime(start, end, city, avgSec, p95Sec);
                log.info("DT UPSERT city={} window=[{},{}] avg={}s p95={}s count={}",
                        city, start, end, avgSec, p95Sec, st.count);
            } catch (Exception e) {
                log.error("Failed to write delivery time metrics for city={}", city, e);
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(streamsProps()));

        streams.setStateListener((newState, oldState) -> {
            log.info("Kafka Streams state changed: {} -> {}", oldState, newState);
        });

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Kafka Streams...");
            streams.close(Duration.ofSeconds(10));
        }));

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deliverywatch-metrics-v5");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstreams/metrics-v5");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000); // Коммитим каждые 30 сек
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024); // 10MB cache
        props.put("auto.offset.reset", "earliest");

        return props;
    }
}
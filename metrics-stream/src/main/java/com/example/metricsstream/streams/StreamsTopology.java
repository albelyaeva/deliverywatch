package com.example.metricsstream.streams;

import com.example.metricsstream.model.OrderEvent;
import com.example.metricsstream.repository.MetricsWriter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;


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

    private record CitySec(String city, Long seconds) {}

    @Bean
    public KafkaStreams kafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        JsonSerde<OrderEvent> orderSerde = new JsonSerde<>(OrderEvent.class);

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        orderSerde.deserializer().configure(serdeConfigs, false);

        KStream<String, OrderEvent> eventsRaw =
                builder.stream(inputTopic, Consumed.with(stringSerde, orderSerde));

        KStream<String, OrderEvent> events =
                eventsRaw.selectKey((k, e) -> e == null || e.orderId() == null ? k : e.orderId().toString());

        TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowSizeMinutes));

        KStream<String, OrderEvent> statusChanged = events
                .filter((k, e) -> e != null && "STATUS_CHANGED".equals(e.eventType()));

        KTable<Windowed<String>, Long> deliveredTotal = statusChanged
                .filter((k, e) -> "DELIVERED".equals(e.status()))
                .groupBy((k, e) -> e.city(), Grouped.with(stringSerde, orderSerde))
                .windowedBy(windows)
                .count();

        KTable<Windowed<String>, Long> deliveredOnTime = statusChanged
                .filter((k, e) -> "DELIVERED".equals(e.status())
                        && e.promisedAt() != null
                        && e.eventTime() != null
                        && !e.promisedAt().isBefore(e.eventTime()))
                .groupBy((k, e) -> e.city(), Grouped.with(stringSerde, orderSerde))
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
        });


        KTable<String, Long> createdAtTable = events
                .filter((k, e) -> e != null && "ORDER_CREATED".equals(e.eventType()) && e.createdAt() != null)
                .mapValues(e -> e.createdAt().toEpochMilli())
                .toTable(Materialized.with(stringSerde, Serdes.Long()));

        KStream<String, Long> durationsByCity = events
                .filter((k, e) -> e != null && "STATUS_CHANGED".equals(e.eventType())
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

        TimeWindowedKStream<String, Long> winDur = durationsByCity.groupByKey().windowedBy(windows);

        var statsSerde = new org.springframework.kafka.support.serializer.JsonSerde<>(Stats.class);
        statsSerde.deserializer().configure(java.util.Map.of(
                org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES, "*"
        ), false);

        KTable<Windowed<String>, Stats> stats = winDur.aggregate(
                Stats::new,
                (city, seconds, agg) -> { agg.add(seconds); return agg; },
                Materialized.with(Serdes.String(), statsSerde)
        );

        KTable<Windowed<String>, Long> sumSeconds = winDur.reduce(Long::sum);
        KTable<Windowed<String>, Long> cnt = winDur.count();

        stats.toStream().foreach((winCity, st) -> {
            String city = winCity.key();
            var start = winCity.window().startTime();
            var end = winCity.window().endTime();
            int avgSec = (int)Math.round(st.avg());
            int p95Sec = st.p95Sec();
            writer.upsertDeliveryTime(start, end, city, avgSec, p95Sec);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(streamsProps()));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    private Map<String, Object> streamsProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deliverywatch-metrics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstreams/metrics");
        return props;
    }
}

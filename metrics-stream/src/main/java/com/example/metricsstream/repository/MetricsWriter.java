package com.example.metricsstream.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class MetricsWriter {
    private final JdbcTemplate jdbc;

    public void upsertSla(Instant windowStart, Instant windowEnd, String city, Double slaPercent) {
        jdbc.update(
                "insert into kpi_sla(window_start, window_end, city, sla_percent) values (?,?,?,?) " +
                        "on conflict (window_start, window_end, city) do update set sla_percent = excluded.sla_percent",
                windowStart, windowEnd, city, slaPercent
        );
    }

    public void upsertDeliveryTime(Instant windowStart, Instant windowEnd, String city, int avgSeconds, int p95Seconds) {
        jdbc.update(
                "insert into kpi_delivery_time(window_start, window_end, city, p95_seconds, avg_seconds) values (?,?,?,?,?) " +
                        "on conflict (window_start, window_end, city) do update set p95_seconds = excluded.p95_seconds, avg_seconds = excluded.avg_seconds",
                windowStart, windowEnd, city, p95Seconds, avgSeconds
        );
    }

    public void insertAlert(Instant ts, String kind, String city, Double value, String details) {
        jdbc.update(
                "insert into alert_anomalies(ts, kind, city, value, details) values (?,?,?,?,?)",
                ts, kind, city, value, details
        );
    }
}

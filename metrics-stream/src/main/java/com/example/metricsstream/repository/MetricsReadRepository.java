package com.example.metricsstream.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class MetricsReadRepository {
    private final JdbcTemplate jdbc;

    public record SlaRow(Instant windowStart, Instant windowEnd, String city, double slaPercent) {}
    public record TimeRow(Instant windowStart, Instant windowEnd, String city, int p95Seconds, int avgSeconds) {}
    public record AlertRow(long id, Instant ts, String kind, String city, Double value, String details) {}

    public List<SlaRow> findSla(String city, int limit) {
        String sql = """
            select window_start as windowStart, window_end as windowEnd, city, sla_percent as slaPercent
            from kpi_sla
            where (:city is null or city = :city)
            order by window_end desc
            limit :limit
        """;

        if (city == null) {
            return jdbc.query("""
                select window_start as windowStart, window_end as windowEnd, city, sla_percent as slaPercent
                from kpi_sla
                order by window_end desc
                limit ?
            """, new BeanPropertyRowMapper<>(SlaRow.class), limit);
        }
        return jdbc.query("""
            select window_start as windowStart, window_end as windowEnd, city, sla_percent as slaPercent
            from kpi_sla
            where city = ?
            order by window_end desc
            limit ?
        """, new BeanPropertyRowMapper<>(SlaRow.class), city, limit);
    }

    public List<TimeRow> findDeliveryTime(String city, int limit) {
        if (city == null) {
            return jdbc.query("""
                select window_start as windowStart, window_end as windowEnd, city, p95_seconds as p95Seconds, avg_seconds as avgSeconds
                from kpi_delivery_time
                order by window_end desc
                limit ?
            """, new BeanPropertyRowMapper<>(TimeRow.class), limit);
        }
        return jdbc.query("""
            select window_start as windowStart, window_end as windowEnd, city, p95_seconds as p95Seconds, avg_seconds as avgSeconds
            from kpi_delivery_time
            where city = ?
            order by window_end desc
            limit ?
        """, new BeanPropertyRowMapper<>(TimeRow.class), city, limit);
    }

    public List<AlertRow> findRecentAlerts(int limit) {
        return jdbc.query("""
            select id, ts, kind, city, value, details
            from alert_anomalies
            order by ts desc
            limit ?
        """, new BeanPropertyRowMapper<>(AlertRow.class), limit);
    }
}

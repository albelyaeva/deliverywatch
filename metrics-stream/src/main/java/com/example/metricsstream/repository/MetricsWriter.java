package com.example.metricsstream.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class MetricsWriter {

    private final JdbcTemplate jdbc;

    public void upsertSla(Instant windowStart, Instant windowEnd, String city, Double slaPercent) {
        jdbc.update(con -> {
            var ps = con.prepareStatement(
                    "insert into kpi_sla(window_start, window_end, city, sla_percent) " +
                            "values (?,?,?,?) " +
                            "on conflict (window_start, window_end, city) do update set sla_percent = excluded.sla_percent"
            );
            ps.setTimestamp(1, Timestamp.from(windowStart));
            ps.setTimestamp(2, Timestamp.from(windowEnd));
            ps.setString(3, city);
            if (slaPercent == null) {
                ps.setNull(4, java.sql.Types.DOUBLE);
            } else {
                ps.setDouble(4, slaPercent);
            }
            return ps;
        });
    }

    public void upsertDeliveryTime(Instant windowStart, Instant windowEnd, String city, int avgSec, int p95Sec) {
        jdbc.update(con -> {
            var ps = con.prepareStatement(
                    "insert into kpi_delivery_time(window_start, window_end, city, avg_seconds, p95_seconds) " +
                            "values (?,?,?,?,?) " +
                            "on conflict (window_start, window_end, city) do update set " +
                            "avg_seconds = excluded.avg_seconds, p95_seconds = excluded.p95_seconds"
            );
            ps.setTimestamp(1, Timestamp.from(windowStart));
            ps.setTimestamp(2, Timestamp.from(windowEnd));
            ps.setString(3, city);
            ps.setInt(4, avgSec);
            ps.setInt(5, p95Sec);
            return ps;
        });
    }

    public void insertAlert(Instant ts, String code, String city, Double value, String details) {
        jdbc.update(con -> {
            var ps = con.prepareStatement(
                    "insert into alerts(ts, code, city, value, details) values (?,?,?,?,?)"
            );
            ps.setTimestamp(1, Timestamp.from(ts));
            ps.setString(2, code);
            ps.setString(3, city);
            if (value == null) {
                ps.setNull(4, java.sql.Types.DOUBLE);
            } else {
                ps.setDouble(4, value);
            }
            ps.setString(5, details);
            return ps;
        });
    }
}

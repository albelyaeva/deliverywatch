package com.example.metricsstream.streams;

import lombok.Getter;

@Getter
public class Stats {
    private long count;
    private long sumSeconds;
    private final P2QuantileEstimator p95 = new P2QuantileEstimator(0.95);

    public void add(long seconds) {
        count++;
        sumSeconds += seconds;
        p95.add(seconds);
    }

    public double avg() {
        return count == 0 ? 0.0 : (sumSeconds * 1.0) / count;
    }

    public int p95Sec() {
        double q = p95.getQuantile();
        return Double.isNaN(q) ? 0 : (int)Math.round(q);
    }
}

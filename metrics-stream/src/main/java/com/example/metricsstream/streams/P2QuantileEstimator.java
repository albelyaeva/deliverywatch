package com.example.metricsstream.streams;


public class P2QuantileEstimator {
    private final double p;
    private final double[] q = new double[5];
    private final double[] n = new double[5];
    private final double[] np = new double[5];
    private final double[] dn = new double[5];
    private int count = 0;
    private double min = Double.NaN, max = Double.NaN;

    public P2QuantileEstimator(double p) {
        if (p <= 0 || p >= 1) throw new IllegalArgumentException("p must be in (0,1)");
        this.p = p;
    }

    public void add(double x) {
        if (count < 5) {
            if (count == 0) { min = max = x; }
            else { min = Math.min(min, x); max = Math.max(max, x); }
            q[count] = x;
            count++;
            if (count == 5) initAfterFirstFive();
            return;
        }
        if (x < min) min = x;
        if (x > max) max = x;

        int k;
        if (x < q[0]) { q[0] = x; k = 0; }
        else if (x >= q[4]) { q[4] = x; k = 3; }
        else {
            k = 0;
            while (k < 4 && x >= q[k+1]) k++;
        }

        for (int i = k + 1; i < 5; i++) n[i] += 1.0;
        for (int i = 0; i < 5; i++) np[i] += dn[i];

        for (int i = 1; i <= 3; i++) adjust(i);
    }

    public double getQuantile() {
        if (count == 0) return Double.NaN;
        if (count < 5) {
            double[] tmp = new double[count];
            System.arraycopy(q, 0, tmp, 0, count);
            java.util.Arrays.sort(tmp);
            int idx = (int)Math.round(p * (count - 1));
            return tmp[Math.min(Math.max(idx, 0), count-1)];
        }
        return q[2];
    }

    private void initAfterFirstFive() {
        java.util.Arrays.sort(q);
        n[0] = 1; n[1] = 2; n[2] = 3; n[3] = 4; n[4] = 5;
        np[0] = 1;
        np[1] = 1 + 2*p;
        np[2] = 1 + 4*p;
        np[3] = 3 + 2*p;
        np[4] = 5;
        dn[0] = 0;
        dn[1] = p/2.0;
        dn[2] = p;
        dn[3] = (1+p)/2.0;
        dn[4] = 1;
    }

    private void adjust(int i) {
        double d = np[i] - n[i];
        if ((d >= 1 && n[i+1] - n[i] > 1) || (d <= -1 && n[i-1] - n[i] < -1)) {
            int sign = (d >= 0) ? 1 : -1;
            double qip = parabolic(i, sign);
            if (q[i-1] < qip && qip < q[i+1]) {
                q[i] = qip;
            } else {
                q[i] = linear(i, sign);
            }
            n[i] += sign;
        }
    }

    private double parabolic(int i, int sign) {
        return q[i] + sign * (
                ((n[i] - n[i-1] + sign) * (q[i+1] - q[i]) / (n[i+1] - n[i])) +
                        ((n[i+1] - n[i] - sign) * (q[i] - q[i-1]) / (n[i] - n[i-1]))
        ) / (n[i+1] - n[i-1]);
    }

    private double linear(int i, int sign) {
        return q[i] + sign * (q[i + sign] - q[i]) / (n[i + sign] - n[i]);
    }
}

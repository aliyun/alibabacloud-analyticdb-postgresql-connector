package org.apache.flink.connector.jdbc.table.metric;

import org.apache.flink.metrics.Gauge;

/** LatencyGauge. */
public class SimpleGauge implements Gauge<Double> {
    private double value;

    public void report(long timeDelta, long batchSize) {
        if (batchSize != 0) {
            this.value = (1.0 * timeDelta) / batchSize;
        }
    }

    public void report(long value) {
        this.value = 1.0 * value;
    }

    public void report(double value) {
        this.value = value;
    }

    @Override
    public Double getValue() {
        return value;
    }
}
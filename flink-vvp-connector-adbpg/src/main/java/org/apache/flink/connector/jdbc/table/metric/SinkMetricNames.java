package org.apache.flink.connector.jdbc.table.metric;

import org.apache.flink.runtime.metrics.MetricNames;

/** An class that defines the metric names of the Sink (FLIP-33). */
public class SinkMetricNames {
    public static final String NUM_BYTES_OUT = MetricNames.IO_NUM_BYTES_OUT;
    public static final String NUM_BYTES_OUT_RATE = MetricNames.IO_NUM_BYTES_OUT_RATE;
    public static final String NUM_RECORDS_OUT = MetricNames.IO_NUM_RECORDS_OUT;
    public static final String NUM_RECORDS_OUT_RATE = MetricNames.IO_NUM_RECORDS_OUT_RATE;
    public static final String NUM_RECORDS_OUT_ERRORS = "numRecordsOutErrors";
    public static final String CURRENT_SEND_TIME = "currentSendTime";

    // VVR connector metrics.
    public static final String GROUP_SINK = "sink";
    public static final String SINK_IN_SKIP_COUNTER = "sinkSkipCount";
    private static final String SINK_IN_TPS = "inTps";
}

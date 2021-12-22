package org.apache.flink.connector.jdbc.table.metric;

import org.apache.flink.runtime.metrics.MetricNames;

/** An class that defines the metric names of the Source (FLIP-27). */
public class SourceMetricNames {
    // Source metrics convention specified in FLIP-33.
    public static final String NUM_RECORDS_IN = MetricNames.IO_NUM_RECORDS_IN;
    public static final String NUM_RECORDS_IN_RATE = MetricNames.IO_NUM_RECORDS_IN_RATE;
    public static final String NUM_BYTES_IN = MetricNames.IO_NUM_BYTES_IN;
    public static final String NUM_BYTES_IN_RATE = MetricNames.IO_NUM_BYTES_IN_RATE;
    public static final String CURRENT_FETCH_EVENT_TIME_LAG = "currentFetchEventTimeLag";
    public static final String CURRENT_EMIT_EVENT_TIME_LAG = "currentEmitEventTimeLag";
    public static final String WATERMARK_LAG = "watermarkLag";
    public static final String SOURCE_IDLE_TIME = "sourceIdleTime";
    public static final String PENDING_BYTES = "pendingBytes";
    public static final String PENDING_RECORDS = "pendingRecords";
    public static final String NUM_RECORDS_IN_ERRORS = "numRecordsInErrors";

    // Source metrics of VVR connectors
    public static final String NUM_RECORD_BATCHES_IN = "numRecordBatchesIn";
    public static final String NUM_RECORD_BATCHES_IN_RATE =
            NUM_RECORD_BATCHES_IN + MetricNames.SUFFIX_RATE;
    public static final String CURRENT_NUM_RECORDS_PER_BATCH = "currentNumRecordsPerBatch";
    public static final String SOURCE_PROCESS_LATENCY = "sourceProcessLatency";
    public static final String SOURCE_PARTITION_LATENCY = "sourcePartitionLatency";
    public static final String SOURCE_PARTITION_COUNT = "sourcePartitionCount";
}

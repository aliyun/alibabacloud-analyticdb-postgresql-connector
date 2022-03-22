/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table.metric;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.table.functions.FunctionContext;

/**
 * MetricUtils.
 */

/** code from ververica-connectors */
public class MetricUtils {

    public static final String METRICS_TAG_CONNECTOR_TYPE = "connector_type";

    public static final String SINK_METRIC_GROUP = "sink";
    public static final String SOURCE_METRIC_GROUP = "source";

    // ----------- Source metrics -------------------

    public static Meter registerNumRecordBatchesInRate(RuntimeContext context) {
        return context.getMetricGroup()
                .meter(
                        SourceMetricNames.NUM_RECORD_BATCHES_IN_RATE,
                        new MeterView(new SimpleCounter()));
    }

    public static Meter registerNumRecordsInRate(FunctionContext context) {
        return registerNumRecordsInRate(context.getMetricGroup());
    }

    public static Meter registerNumRecordsInRate(InitializationContext context) {
        return registerNumRecordsInRate(context.getMetricGroup());
    }

    public static Meter registerNumRecordsInRate(RuntimeContext context) {
        return registerNumRecordsInRate(context.getMetricGroup());
    }

    public static Meter registerNumRecordsInRate(MetricGroup metricGroup) {
        final Meter numRecordsInRateMeter =
                ((OperatorMetricGroup) metricGroup).getIOMetricGroup().getNumRecordsInRateMeter();
        final Counter numRecordsInCounter =
                ((OperatorMetricGroup) metricGroup).getIOMetricGroup().getNumRecordsInCounter();

        final MetricGroup sourceMetricGroup = metricGroup.addGroup(SOURCE_METRIC_GROUP);
        sourceMetricGroup.counter(SourceMetricNames.NUM_RECORDS_IN, numRecordsInCounter);
        sourceMetricGroup.meter(SourceMetricNames.NUM_RECORDS_IN_RATE, numRecordsInRateMeter);
        return numRecordsInRateMeter;
    }

    public static Counter registerNumRecordsInErrors(FunctionContext context) {
        return context.getMetricGroup().counter(SourceMetricNames.NUM_RECORDS_IN_ERRORS);
    }

    public static Counter registerNumRecordsInErrors(InitializationContext context) {
        return context.getMetricGroup().counter(SourceMetricNames.NUM_RECORDS_IN_ERRORS);
    }

    public static Gauge<Long> registerPendingRecords(RuntimeContext context, Gauge<Long> gauge) {
        return context.getMetricGroup().gauge(SourceMetricNames.PENDING_RECORDS, gauge);
    }

    public static Gauge<Long> registerSourceIdleTime(RuntimeContext context, Gauge<Long> gauge) {
        return context.getMetricGroup().gauge(SourceMetricNames.SOURCE_IDLE_TIME, gauge);
    }

    public static Meter registerNumBytesInRate(FunctionContext context, String sourceType) {
        return registerNumBytesInRate(context.getMetricGroup(), sourceType);
    }

    public static Meter registerNumBytesInRate(InitializationContext context, String sourceType) {
        return registerNumBytesInRate(context.getMetricGroup(), sourceType);
    }

    public static Meter registerNumBytesInRate(RuntimeContext context, String sourceType) {
        return registerNumBytesInRate(context.getMetricGroup(), sourceType);
    }

    public static Meter registerNumBytesInRate(MetricGroup metricGroup, String sourceType) {
        String taggedNumBytesIn =
                maybeAppendConnectorTypeTag(SourceMetricNames.NUM_BYTES_IN, sourceType);
        String taggedNumBytesInRate =
                maybeAppendConnectorTypeTag(SourceMetricNames.NUM_BYTES_IN_RATE, sourceType);
        final MetricGroup sourceMetricGroup = metricGroup.addGroup(SOURCE_METRIC_GROUP);

        Counter numBytesInCounter = metricGroup.counter(taggedNumBytesIn);
        sourceMetricGroup.counter(SourceMetricNames.NUM_BYTES_IN, numBytesInCounter);

        final MeterView meter = new MeterView(numBytesInCounter);
        metricGroup.meter(taggedNumBytesInRate, meter);
        sourceMetricGroup.meter(taggedNumBytesInRate, meter);

        return meter;
    }

    // ---------------- Common Sink Metrics ---------

    public static Meter registerNumRecordsOutRate(RuntimeContext context) {
        final Meter numRecordsOutRate =
                ((OperatorMetricGroup) context.getMetricGroup())
                        .getIOMetricGroup()
                        .getNumRecordsOutRate();
        final Counter numRecordsOutCounter =
                ((OperatorMetricGroup) context.getMetricGroup())
                        .getIOMetricGroup()
                        .getNumRecordsOutCounter();
        context.getMetricGroup()
                .addGroup(SINK_METRIC_GROUP)
                .meter(SinkMetricNames.NUM_RECORDS_OUT_RATE, numRecordsOutRate);
        context.getMetricGroup()
                .addGroup(SINK_METRIC_GROUP)
                .counter(SinkMetricNames.NUM_RECORDS_OUT, numRecordsOutCounter);
        return numRecordsOutRate;
    }

    public static Meter registerNumRecordsOutRate(Sink.InitContext context) {
        return ((OperatorMetricGroup) context.metricGroup())
                .getIOMetricGroup()
                .getNumRecordsOutRate();
    }

    public static Meter registerNumBytesOutRate(RuntimeContext context, String connectorType) {
        String taggedNumBytesOut =
                maybeAppendConnectorTypeTag(SinkMetricNames.NUM_BYTES_OUT, connectorType);
        String taggedNumBytesOutRate =
                maybeAppendConnectorTypeTag(SinkMetricNames.NUM_BYTES_OUT_RATE, connectorType);
        final MetricGroup metricGroup = context.getMetricGroup();
        final MetricGroup sinkMetricGroup = metricGroup.addGroup(SINK_METRIC_GROUP);

        Counter numBytesOutCounter = metricGroup.counter(taggedNumBytesOut);
        sinkMetricGroup.counter(SinkMetricNames.NUM_BYTES_OUT, numBytesOutCounter);

        final MeterView meter = new MeterView(numBytesOutCounter);
        metricGroup.meter(taggedNumBytesOutRate, meter);
        sinkMetricGroup.meter(taggedNumBytesOutRate, meter);

        return meter;
    }

    public static Meter registerNumBytesOutRate(Sink.InitContext context, String connectorType) {
        String taggedNumBytesOut =
                maybeAppendConnectorTypeTag(SinkMetricNames.NUM_BYTES_OUT, connectorType);
        String taggedNumBytesOutRate =
                maybeAppendConnectorTypeTag(SinkMetricNames.NUM_BYTES_OUT_RATE, connectorType);
        Counter numBytesOutCounter = context.metricGroup().counter(taggedNumBytesOut);
        return context.metricGroup()
                .meter(taggedNumBytesOutRate, new MeterView(numBytesOutCounter));
    }

    public static SimpleGauge registerCurrentSendTime(RuntimeContext context) {
        return context.getMetricGroup().gauge(SinkMetricNames.CURRENT_SEND_TIME, new SimpleGauge());
    }

    public static SimpleGauge registerCurrentSendTime(Sink.InitContext context) {
        return context.metricGroup().gauge(SinkMetricNames.CURRENT_SEND_TIME, new SimpleGauge());
    }

    public static Counter registerSinkDeleteCounter(RuntimeContext context) {
        return context.getMetricGroup().addGroup(SinkMetricNames.GROUP_SINK).counter("del");
    }

    public static Counter registerSinkDeleteCounter(Sink.InitContext context) {
        return context.metricGroup().addGroup(SinkMetricNames.GROUP_SINK).counter("del");
    }

    public static Counter registerNumRecordsOutErrors(RuntimeContext context) {
        return context.getMetricGroup()
                .counter(SinkMetricNames.NUM_RECORDS_OUT_ERRORS, new SimpleCounter());
    }

    // ---------------------

    private static String maybeAppendConnectorTypeTag(String name, String connectorType) {
        // NOTE: VVP is currently using Prometheus as metric service, which does not support
        // appending tags at the
        // end of metric names. Currently we just disable this functionality. Adding tags/labels for
        // metrics might
        // be introduced in the community in the future, or we can wrap a reporter on top of
        // Prometheus reporter to
        // support this feature.
        return name;
    }
}
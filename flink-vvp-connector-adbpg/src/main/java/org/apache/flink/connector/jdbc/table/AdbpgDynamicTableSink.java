package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.Objects;

/**
 * ADBPG Sink Implementation.
 * create AdbpgOutputFormat for detail implementation
 */
@Internal
public class AdbpgDynamicTableSink implements DynamicTableSink {

    private final String url;
    private final String tablename;
    private final String username;
    private final String password;
    private int fieldNum;
    private String[] fieldNamesStr;
    private String[] keyFields;
    private LogicalType[] lts;
    private int retryWaitTime;
    private int batchSize;
    private int batchWriteTimeoutMs;
    private int maxRetryTime;
    private int connectionMaxActive;
    private String conflictMode;
    private int useCopy;
    private String targetSchema;
    private String exceptionMode;
    private int reserveMS;
    private int caseSensitive;
    private int writeMode;
    private int verbose;

    public AdbpgDynamicTableSink(
            String url,
            String tablename,
            String username,
            String password,
            int fieldNum,
            String[] fieldNamesStr,
            String[] keyFields,
            LogicalType[] lts,
            int retryWaitTime,
            int batchSize,
            int batchWriteTimeoutMs,
            int maxRetryTime,
            int connectionMaxActive,
            String conflictMode,
            int useCopy,
            String targetSchema,
            String exceptionMode,
            int reserveMS,
            int caseSensitive,
            int writeMode,
            int verbose) {
        this.url = url;
        this.tablename = tablename;
        this.username = username;
        this.password = password;
        this.fieldNamesStr = fieldNamesStr;
        this.fieldNum = fieldNum;
        this.lts = lts;
        this.keyFields = keyFields;
        this.retryWaitTime = retryWaitTime;
        this.batchSize = batchSize;
        this.batchWriteTimeoutMs = batchWriteTimeoutMs;
        this.maxRetryTime = maxRetryTime;
        this.connectionMaxActive = connectionMaxActive;
        this.conflictMode = conflictMode;
        this.useCopy = useCopy;
        this.targetSchema = targetSchema;
        this.exceptionMode = exceptionMode;
        this.reserveMS = reserveMS;
        this.caseSensitive = caseSensitive;
        this.writeMode = writeMode;
        this.verbose = verbose;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final AdbpgOutputFormat aof = new AdbpgOutputFormat(url,
                tablename,
                username,
                password,
                fieldNum,
                fieldNamesStr,
                keyFields,
                lts,
                retryWaitTime,
                batchSize,
                batchWriteTimeoutMs,
                maxRetryTime,
                connectionMaxActive,
                conflictMode,
                useCopy,
                targetSchema,
                exceptionMode,
                reserveMS,
                caseSensitive,
                writeMode,
                verbose);
        return OutputFormatProvider.of(aof);
    }

    @Override
    public DynamicTableSink copy() {
        return new AdbpgDynamicTableSink(url,
                tablename,
                username,
                password,
                fieldNum,
                fieldNamesStr,
                keyFields,
                lts,
                retryWaitTime,
                batchSize,
                batchWriteTimeoutMs,
                maxRetryTime,
                connectionMaxActive,
                conflictMode,
                useCopy,
                targetSchema,
                exceptionMode,
                reserveMS,
                caseSensitive,
                writeMode,
                verbose);
    }

    @Override
    public String asSummaryString() {
        return "ADBPG Sink:";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AdbpgDynamicTableSink)) {
            return false;
        }
        AdbpgDynamicTableSink that = (AdbpgDynamicTableSink) o;
        return Objects.equals(url, that.url)
                && Objects.equals(tablename, that.tablename)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(fieldNum, that.fieldNum)
                && Arrays.equals(fieldNamesStr, that.fieldNamesStr)
                && Arrays.equals(keyFields, that.keyFields)
                && Arrays.equals(lts, that.lts)
                && Objects.equals(retryWaitTime, that.retryWaitTime)
                && Objects.equals(batchSize, that.batchSize)
                && Objects.equals(batchWriteTimeoutMs, that.batchWriteTimeoutMs)
                && Objects.equals(maxRetryTime, that.maxRetryTime)
                && Objects.equals(connectionMaxActive, that.connectionMaxActive)
                && Objects.equals(conflictMode, that.conflictMode)
                && Objects.equals(useCopy, that.useCopy)
                && Objects.equals(targetSchema, that.targetSchema)
                && Objects.equals(exceptionMode, that.exceptionMode)
                && Objects.equals(reserveMS, that.reserveMS)
                && Objects.equals(caseSensitive, that.caseSensitive)
                && Objects.equals(writeMode, that.writeMode)
                && Objects.equals(verbose, that.verbose);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url,
                tablename,
                username,
                password,
                fieldNum,
                fieldNamesStr,
                keyFields,
                lts,
                retryWaitTime,
                batchSize,
                batchWriteTimeoutMs,
                maxRetryTime,
                connectionMaxActive,
                conflictMode,
                useCopy,
                targetSchema,
                exceptionMode,
                reserveMS,
                caseSensitive,
                writeMode,
                verbose);
    }
}

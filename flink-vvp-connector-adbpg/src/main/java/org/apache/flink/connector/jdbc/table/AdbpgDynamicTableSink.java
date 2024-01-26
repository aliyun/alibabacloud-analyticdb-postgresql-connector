package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.table.sink.AdbpgOutputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import java.util.Arrays;
import java.util.Objects;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.ACCESS_METHOD;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.BATCH_SIZE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.BATCH_WRITE_TIMEOUT_MS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CASE_SENSITIVE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONFLICT_MODE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONNECTION_MAX_ACTIVE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.EXCEPTION_MODE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.MAX_RETRY_TIMES;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.RESERVEMS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.RETRY_WAIT_TIME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.TARGET_SCHEMA;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.URL;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.USE_COPY;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.VERBOSE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.WRITE_MODE;

/**
 * ADBPG Sink Implementation.
 * create AdbpgOutputFormat for detail implementation
 */
@Internal
public class AdbpgDynamicTableSink implements DynamicTableSink {

    private final ReadableConfig config;
    private  TableSchema tableSchema;
    private int fieldNum;
    private String[] fieldNamesStr;
    private String[] keyFields;
    private LogicalType[] lts;

    public AdbpgDynamicTableSink(
            ReadableConfig config,
            TableSchema tableSchema
    ) {
        this.config = config;
        this.tableSchema = tableSchema;
        this.fieldNum = tableSchema.getFieldCount();
        this.fieldNamesStr = new String[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            this.fieldNamesStr[i] = tableSchema.getFieldName(i).get();
        }
        this.keyFields =
                tableSchema.getPrimaryKey()
                        .map(pk -> pk.getColumns().toArray(new String[0]))
                        .orElse(null);
        this.lts = new LogicalType[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            this.lts[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
        }
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
        final AdbpgOutputFormat aof = new AdbpgOutputFormat(fieldNum, fieldNamesStr, keyFields, lts, config);
        return OutputFormatProvider.of(aof);
    }

    @Override
    public DynamicTableSink copy() {
        return new AdbpgDynamicTableSink(config, tableSchema);
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
        return Objects.equals(this.config.get(URL),  that.config.get(URL))
                && Objects.equals(this.config.get(TABLE_NAME), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(USERNAME), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(PASSWORD), that.config.get(TABLE_NAME))
                && Objects.equals(fieldNum, that.fieldNum)
                && Arrays.equals(fieldNamesStr, that.fieldNamesStr)
                && Arrays.equals(keyFields, that.keyFields)
                && Arrays.equals(lts, that.lts)
                && Objects.equals(this.config.get(RETRY_WAIT_TIME), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(BATCH_SIZE), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(BATCH_WRITE_TIMEOUT_MS), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(MAX_RETRY_TIMES), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(CONNECTION_MAX_ACTIVE), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(CONFLICT_MODE), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(ACCESS_METHOD), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(USE_COPY), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(TARGET_SCHEMA), that.config.get(TABLE_NAME))
                && Objects.equals(this.config.get(EXCEPTION_MODE), that.config.get(EXCEPTION_MODE))
                && Objects.equals(this.config.get(RESERVEMS), that.config.get(RESERVEMS))
                && Objects.equals(this.config.get(CASE_SENSITIVE), that.config.get(CASE_SENSITIVE))
                && Objects.equals(this.config.get(WRITE_MODE), that.config.get(WRITE_MODE))
                && Objects.equals(this.config.get(VERBOSE), that.config.get(VERBOSE));
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                config,
                tableSchema);
    }
}

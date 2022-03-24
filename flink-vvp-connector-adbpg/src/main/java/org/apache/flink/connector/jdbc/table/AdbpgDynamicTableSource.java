package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.table.sourceImpl.AdbpgDataScanFunction;
import org.apache.flink.connector.jdbc.table.sourceImpl.AdbpgRowDataLookupFunction;
import org.apache.flink.connector.jdbc.table.utils.AdbpgDialect;
import org.apache.flink.connector.jdbc.table.utils.AdbpgOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import java.util.Arrays;
import java.util.List;

/**
 * ADBPG Source Implementation.
 * parse look up keys and
 * create AdbpgRowDataLookupFunction for detail implementation
 */
public class AdbpgDynamicTableSource implements LookupTableSource, ScanTableSource, SupportsProjectionPushDown,
        SupportsLimitPushDown {
    private int fieldNum;
    private String[] fieldNamesStr;
    private LogicalType[] lts;
    private ReadableConfig config;
    private TableSchema tableSchema;
    private long limit = -1;

    public AdbpgDynamicTableSource(int fieldNum, String[] fieldNamesStr, LogicalType[] lts,
                                   ReadableConfig config, TableSchema tableSchema) {
        this.fieldNum = fieldNum;
        this.fieldNamesStr = fieldNamesStr;
        this.lts = lts;
        this.config = config;
        this.tableSchema = tableSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        String[] keyNames = new String[lookupContext.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = fieldNamesStr[innerKeyArr[0]];
        }
        List<String> nameList = Arrays.asList(fieldNamesStr);
        LogicalType[] keyTypes =
                Arrays.stream(keyNames)
                        .map(s -> {
                            Preconditions.checkArgument(
                                    nameList.contains(s),
                                    "keyName %s can't find in fieldNames %s.",
                                    s,
                                    nameList);
                            return lts[nameList.indexOf(s)];
                        })
                        .toArray(LogicalType[]::new);

        return TableFunctionProvider.of(
                new AdbpgRowDataLookupFunction(
                        fieldNum,
                        fieldNamesStr,
                        lts,
                        keyNames,
                        keyTypes,
                        config));
    }

    @Override
    public DynamicTableSource copy() {
        return new AdbpgDynamicTableSource(fieldNum, fieldNamesStr, lts, config, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "ADBPG Source:";
    }

    @Override
    public ChangelogMode getChangelogMode() {

        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        JdbcReadOptions readOptions = AdbpgOptions.getJdbcReadOptions(config);
        AdbpgDialect adbpgDialect = new AdbpgDialect(config.get(AdbpgOptions.TARGET_SCHEMA), AdbpgOptions.isConfigOptionTrue(config, AdbpgOptions.CASE_SENSITIVE));
        String query =
                adbpgDialect.getSelectFromStatement(
                        config.get(AdbpgOptions.TABLE_NAME), tableSchema.getFieldNames(), new String[0]);
        Object[][] parameterValues = null;
        if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            JdbcParameterValuesProvider parameterValuesProvider = new JdbcNumericBetweenParametersProvider(lowerBound, upperBound).ofBatchNum(numPartitions);
            parameterValues = parameterValuesProvider.getParameterValues();
            query +=
                    " WHERE "
                            + adbpgDialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                            + " BETWEEN %s AND %s";
        }
        if (limit >= 0) {
            query = String.format("%s limit %s", query, limit);
        }

        final InputFormat<RowData, InputSplit> inputFunction = new AdbpgDataScanFunction(
                fieldNum,
                fieldNamesStr,
                lts,
                config,
                tableSchema, parameterValues, query);

        return InputFormatProvider.of(inputFunction);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.tableSchema = TableSchemaUtils.projectSchema(tableSchema, projectedFields);
    }
}

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.table.utils.AdbpgOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Table Factory for ADBPG connector.
 * createDynamicTableSink: create ADBPG sink
 * createDynamicTableSource: create ADBPG source
 */
@Internal
public class AdbpgDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AdbpgDynamicTableFactory.class);

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        LOG.info("Start to create adbpg sink.");
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        LOG.info("Try to get and validate configuration.");
        int fieldNum = tableSchema.getFieldCount();
        String[] fieldNamesStr = new String[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            fieldNamesStr[i] = tableSchema.getFieldName(i).get();
        }
        String[] keyFields =
                tableSchema.getPrimaryKey()
                        .map(pk -> pk.getColumns().toArray(new String[0]))
                        .orElse(null);
        LogicalType[] lts = new LogicalType[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            lts[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
        }
        AdbpgOptions.validateSink(config, tableSchema);
        LOG.info("Validation passed, adbpg sink created successfully.");
        return new AdbpgDynamicTableSink(config, tableSchema);
    }

    @Override
    public String factoryIdentifier() {
        return AdbpgOptions.CONNECTOR_TYPE;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(AdbpgOptions.URL);
        requiredOptions.add(AdbpgOptions.TABLE_NAME);
        requiredOptions.add(AdbpgOptions.USERNAME);
        requiredOptions.add(AdbpgOptions.PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(AdbpgOptions.MAX_RETRY_TIMES);
        optionalOptions.add(AdbpgOptions.BATCH_SIZE);
        optionalOptions.add(AdbpgOptions.BATCH_WRITE_TIMEOUT_MS);
        optionalOptions.add(AdbpgOptions.CONNECTION_MAX_ACTIVE);
        optionalOptions.add(AdbpgOptions.CONFLICT_MODE);
        optionalOptions.add(AdbpgOptions.USE_COPY);
        optionalOptions.add(AdbpgOptions.TARGET_SCHEMA);
        optionalOptions.add(AdbpgOptions.EXCEPTION_MODE);
        optionalOptions.add(AdbpgOptions.RESERVEMS);
        optionalOptions.add(AdbpgOptions.CASE_SENSITIVE);
        optionalOptions.add(AdbpgOptions.WRITE_MODE);
        optionalOptions.add(AdbpgOptions.RETRY_WAIT_TIME);
        optionalOptions.add(AdbpgOptions.JOINMAXROWS);
        optionalOptions.add(AdbpgOptions.CACHE);
        optionalOptions.add(AdbpgOptions.CACHESIZE);
        optionalOptions.add(AdbpgOptions.CACHETTLMS);
        optionalOptions.add(AdbpgOptions.VERBOSE);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        LOG.info("Start to create adbpg source.");
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        LOG.info("Try to get and validate configuration.");
        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        int fieldNum = tableSchema.getFieldCount();
        String[] fieldNamesStr = new String[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            fieldNamesStr[i] = tableSchema.getFieldName(i).get();
        }
        LogicalType[] lts = new LogicalType[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            lts[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
        }

        AdbpgOptions.validateSource(config, tableSchema);
        LOG.info("Validation passed, adbpg source created successfully.");
        return new AdbpgDynamicTableSource(fieldNum, fieldNamesStr, lts, config, tableSchema);
    }
}

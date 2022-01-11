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
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.JOINMAXROWS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CACHESIZE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CACHE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CACHETTLMS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.validateSource;

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
        return "adbpg-nightly";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(MAX_RETRY_TIMES);
        optionalOptions.add(BATCH_SIZE);
        optionalOptions.add(BATCH_WRITE_TIMEOUT_MS);
        optionalOptions.add(CONNECTION_MAX_ACTIVE);
        optionalOptions.add(CONFLICT_MODE);
        optionalOptions.add(USE_COPY);
        optionalOptions.add(TARGET_SCHEMA);
        optionalOptions.add(EXCEPTION_MODE);
        optionalOptions.add(RESERVEMS);
        optionalOptions.add(CASE_SENSITIVE);
        optionalOptions.add(WRITE_MODE);
        optionalOptions.add(RETRY_WAIT_TIME);
        optionalOptions.add(JOINMAXROWS);
        optionalOptions.add(CACHE);
        optionalOptions.add(CACHESIZE);
        optionalOptions.add(CACHETTLMS);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        LOG.info("Start to create adbpg source.");
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        LOG.info("Try to get and validate configuration.");
        String url = config.get(URL);
        String tablename = config.get(TABLE_NAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        int retryWaitTime = config.getOptional(RETRY_WAIT_TIME).orElse(100);
        int batchWriteTimeoutMs = config.getOptional(BATCH_WRITE_TIMEOUT_MS).orElse(5000);
        int maxRetryTime = config.getOptional(MAX_RETRY_TIMES).orElse(3);
        int connectionMaxActive = config.getOptional(CONNECTION_MAX_ACTIVE).orElse(5);
        int verbose = config.getOptional(VERBOSE).orElse(0);
        String exceptionMode = config.getOptional(EXCEPTION_MODE).orElse("ignore");
        String targetSchema = config.getOptional(TARGET_SCHEMA).orElse("public");
        int caseSensitive = config.getOptional(CASE_SENSITIVE).orElse(0);
        int joinMaxRows = config.getOptional(JOINMAXROWS).orElse(1024);
        String cache = config.getOptional(CACHE).orElse("none");
        int cacheSize = config.getOptional(CACHESIZE).orElse(10000);
        int cacheTTLMs = config.getOptional(CACHETTLMS).orElse(2000000000);

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

        validateSource(config, tableSchema);
        LOG.info("Validation passed, adbpg source created successfully.");
        return new AdbpgDynamicTableSource(url,
                tablename,
                username,
                password,
                fieldNum,
                fieldNamesStr,
                lts,
                retryWaitTime,
                batchWriteTimeoutMs,
                maxRetryTime,
                connectionMaxActive,
                exceptionMode,
                targetSchema,
                caseSensitive,
                joinMaxRows,
                cache,
                cacheSize,
                cacheTTLMs,
                verbose);
    }


}

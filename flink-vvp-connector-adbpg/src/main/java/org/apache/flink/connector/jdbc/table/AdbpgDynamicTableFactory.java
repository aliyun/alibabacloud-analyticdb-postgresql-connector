package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
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
public class AdbpgDynamicTableFactory implements  DynamicTableSinkFactory, DynamicTableSourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AdbpgDynamicTableFactory.class);

    // required options
    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The jdbc database url.");
    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("tablename")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The jdbc table name.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The jdbc user name.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The jdbc password.");

    // common optional options
    public static final ConfigOption<Integer> RETRY_WAIT_TIME =
            ConfigOptions.key("retrywaittime")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Retry Wait Time");
    public static final ConfigOption<Integer> MAX_RETRY_TIMES =
            ConfigOptions.key("maxretrytimes")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Max retry times when execute query");
    public static final ConfigOption<String> TARGET_SCHEMA =
            ConfigOptions.key("targetschema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target schema for data.");
    public static final ConfigOption<String> EXCEPTION_MODE =
            ConfigOptions.key("exceptionmode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exception Mode");
    public static final ConfigOption<Integer> VERBOSE =
            ConfigOptions.key("VERBOSE")
                    .intType()
                    .noDefaultValue()
                    .withDescription("VERBOSE OR NOT");

    //optional sink options
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batchsize")
                    .intType()
                    .noDefaultValue()
                    .withDescription("batch size for data import");
    public static final ConfigOption<Integer> BATCH_WRITE_TIMEOUT_MS =
            ConfigOptions.key("batchWriteTimeoutMs")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Timeout setting");
    public static final ConfigOption<Integer> CONNECTION_MAX_ACTIVE =
            ConfigOptions.key("connectionmaxactive")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Max Active Connections");
    public static final ConfigOption<String> CONFLICT_MODE =
            ConfigOptions.key("conflictmode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Conflict Mode");
    public static final ConfigOption<Integer> USE_COPY =
            ConfigOptions.key("usecopy")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Whether to use copy for data import.");
    public static final ConfigOption<Integer> RESERVEMS =
            ConfigOptions.key("reservems")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Whether to reserve millseconds.");
    public static final ConfigOption<Integer> CASE_SENSITIVE =
            ConfigOptions.key("casesensitive")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Case Sensitive");
    public static final ConfigOption<Integer> WRITE_MODE =
            ConfigOptions.key("writemode")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Write Mode");

    //optional dim options
    public static final ConfigOption<Integer> JOINMAXROWS =
            ConfigOptions.key("joinmaxrows")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Join Max Rows");
    public static final ConfigOption<String> CACHE =
            ConfigOptions.key("cache")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cache Strategy");
    public static final ConfigOption<Integer> CACHESIZE =
            ConfigOptions.key("cachesize")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Cache Size");
    public static final ConfigOption<Integer> CACHETTLMS =
            ConfigOptions.key("cachettlms")
                    .intType()
                    .noDefaultValue()
                    .withDescription("cacheTTLMs");

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
        String url =  config.get(URL);
        String tablename = config.get(TABLE_NAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        int retryWaitTime = config.getOptional(RETRY_WAIT_TIME).orElse(100);
        int batchSize = config.getOptional(BATCH_SIZE).orElse(500);
        int batchWriteTimeoutMs = config.getOptional(BATCH_WRITE_TIMEOUT_MS).orElse(5000);
        int maxRetryTime = config.getOptional(MAX_RETRY_TIMES).orElse(3);
        int connectionMaxActive = config.getOptional(CONNECTION_MAX_ACTIVE).orElse(5);
        String conflictMode = config.getOptional(CONFLICT_MODE).orElse("ignore");
        int verbose = config.getOptional(VERBOSE).orElse(1);
        int useCopy = config.getOptional(USE_COPY).orElse(0);
        String targetSchema = config.getOptional(TARGET_SCHEMA).orElse("public");
        String exceptionMode = config.getOptional(EXCEPTION_MODE).orElse("ignore");
        int reserveMS = config.getOptional(RESERVEMS).orElse(0);
        int caseSensitive = config.getOptional(CASE_SENSITIVE).orElse(0);
        int writeMode = config.getOptional(WRITE_MODE).orElse(0);
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
        for(int i = 0; i < fieldNum; i++) {
            lts[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
        }
        validateSink(url,
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
        LOG.info("Validation passed, adbpg sink created successfully.");
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
                verbose
        );
    }

    @Override
    public String factoryIdentifier() {
        return "adbpg";
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
        String url =  config.get(URL);
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
        for(int i = 0; i < fieldNum; i++) {
            lts[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
        }

        validateSource(url,
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
    private void validateSink(String url,
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
        if (!url.contains("jdbc:postgresql:")) {
            throw new RuntimeException("invalid url, get " + url + ", expected jdbc:postgresql://hostname:port/database");
        }
        if (tablename == null || tablename.trim().length() == 0) {
            throw new RuntimeException("invalid tablename, get " + tablename);
        }
        if (username == null || username.trim().length() == 0) {
            throw new RuntimeException("invalid username, get " + username);
        }
        if (password == null || password.trim().length() == 0) {
            throw new RuntimeException("invalid password, get " + password);
        }
        if (fieldNum <= 0) {
            throw new RuntimeException("invalid fieldNum, get " + fieldNum);
        }
        if (retryWaitTime <= 0) {
            throw new RuntimeException("invalid retryWaitTime, get " + retryWaitTime);
        }
        if (batchSize <= 0) {
            throw new RuntimeException("invalid batchSize, get " + batchSize);
        }
        if (batchWriteTimeoutMs <= 0) {
            throw new RuntimeException("invalid batchWriteTimeoutMs, get " + batchWriteTimeoutMs);
        }
        if (maxRetryTime <= 0) {
            throw new RuntimeException("invalid maxRetryTime, get " + maxRetryTime);
        }
        if (connectionMaxActive <= 0) {
            throw new RuntimeException("invalid connectionMaxActive, get " + connectionMaxActive);
        }
        if (!"ignore".equalsIgnoreCase(conflictMode) && !"update".equalsIgnoreCase(conflictMode)&& !"upsert".equalsIgnoreCase(conflictMode)&& !"strict".equalsIgnoreCase(conflictMode)) {
            throw new RuntimeException("invlid conflictMode, get " + conflictMode + " ,expected ignore/upsert/update/strict");
        }
        if (useCopy != 0 && useCopy != 1) {
            throw new RuntimeException("invalid useCopy, get " + useCopy+" expected 0 or 1");
        }
        if (targetSchema.length() == 0) {
            throw new RuntimeException("invalid targetschema, get " + targetSchema);
        }
        if (!"ignore".equalsIgnoreCase(exceptionMode) && !"strict".equalsIgnoreCase(exceptionMode)) {
            throw new RuntimeException("invlid exceptionMode, get " + exceptionMode + " ,expected ignore/strict");
        }
        if (reserveMS != 0 && reserveMS != 1) {
            throw new RuntimeException("invalid reserveMS, get " + reserveMS+" expected 0 or 1");
        }
        if (caseSensitive != 0 && caseSensitive != 1) {
            throw new RuntimeException("invalid caseSensitive, get " + caseSensitive+" expected 0 or 1");
        }
        if (writeMode != 0 && writeMode != 1 && writeMode != 2) {
            throw new RuntimeException("invalid writeMode, get " + writeMode+" expected 0/1/2");
        }
        if (verbose != 0 && verbose != 1) {
            throw new RuntimeException("invalid verbose, get " + verbose + ", expected 0 or 1");
        }
    }

    private void validateSource(String url,
                                String tablename,
                                String username,
                                String password,
                                int fieldNum,
                                String[] fieldNamesStr,
                                LogicalType[] lts,
                                int retryWaitTime,
                                int batchWriteTimeoutMs,
                                int maxRetryTime,
                                int connectionMaxActive,
                                String exceptionMode,
                                String targetSchema,
                                int caseSensitive,
                                int joinMaxRows,
                                String cache,
                                int cacheSize,
                                int cacheTTLMs,
                                int verbose){
        if (!url.contains("jdbc:postgresql:")) {
            throw new RuntimeException("invalid url, get " + url + ", expected jdbc:postgresql://hostname:port/database");
        }
        if (tablename == null || tablename.trim().length() == 0) {
            throw new RuntimeException("invalid tablename, get " + tablename);
        }
        if (username == null || username.trim().length() == 0) {
            throw new RuntimeException("invalid username, get " + username);
        }
        if (password == null || password.trim().length() == 0) {
            throw new RuntimeException("invalid password, get " + password);
        }
        if (fieldNum <= 0) {
            throw new RuntimeException("invalid fieldNum, get " + fieldNum);
        }
        if (retryWaitTime <= 0) {
            throw new RuntimeException("invalid retryWaitTime, get " + retryWaitTime);
        }
        if (batchWriteTimeoutMs <= 0) {
            throw new RuntimeException("invalid batchWriteTimeoutMs, get " + batchWriteTimeoutMs);
        }
        if (maxRetryTime <= 0) {
            throw new RuntimeException("invalid maxRetryTime, get " + maxRetryTime);
        }
        if (connectionMaxActive <= 0) {
            throw new RuntimeException("invalid connectionMaxActive, get " + connectionMaxActive);
        }
        if (targetSchema.length() == 0) {
            throw new RuntimeException("invalid targetschema, get " + targetSchema);
        }
        if (caseSensitive != 0 && caseSensitive != 1) {
            throw new RuntimeException("invalid caseSensitive, get " + caseSensitive+" expected 0 or 1");
        }
        if (joinMaxRows <= 0) {
            throw new RuntimeException("invalid joinMaxRows, get " + joinMaxRows);
        }
        if (!"none".equalsIgnoreCase(cache) && !"lru".equalsIgnoreCase(cache)) {
            throw new RuntimeException("invlid cache, get " + cache + " ,expected none/lru");
        }
        if (cacheSize <= 0) {
            throw new RuntimeException("invalid cacheSize, get " + cacheSize);
        }
        if (cacheTTLMs <= 0) {
            throw new RuntimeException("invalid cacheTTLMs, get " + cacheTTLMs);
        }
        if (verbose != 0 && verbose != 1) {
            throw new RuntimeException("invalid verbose, get " + verbose + ", expected 0 or 1");
        }
    }
}

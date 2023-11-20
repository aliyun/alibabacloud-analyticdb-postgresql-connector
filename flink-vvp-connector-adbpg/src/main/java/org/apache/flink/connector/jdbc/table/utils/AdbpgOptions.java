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

package org.apache.flink.connector.jdbc.table.utils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Adbpg options.
 */
public class AdbpgOptions {
    private static final transient Logger LOG = LoggerFactory.getLogger(AdbpgOptions.class);

    public static final String CONNECTOR_TYPE = "adbpg-nightly-1.13";
    public static final String DRIVER_CLASS = "org.postgresql.Driver";

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
                    .defaultValue(100)
                    .withDescription("Retry Wait Time");
    public static final ConfigOption<Integer> MAX_RETRY_TIMES =
            ConfigOptions.key("maxretrytimes")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Max retry times when execute query");
    public static final ConfigOption<String> TARGET_SCHEMA =
            ConfigOptions.key("targetschema")
                    .stringType()
                    .defaultValue("public")
                    .withDescription("Target schema for data.");
    public static final ConfigOption<String> EXCEPTION_MODE =
            ConfigOptions.key("exceptionmode")
                    .stringType()
                    .defaultValue("ignore")
                    .withDescription("Exception Mode");
    public static final ConfigOption<Integer> VERBOSE =
            ConfigOptions.key("verbose")
                    .intType()
                    .defaultValue(1)
                    .withDescription("VERBOSE OR NOT");
    //optional sink options
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batchsize")
                    .intType()
                    .defaultValue(50000)
                    .withDescription("batch size for data import");
    public static final ConfigOption<Integer> BATCH_WRITE_TIMEOUT_MS =
            ConfigOptions.key("batchwritetimeoutms")
                    .intType()
                    .defaultValue(50000)
                    .withDescription("Timeout setting");
    public static final ConfigOption<Integer> CONNECTION_MAX_ACTIVE =
            ConfigOptions.key("connectionmaxactive")
                    .intType()
                    .defaultValue(5)
                    .withDescription("Max Active Connections");
    public static final ConfigOption<String> CONFLICT_MODE =
            ConfigOptions.key("conflictmode")
                    .stringType()
                    .defaultValue("upsert")
                    .withDescription("Conflict Mode");
    public static final ConfigOption<Integer> USE_COPY =
            ConfigOptions.key("usecopy")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Whether to use copy for data import.");
    public static final ConfigOption<Integer> RESERVEMS =
            ConfigOptions.key("reservems")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Whether to reserve millseconds.");
    public static final ConfigOption<Integer> CASE_SENSITIVE =
            ConfigOptions.key("casesensitive")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Case Sensitive");
    public static final ConfigOption<Integer> WRITE_MODE =
            ConfigOptions.key("writemode")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Write Mode");
    //optional dim options
    public static final ConfigOption<Integer> JOINMAXROWS =
            ConfigOptions.key("joinmaxrows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Join Max Rows");
    public static final ConfigOption<String> CACHE =
            ConfigOptions.key("cache")
                    .stringType()
                    .defaultValue("none")
                    .withDescription("Cache Strategy");
    public static final ConfigOption<Integer> CACHESIZE =
            ConfigOptions.key("cachesize")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Cache Size");
    public static final ConfigOption<Integer> CACHETTLMS =
            ConfigOptions.key("cachettlms")
                    .intType()
                    .defaultValue(2000000000)
                    .withDescription("cacheTTLMs");

    public static final ConfigOption<Long> CONNECTION_MAX_WAIT =
            key("connectionMaxWait").longType().defaultValue(15000L);

    // read config options from JDBC connector
    private static final ConfigOption<String> SCAN_PARTITION_COLUMN =
            ConfigOptions.key("scan.partition.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The column name used for partitioning the input.");
    private static final ConfigOption<Integer> SCAN_PARTITION_NUM =
            ConfigOptions.key("scan.partition.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of partitions.");
    private static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND =
            ConfigOptions.key("scan.partition.lower-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The smallest value of the first partition.");
    private static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND =
            ConfigOptions.key("scan.partition.upper-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The largest value of the last partition.");
    private static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Gives the reader a hint as to the number of rows that should be fetched "
                                    + "from the database per round-trip when reading. "
                                    + "If the value is zero, this hint is ignored.");
    private static final ConfigOption<Boolean> SCAN_AUTO_COMMIT =
            ConfigOptions.key("scan.auto-commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Sets whether the driver is in auto-commit mode.");

    public static boolean isCaseSensitive(ReadableConfig readableConfig) {
        switch (readableConfig.get(CASE_SENSITIVE).toString().toLowerCase()) {
            case "0":
            case "false":
                return false;
            case "1":
            case "true":
                return true;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid value for config %s : %s",
                                CASE_SENSITIVE.key(),
                                readableConfig.get(CASE_SENSITIVE)));
        }
    }

    public static boolean isConfigOptionTrue(ReadableConfig readableConfig, ConfigOption<Integer> target) {
        switch (readableConfig.get(target).toString().toLowerCase()) {
            case "0":
            case "false":
                return false;
            case "1":
            case "true":
                return true;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid value for config %s : %s",
                                target.key(),
                                readableConfig.get(target)));
        }
    }

    public static ConflictMode getConflictMode(ReadableConfig readableConfig) {
        switch (readableConfig.get(CONFLICT_MODE).toLowerCase()) {
            case "ignore":
                return ConflictMode.ignore;
            case "strict":
                return ConflictMode.strict;
            case "update":
                return ConflictMode.update;
            case "upsert":
                return ConflictMode.upsert;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid value for config %s : %s",
                                CONFLICT_MODE.key(),
                                readableConfig.get(CONFLICT_MODE)));
        }
    }

    public static WriteMode getWriteMode(ReadableConfig readableConfig) {
        switch (readableConfig.get(WRITE_MODE).toString().toLowerCase()) {
            case "insert":
            case "0":
                return WriteMode.insert;
            case "copy":
            case "1":
                return WriteMode.copy;
            case "upsert":
            case "2":
                return WriteMode.upsert;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid value for config %s : %s",
                                WRITE_MODE.key(),
                                readableConfig.get(WRITE_MODE)));
        }
    }

    public static ExceptionMode getExceptionMode(ReadableConfig readableConfig) {
        switch (readableConfig.get(EXCEPTION_MODE).toLowerCase()) {
            case "ignore":
                return ExceptionMode.ignore;
            case "strict":
                return ExceptionMode.strict;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid value for config %s : %s",
                                EXCEPTION_MODE.key(),
                                readableConfig.get(EXCEPTION_MODE)));
        }
    }

    /**
     * Conflict mode.
     */
    public enum ConflictMode {
        ignore,
        strict,
        update,
        upsert;

        public static List<String> stringList() {
            return Stream.of(ConflictMode.values()).map(Enum::name).collect(Collectors.toList());
        }
    }

    /**
     * cache mode.
     */
    public enum CacheMode {
        none,
        all,
        lru;

        public static List<String> stringList() {
            return Stream.of(CacheMode.values()).map(Enum::name).collect(Collectors.toList());
        }
    }

    /**
     * Write mode.
     */
    public enum WriteMode {
        insert,
        copy,
        upsert;

        public static List<Integer> integerList() {
            return Stream.of(WriteMode.values()).map(Enum::ordinal).collect(Collectors.toList());
        }
    }

    /**
     * common enum for 0 and 1
     */
    public enum ExceptionMode {
        ignore,
        strict;

        public static List<String> stringList() {
            return Stream.of(ExceptionMode.values()).map(Enum::name).collect(Collectors.toList());
        }
    }

    /**
     * common enum for 0 and 1
     */
    public enum ZeroOrOneEnum {
        Zero,
        One;

        public static List<Integer> integerList() {
            return Stream.of(ZeroOrOneEnum.values()).map(Enum::ordinal).collect(Collectors.toList());
        }
    }

    public static DruidDataSource buildDataSourceFromOptions(ReadableConfig config) {

        String url = config.get(AdbpgOptions.URL);
        String userName = config.get(AdbpgOptions.USERNAME);
        String password = config.get(AdbpgOptions.PASSWORD);
        if (!url.startsWith("jdbc:postgresql://")) {
            throw new IllegalArgumentException(
                    String.format(
                            "url of %s must starts with jdbc:postgresql:// format, but actual url is %s",
                            CONNECTOR_TYPE, url));
        }

        int connectionMaxActive = config.get(CONNECTION_MAX_ACTIVE);
        long connectionMaxWait = config.get(CONNECTION_MAX_WAIT);

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(userName);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(AdbpgOptions.DRIVER_CLASS);
        dataSource.setMaxActive(connectionMaxActive);
        dataSource.setMaxWait(connectionMaxWait);
        dataSource.setInitialSize(0);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setValidationQuery("select 'adbpg_flink_connector'");
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setTimeBetweenEvictionRunsMillis(180000);
        dataSource.setMinEvictableIdleTimeMillis(3600000);
        dataSource.setMaxEvictableIdleTimeMillis(9000000);
        dataSource.setRemoveAbandoned(false);
        dataSource.setRemoveAbandonedTimeout(300);
        LOG.info("connector " + CONNECTOR_TYPE + " created using url=" + url + ", "
                + "tableName=" + config.get(TABLE_NAME) + ", "
                + "userName=" + userName + ", "
                + "password=" + password + ", "
                + "maxRetries=" + config.get(MAX_RETRY_TIMES) + ", "
                + "retryWaitTime=" + config.get(RETRY_WAIT_TIME) + ", "
                + "batchSize=" + config.get(BATCH_SIZE) + ", "
                + "connectionMaxActive=" + connectionMaxActive + ", "
                + "batchWriteTimeoutMs=" + config.get(BATCH_WRITE_TIMEOUT_MS) + ", "
                + "conflictMode=" + config.get(CONFLICT_MODE) + ", "
                + "timeZone=" + "Asia/Shanghai" + ", "
                + "useCopy=" + config.get(USE_COPY) + ", "
                + "targetSchema=" + config.get(TARGET_SCHEMA) + ", "
                + "exceptionMode=" + config.get(EXCEPTION_MODE) + ", "
                + "reserveMs=" + config.get(RESERVEMS) + ", "
                + "caseSensitive=" + config.get(CASE_SENSITIVE) + ", "
                + "writeMode=" + config.get(WRITE_MODE) + ", "
                + "verbose=" + config.get(VERBOSE));
        return dataSource;
    }

    public static void validateStringConfigOption(
            ReadableConfig config, ConfigOption<String> configOption) {
        if (config.getOptional(configOption).isPresent()) {

            if (YaStringUtils.isEmpty(config.get(configOption)) || config.get(configOption).trim().length() == 0) {
                throw new IllegalArgumentException(
                        String.format(
                                " option %s should not be null or empty.", configOption.key()));
            }
        }
    }

    public static void validateRequiredStringConfigOption(
            ReadableConfig config, ConfigOption<String> configOption) {
        if (!config.getOptional(configOption).isPresent()) {
            throw new IllegalArgumentException(
                    String.format("Could not find required option: %s", configOption.key()));
        }
        if (YaStringUtils.isEmpty(config.get(configOption)) || config.get(configOption).trim().length() == 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Required option %s should not be null or empty.", configOption.key()));
        }
    }

    public static void validateIntegerConfigOption(
            ReadableConfig config, ConfigOption<Integer> configOption) {
        if (config.getOptional(configOption).isPresent()) {

            if (config.get(configOption) <= 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Option %s should not be bigger than 0", configOption.key()));
            }
        }
    }

    public static void validateIntegerEnumConfigOption(
            ReadableConfig config, ConfigOption<Integer> configOption, List<Integer> enumList) {
        if (config.getOptional(configOption).isPresent()) {
            Integer confValue = config.get(configOption);
            if (!enumList.contains(confValue)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Option %s should be one of %s, not %s", configOption.key(), enumList, config.get(configOption)));
            }
        }
    }

    public static void validateStringEnumConfigOption(
            ReadableConfig config, ConfigOption<String> configOption, List<String> enumList) {
        if (config.getOptional(configOption).isPresent()) {
            String confValue = config.get(configOption);
            if (!enumList.contains(confValue)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Option %s should be one of %s, not %s", configOption.key(), enumList, config.get(configOption)));
            }
        }
    }

    public static void validateRequiredConfigOptions(ReadableConfig config) {
        validateRequiredStringConfigOption(config, URL);
        validateRequiredStringConfigOption(config, TABLE_NAME);
        validateRequiredStringConfigOption(config, USERNAME);
        validateRequiredStringConfigOption(config, PASSWORD);
    }

    public static void validateSink(ReadableConfig config, TableSchema tableSchema) {
        int fieldNum = tableSchema.getFieldCount();
        if (fieldNum <= 0) {
            throw new RuntimeException("invalid fieldNum, get " + fieldNum);
        }
        validateRequiredConfigOptions(config);
        validateIntegerConfigOption(config, RETRY_WAIT_TIME);
        validateIntegerConfigOption(config, BATCH_SIZE);
        validateIntegerConfigOption(config, BATCH_WRITE_TIMEOUT_MS);
        validateIntegerConfigOption(config, MAX_RETRY_TIMES);
        validateIntegerConfigOption(config, CONNECTION_MAX_ACTIVE);
        validateStringEnumConfigOption(config, CONFLICT_MODE, ConflictMode.stringList());
        validateIntegerEnumConfigOption(config, USE_COPY, ZeroOrOneEnum.integerList());
        validateStringConfigOption(config, TARGET_SCHEMA);
        validateStringEnumConfigOption(config, EXCEPTION_MODE, ExceptionMode.stringList());
        validateIntegerEnumConfigOption(config, RESERVEMS, ZeroOrOneEnum.integerList());
        validateIntegerEnumConfigOption(config, CASE_SENSITIVE, ZeroOrOneEnum.integerList());
        validateIntegerEnumConfigOption(config, WRITE_MODE, WriteMode.integerList());
        validateIntegerEnumConfigOption(config, VERBOSE, ZeroOrOneEnum.integerList());
    }

    public static void validateSource(ReadableConfig config, TableSchema tableSchema) {
        validateSink(config, tableSchema);
        validateIntegerConfigOption(config, JOINMAXROWS);
        validateIntegerConfigOption(config, CACHESIZE);
        validateIntegerConfigOption(config, CACHETTLMS);
        validateStringEnumConfigOption(config, CACHE, CacheMode.stringList());
    }

    public static JdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
        final Optional<String> partitionColumnName =
                readableConfig.getOptional(SCAN_PARTITION_COLUMN);
        final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
        if (partitionColumnName.isPresent()) {
            builder.setPartitionColumnName(partitionColumnName.get());
            builder.setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND));
            builder.setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND));
            builder.setNumPartitions(readableConfig.get(SCAN_PARTITION_NUM));
        }
        builder.setAutoCommit(readableConfig.get(SCAN_AUTO_COMMIT));
        return builder.build();
    }
}

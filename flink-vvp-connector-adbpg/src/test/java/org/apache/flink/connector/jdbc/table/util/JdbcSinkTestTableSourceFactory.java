/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/** A mock table source for testing jdbc sinks. */
public class JdbcSinkTestTableSourceFactory implements DynamicTableSourceFactory {

    private static final int DECIMAL_PRECISION = 10;
    private static final int DECIMAL_SCALE = 2;

    public static final ConfigOption<Integer> NUM_INSERT =
            ConfigOptions.key("numInsert".toLowerCase()).intType().defaultValue(1000);
    public static final ConfigOption<Integer> NUM_DELETE =
            ConfigOptions.key("numDelete".toLowerCase()).intType().defaultValue(250);
    public static final ConfigOption<Integer> NUM_UPDATE =
            ConfigOptions.key("numUpdate".toLowerCase()).intType().defaultValue(250);

    public static final ConfigOption<Integer> INSERT_START_KEY =
            ConfigOptions.key("insertStartKey".toLowerCase()).intType().defaultValue(0);
    public static final ConfigOption<Integer> INSERT_START_VALUE =
            ConfigOptions.key("insertStartValue".toLowerCase()).intType().defaultValue(0);
    public static final ConfigOption<Integer> DELETE_START_KEY =
            ConfigOptions.key("deleteStartKey".toLowerCase()).intType().defaultValue(0);
    public static final ConfigOption<Integer> DELETE_START_VALUE =
            ConfigOptions.key("deleteStartValue".toLowerCase()).intType().defaultValue(0);
    public static final ConfigOption<Integer> UPDATE_START_KEY =
            ConfigOptions.key("updateStartKey".toLowerCase()).intType().defaultValue(250);
    public static final ConfigOption<Integer> UPDATE_START_VALUE =
            ConfigOptions.key("updateStartValue".toLowerCase()).intType().defaultValue(1000);

    public static final ConfigOption<String> FIELD_TYPES =
            ConfigOptions.key("fieldTypes".toLowerCase()).stringType().defaultValue("INT,STRING");
    public static final ConfigOption<Boolean> MARK_EVEN_AS_NULL =
            ConfigOptions.key("markEvenAsNull".toLowerCase()).booleanType().defaultValue(false);

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TestTableSourceConfig testTableSourceConfig =
                createConfiguration(context.getCatalogTable().getOptions());
        return new JdbcSinkTestTableSource(testTableSourceConfig);
    }

    @Override
    public String factoryIdentifier() {
        return "jdbc_sink_test";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        NUM_INSERT,
                        NUM_DELETE,
                        NUM_UPDATE,
                        INSERT_START_KEY,
                        INSERT_START_VALUE,
                        DELETE_START_KEY,
                        DELETE_START_VALUE,
                        UPDATE_START_KEY,
                        UPDATE_START_VALUE,
                        FIELD_TYPES));
    }

    // -------------- Utility test classes -------------------------------

    private static class TestTableSourceConfig implements Serializable {
        int numInsert;
        int numDelete;
        int numUpdateAfter;

        int insertStartKeyIndex;
        int insertStartValue;
        int deleteStartKeyIndex;
        int deleteStartValue;
        int updateStartKeyIndex;
        int updateStartValue;

        List<LogicalTypeRoot> fieldTypes;
        boolean markEvenAsNull;
    }

    private static class JdbcSinkTestTableSource implements ScanTableSource {

        private final TestTableSourceConfig tableSourceConfig;

        JdbcSinkTestTableSource(TestTableSourceConfig tableSourceConfig) {
            this.tableSourceConfig = tableSourceConfig;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();

            if (tableSourceConfig.numInsert > 0) {
                builder.addContainedKind(RowKind.INSERT);
            }

            if (tableSourceConfig.numDelete > 0) {
                builder.addContainedKind(RowKind.DELETE);
            }

            if (tableSourceConfig.numUpdateAfter > 0) {
                builder.addContainedKind(RowKind.UPDATE_AFTER);
            }

            return builder.build();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
            return SourceFunctionProvider.of(new SourceFunction(tableSourceConfig), true);
        }

        @Override
        public DynamicTableSource copy() {
            return new JdbcSinkTestTableSource(tableSourceConfig);
        }

        @Override
        public String asSummaryString() {
            return "jdbc_sink_test_source";
        }
    }

    private static class SourceFunction extends RichParallelSourceFunction<RowData> {

        private final TestTableSourceConfig tableSourceConfig;

        private transient List<RowData> allData;

        SourceFunction(TestTableSourceConfig tableSourceConfig) {
            this.tableSourceConfig = tableSourceConfig;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            int remainInsert = tableSourceConfig.numInsert;
            int remainDelete = tableSourceConfig.numDelete;
            int remainUpdate = tableSourceConfig.numUpdateAfter;

            int nextInsertIndex = tableSourceConfig.insertStartKeyIndex;
            int nextInsertValue = tableSourceConfig.insertStartValue;
            int nextDeleteIndex = tableSourceConfig.deleteStartKeyIndex;
            int nextDeleteValue = tableSourceConfig.deleteStartValue;
            int nextUpdateIndex = tableSourceConfig.updateStartKeyIndex;
            int nextUpdateValue = tableSourceConfig.updateStartValue;

            Random random = new Random(1000);

            allData = new ArrayList<>();

            while (remainInsert + remainDelete + remainUpdate > 0) {
                if (remainInsert > 0) {
                    allData.add(
                            generateRowData(
                                    RowKind.INSERT,
                                    nextInsertIndex++,
                                    nextInsertValue++,
                                    tableSourceConfig.fieldTypes,
                                    tableSourceConfig.markEvenAsNull));
                    remainInsert--;
                }

                int nextIndex = random.nextInt(remainInsert + remainDelete + remainUpdate);
                if (nextIndex < remainInsert) {
                    allData.add(
                            generateRowData(
                                    RowKind.INSERT,
                                    nextInsertIndex++,
                                    nextInsertValue++,
                                    tableSourceConfig.fieldTypes,
                                    tableSourceConfig.markEvenAsNull));
                    remainInsert--;
                } else if (nextIndex < remainInsert + remainDelete) {
                    allData.add(
                            generateRowData(
                                    RowKind.DELETE,
                                    nextDeleteIndex++,
                                    nextDeleteValue++,
                                    tableSourceConfig.fieldTypes,
                                    tableSourceConfig.markEvenAsNull));
                    remainDelete--;
                } else {
                    allData.add(
                            generateRowData(
                                    RowKind.UPDATE_AFTER,
                                    nextUpdateIndex++,
                                    nextUpdateValue++,
                                    tableSourceConfig.fieldTypes,
                                    tableSourceConfig.markEvenAsNull));
                    remainUpdate--;
                }
            }
        }

        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            for (RowData rowData : allData) {
                sourceContext.collect(rowData);
            }
        }

        @Override
        public void cancel() {}
    }

    private static RowData generateRowData(
            RowKind rowKind,
            int index,
            int value,
            List<LogicalTypeRoot> fieldTypes,
            boolean markEvenAsNull) {

        GenericRowData rowData = new GenericRowData(fieldTypes.size() + 2);
        rowData.setRowKind(rowKind);
        rowData.setField(0, index);
        rowData.setField(1, value);

        for (int i = 0; i < fieldTypes.size(); ++i) {
            if (markEvenAsNull && i % 2 == 0 && value % 2 == 0) {
                rowData.setField(i + 2, null);
                continue;
            }

            switch (fieldTypes.get(i)) {
                case CHAR:
                case VARCHAR:
                    rowData.setField(i + 2, StringData.fromString("var_" + value));
                    break;
                case BOOLEAN:
                    rowData.setField(i + 2, value != 0);
                    break;
                case TINYINT:
                    rowData.setField(i + 2, (byte) value);
                    break;
                case INTEGER:
                    rowData.setField(i + 2, value);
                    break;
                case FLOAT:
                    rowData.setField(i + 2, (float) value);
                    break;
                case DOUBLE:
                    rowData.setField(i + 2, (double) value);
                    break;
                case DECIMAL:
                    rowData.setField(
                            i + 2,
                            DecimalData.fromBigDecimal(
                                    new BigDecimal(value), DECIMAL_PRECISION, DECIMAL_SCALE));
                    break;
                case DATE:
                    rowData.setField(
                            i + 2, value);
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    rowData.setField(
                            i + 2, TimestampData.fromTimestamp(new Timestamp(value * 1_000_000L)));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "not supported type: " + fieldTypes.get(i));
            }
        }

        return rowData;
    }

    private static TestTableSourceConfig createConfiguration(Map<String, String> options) {

        TestTableSourceConfig testTableSourceConfig = new TestTableSourceConfig();
        testTableSourceConfig.numInsert = Integer.valueOf(options.get(NUM_INSERT.key()));
        testTableSourceConfig.numDelete = Integer.valueOf(options.get(NUM_DELETE.key()));
        testTableSourceConfig.numUpdateAfter = Integer.valueOf(options.get(NUM_UPDATE.key()));
        testTableSourceConfig.insertStartKeyIndex = Integer.valueOf(options.get(INSERT_START_KEY.key()));
        testTableSourceConfig.insertStartValue = Integer.valueOf(options.get(INSERT_START_VALUE.key()));
        testTableSourceConfig.deleteStartKeyIndex = Integer.valueOf(options.get(DELETE_START_KEY.key()));
        testTableSourceConfig.deleteStartValue = Integer.valueOf(options.get(DELETE_START_VALUE.key()));
        testTableSourceConfig.updateStartKeyIndex = Integer.valueOf(options.get(UPDATE_START_KEY.key()));
        testTableSourceConfig.updateStartValue = Integer.valueOf(options.get(UPDATE_START_VALUE.key()));
        testTableSourceConfig.fieldTypes =
                Arrays.stream(options.get(FIELD_TYPES.key()).split(","))
                        .map(LogicalTypeRoot::valueOf)
                        .collect(Collectors.toList());
        testTableSourceConfig.markEvenAsNull = Boolean.valueOf(options.get(MARK_EVEN_AS_NULL.key()));

        return testTableSourceConfig;
    }
}

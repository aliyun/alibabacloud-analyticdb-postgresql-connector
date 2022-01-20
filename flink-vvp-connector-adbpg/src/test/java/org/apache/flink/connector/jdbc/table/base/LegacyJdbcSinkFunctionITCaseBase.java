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

package org.apache.flink.connector.jdbc.table.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.table.util.JdbcSinkTestTableSourceFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Base ITCase for Jdbc sink. */
public abstract class LegacyJdbcSinkFunctionITCaseBase {

    private final List<String> fieldTypes;

    private final boolean mockNullFieldValues;

    private final String sinkTableName;

    private final Map<String, String> keyedSinkOptions;

    private final Map<String, String> keyedCaseSensitveSinkOptions;

    private final Map<String, String> nonKeyedSinkOptions;

    public LegacyJdbcSinkFunctionITCaseBase(
            List<String> fieldTypes,
            boolean mockNullFieldValues,
            String sinkTableName,
            Map<String, String> sinkOptions) {
        this.fieldTypes = fieldTypes;
        this.mockNullFieldValues = mockNullFieldValues;
        this.sinkTableName = sinkTableName;
        this.keyedSinkOptions = sinkOptions;
        this.nonKeyedSinkOptions = sinkOptions;
        this.keyedCaseSensitveSinkOptions = sinkOptions;
    }

    public LegacyJdbcSinkFunctionITCaseBase(
            List<String> fieldTypes,
            boolean mockNullFieldValues,
            String sinkTableName,
            Map<String, String> keyedSinkOptions,
            Map<String, String> keyedCaseSensitveSinkOptions,
            Map<String, String> nonKeyedSinkOptions) {
        this.fieldTypes = fieldTypes;
        this.mockNullFieldValues = mockNullFieldValues;
        this.sinkTableName = sinkTableName;
        this.keyedSinkOptions = keyedSinkOptions;
        this.keyedCaseSensitveSinkOptions = keyedCaseSensitveSinkOptions;
        this.nonKeyedSinkOptions = nonKeyedSinkOptions;
    }

    protected abstract void initSinkTable() throws Exception;

    protected abstract ResultSet querySinkTableResult() throws SQLException;

    protected abstract void cleanup() throws Exception;

    protected String getSinkTableName() {
        return sinkTableName;
    }

    protected String getCreateTableSql(String tableName) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + "id INT NOT NULL DEFAULT 0,"
                + "_value INT NOT NULL DEFAULT 0,"
                + "f0 INT,"
                + "f1 VARCHAR(30) NOT NULL,"
                + "f2 DECIMAL ,"
                + "f3 DOUBLE PRECISION,"
                + "f4 DATE,"
                + "f5 REAL,"
                + "PRIMARY KEY (id))";
    }

    @Before
    public void before() throws Exception {
        initSinkTable();
    }

    @After
    public void after() throws Exception {
        cleanup();
    }

    protected Map<String, String> createSourceOptionsForKeyed() {
        return createSourceOptions(200, 50, 50, 0, 0, 0, 0, 50, 200);
    }

    protected Map<String, String> createSourceOptionsForNonKeyed() {
        return createSourceOptions(200, 50, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testKeyed() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(100);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        Map<String, String> sourceOptions = createSourceOptionsForKeyed();

        String sourceTableSql =
                String.format(
                        "CREATE TABLE source ("
                                + " id INT, "
                                + " _value INT, "
                                + " %s,"
                                + " primary key(id) not enforced"
                                + ") WITH ("
                                + "%s"
                                + ")",
                        createFieldLists(fieldTypes), createOptions(sourceOptions));
        bsTableEnv.executeSql(sourceTableSql).print();

        String sinkTableSql =
                String.format(
                        "CREATE TABLE %s ("
                                + " id INT, "
                                + " _value INT, "
                                + " %s,"
                                + " primary key(id) not enforced"
                                + ") WITH ("
                                + "%s"
                                + ")",
                        sinkTableName,
                        createFieldLists(fieldTypes),
                        createOptions(keyedSinkOptions));
        bsTableEnv.executeSql(sinkTableSql).print();

        bsTableEnv
                .executeSql(String.format("insert into %s select * from source", sinkTableName))
                .print();

        List<Row> data = new ArrayList<>();
        bsTableEnv.sqlQuery("select * from source").execute().collect().forEachRemaining(data::add);
        compareResultStr(data, querySinkTableResult(), true);
    }

    @Test
    public void testNonKeyed() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        Map<String, String> sourceOptions = createSourceOptionsForNonKeyed();

        String sourceTableSql =
                String.format(
                        "CREATE TABLE source ("
                                + " id INT, "
                                + " _value INT, "
                                + " %s"
                                + ") WITH ("
                                + "%s"
                                + ")",
                        createFieldLists(fieldTypes), createOptions(sourceOptions));
        bsTableEnv.executeSql(sourceTableSql).print();

        String sinkTableSql =
                String.format(
                        "CREATE TABLE %s ("
                                + " id INT, "
                                + " _value INT, "
                                + " %s"
                                + ") WITH ("
                                + "%s"
                                + ")",
                        sinkTableName,
                        createFieldLists(fieldTypes),
                        createOptions(nonKeyedSinkOptions));
        bsTableEnv.executeSql(sinkTableSql).print();

        bsTableEnv
                .executeSql(String.format("insert into %s select * from source", sinkTableName))
                .print();

        List<Row> data = new ArrayList<>();
        bsTableEnv.sqlQuery("select * from source").execute().collect().forEachRemaining(data::add);
        compareResultStr(data, querySinkTableResult(), false);
    }

    @Test
    public void testKeyedCaseSensitive() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        Map<String, String> sourceOptions = createSourceOptionsForKeyed();

        String sourceTableSql =
                String.format(
                        "CREATE TABLE source ("
                                + " id INT, "
                                + " _value INT, "
                                + " %s,"
                                + " primary key(id) not enforced"
                                + ") WITH ("
                                + "%s"
                                + ")",
                        createFieldLists(fieldTypes), createOptions(sourceOptions));
        bsTableEnv.executeSql(sourceTableSql).print();

        String sinkTableSql =
                String.format(
                        "CREATE TABLE %s ("
                                + " id INT, "
                                + " _value INT, "
                                + " %s,"
                                + " primary key(id) not enforced"
                                + ") WITH ("
                                + "%s"
                                + ")",
                        sinkTableName,
                        createFieldLists(fieldTypes),
                        createOptions(keyedCaseSensitveSinkOptions));
        bsTableEnv.executeSql(sinkTableSql).print();

        bsTableEnv
                .executeSql(String.format("insert into %s select * from source", sinkTableName))
                .print();

        List<Row> data = new ArrayList<>();
        bsTableEnv.sqlQuery("select * from source").execute().collect().forEachRemaining(data::add);
        compareResultStr(data, querySinkTableResult(), true);
    }

    private void compareResult(List<Row> data, ResultSet sinkValues, boolean hasKey)
            throws SQLException {
        List<Row> expectedResult = computeExpectedResultOrderByIndex(data, hasKey);

        List<Row> result = new ArrayList<>();
        try {
            int fieldCount = sinkValues.getMetaData().getColumnCount();
            while (sinkValues.next()) {
                Object[] array = new Object[fieldCount];
                for (int i = 0; i < fieldCount; ++i) {
                    array[i] = sinkValues.getObject(i + 1);
                }

                result.add(Row.ofKind(RowKind.INSERT, array));
            }

            assertEquals(expectedResult, result);
        } finally {
            sinkValues.close();
        }
    }

    private void compareResultStr(List<Row> data, ResultSet sinkValues, boolean hasKey)
            throws SQLException {
        List<Row> expectedResult = computeExpectedResultOrderByIndex(data, hasKey);

        List<Row> result = new ArrayList<>();
        try {
            int fieldCount = sinkValues.getMetaData().getColumnCount();
            while (sinkValues.next()) {
                Object[] array = new Object[fieldCount];
                for (int i = 0; i < fieldCount; ++i) {
                    array[i] = sinkValues.getObject(i + 1);
                }

                result.add(Row.ofKind(RowKind.INSERT, array));
            }
            List<String> result1 = expectedResult.stream()
                            .map(Row::toString)
                            .sorted()
                            .collect(Collectors.toList());
            List<String> result2 = result.stream()
                    .map(Row::toString)
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(result1, result2);
        } finally {
            sinkValues.close();
        }
    }

    protected Map<String, String> createSourceOptions(
            int numInsert,
            int numDelete,
            int numUpdate,
            int insertStartIndex,
            int insertStartValue,
            int deleteStartIndex,
            int deleteStartValue,
            int updateStartIndex,
            int updateStartValue) {
        Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put("connector", "jdbc_sink_test");
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.FIELD_TYPES.key(),
                fieldTypes.stream()
                        .map(
                            type ->
                                    type.contains("(")
                                            ? type.substring(0, type.indexOf('('))
                                            : type)
                        .collect(Collectors.joining(",")));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.MARK_EVEN_AS_NULL.key(),
                Boolean.toString(mockNullFieldValues));

        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.NUM_INSERT.key(), Integer.toString(numInsert));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.NUM_DELETE.key(), Integer.toString(numDelete));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.NUM_UPDATE.key(), Integer.toString(numUpdate));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.INSERT_START_KEY.key(),
                Integer.toString(insertStartIndex));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.INSERT_START_VALUE.key(),
                Integer.toString(insertStartValue));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.DELETE_START_KEY.key(),
                Integer.toString(deleteStartIndex));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.DELETE_START_VALUE.key(),
                Integer.toString(deleteStartValue));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.UPDATE_START_KEY.key(),
                Integer.toString(updateStartIndex));
        sourceOptions.put(
                JdbcSinkTestTableSourceFactory.UPDATE_START_VALUE.key(),
                Integer.toString(updateStartValue));

        return sourceOptions;
    }

    private String createFieldLists(List<String> fieldTypes) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); ++i) {
            fields.add(String.format("f%d %s", i, fieldTypes.get(i)));
        }

        return StringUtils.join(fields, ",\n");
    }

    private String createOptions(Map<String, String> options) {
        List<String> optionValues = new ArrayList<>();
        options.forEach((k, v) -> optionValues.add(String.format("'%s' = '%s'", k, v)));
        return StringUtils.join(optionValues, ",\n");
    }

    private List<Row> computeExpectedResultOrderByIndex(List<Row> data, boolean hasKey) {
        if (hasKey) {
            Map<Integer, Row> idToValue = new HashMap<>();
            for (Row row : data) {
                int id = (int) row.getField(0);
                switch (row.getKind()) {
                    case INSERT:
                    case UPDATE_AFTER:
                        idToValue.put(id, row);
                        break;
                    case DELETE:
                        idToValue.remove(id);
                        break;
                    case UPDATE_BEFORE:
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Not supported kind: " + row.getKind());
                }
            }

            return idToValue.values().stream()
                    .peek(row -> row.setKind(RowKind.INSERT))
                    .sorted(Comparator.comparingInt(row -> (int) row.getField(0)))
                    .collect(Collectors.toList());
        } else {
            Map<Tuple2<Integer, Integer>, Row> idAndValueToRow = new HashMap<>();
            Map<Tuple2<Integer, Integer>, Integer> idAndValueCount = new HashMap<>();

            for (Row row : data) {
                int id = (int) row.getField(0);
                int value = (int) row.getField(1);
                Tuple2<Integer, Integer> key = new Tuple2<>(id, value);

                switch (row.getKind()) {
                    case INSERT:
                        idAndValueToRow.put(key, row);
                        idAndValueCount.compute(key, (k, v) -> v == null ? 1 : v + 1);
                        break;
                    case DELETE:
                        idAndValueToRow.remove(key);
                        idAndValueCount.remove(key);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Not supported kind: " + row.getKind());
                }
            }

            List<Row> result = new ArrayList<>();
            idAndValueCount.forEach(
                (k, v) -> {
                    Row row = idAndValueToRow.get(k);
                    for (int i = 0; i < v; ++i) {
                        result.add(row);
                    }
                });

            return result.stream()
                    .sorted(Comparator.comparingInt(row -> (int) row.getField(0)))
                    .collect(Collectors.toList());
        }
    }
}

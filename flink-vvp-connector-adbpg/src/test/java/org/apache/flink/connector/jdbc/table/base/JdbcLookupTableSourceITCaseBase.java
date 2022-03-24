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

package org.apache.flink.connector.jdbc.table.base;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.table.util.JdbcTestUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/** ITCase for  VervericaJdbcDynamicTableSource  */
public abstract class JdbcLookupTableSourceITCaseBase {

    protected final String lookupTable;
    protected final String lookupTableCaseSensitive;

    protected final String url;
    protected final String userName;
    protected final String password;
    protected final String driverClass;
    protected final String connector;
    protected final String cacheStrategy;

    public JdbcLookupTableSourceITCaseBase(
            String connector,
            String url,
            String userName,
            String password,
            String driverClass,
            String cacheStrategy) {
        this.connector = connector;
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.driverClass = driverClass;
        this.cacheStrategy = cacheStrategy;

        String postfix = RandomStringUtils.randomAlphabetic(16);
        this.lookupTable = "lookupTable_" + postfix;
        this.lookupTableCaseSensitive = "\"lookupTable_" + postfix + "\"";
    }

    @Before
    public void before() throws Exception {
        clearOutputTable();

        Class.forName(driverClass);
        try (Connection conn = DriverManager.getConnection(url, userName, password);
                Statement statement = conn.createStatement()) {
            String[] fieldNames = {"id1", "id2", "comment1", "comment2"};
            statement.executeUpdate(getCreateTableSql(lookupTable));

            Object[][] data =
                    new Object[][] {
                        new Object[] {1, "1", "11-c1-v1", "11-c2-v1"},
                        new Object[] {1, "1", "11-c1-v2", "11-c2-v2"},
                        new Object[] {2, "3", "23-c1", "23-c2"},
                        new Object[] {2, "5", "25-c1", "25-c2"},
                        new Object[] {3, "8", "38-c1", "38-c2"}
                    };

            PreparedStatement stat =
                    conn.prepareStatement(
                            "INSERT INTO "
                                    + lookupTable
                                    + "(id1, id2, comment1, comment2) VALUES (?, ?, ?, ?)");

            for (int i = 0; i < data.length; ++i) {
                for (int j = 0; j < data[i].length; ++j) {
                    stat.setString(j + 1, data[i][j].toString());
                }
                stat.addBatch();
            }
            stat.executeBatch();

            String[] expectRows =
                    Arrays.stream(data)
                            .map(row -> {
                                return Arrays.stream(row)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(","));
                            })
                            .toArray(String[]::new);
            JdbcTestUtil.checkResultWithTimeout(
                    expectRows, lookupTable, fieldNames, url, userName, password, 60000L);
        }
    }

    protected String getCreateTableSql(String tableName) {
        return "CREATE TABLE "
                + tableName
                + " ("
                + "id1 INT NOT NULL DEFAULT 0,"
                + "id2 VARCHAR(20) NOT NULL,"
                + "price NUMERIC(5,2) NOT NULL,"
                + "comment1 VARCHAR(1000),"
                + "comment2 VARCHAR(1000),"
                + "comment3 VARCHAR(1000) default null)";
    }

    @After
    public void clearOutputTable() throws Exception {
        Class.forName(driverClass);
        try (Connection conn = DriverManager.getConnection(url, userName, password);
                Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE " + lookupTable);
        } catch (SQLException e) {
            // do nothing
        }
    }

    @Test
    public void testLookupTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        EnvironmentSettings.Builder envBuilder =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envBuilder.build());

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(2, "3"),
                                        new Tuple2<>(2, "5"),
                                        new Tuple2<>(3, "5"),
                                        new Tuple2<>(3, "8"))),
                        $("id1"),
                        $("id2"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        // test lookup function
        tEnv.executeSql(
                "CREATE TABLE lookupTable ("
                        + "  id1 INT,"
                        + "  id2 VARCHAR,"
                        + "  price DECIMAL(5,2) ,"
                        + "  comment1 VARCHAR,"
                        + "  comment2 VARCHAR,"
                        + "  comment3 VARCHAR"
                        + ") WITH ("
                        + "  'connector' = '"
                        + connector
                        + "',"
                        + "  'url' = '"
                        + url
                        + "',"
                        + "  'username' = '"
                        + userName
                        + "',"
                        + "  'password' = '"
                        + password
                        + "',"
                        + "  'tablename' = '"
                        + lookupTable
                        + "',"
                        + "  'cache' = '"
                        + cacheStrategy
                        + "',"
                        + "  'cacheSize' = '10000"
                        + "',"
                        + "  'exceptionmode' = 'ignore'"
                        + ")");

        String sqlQuery =
                "SELECT source.id1, source.id2, L.price, L.comment1, L.comment2 FROM T AS source "
                        + "JOIN lookupTable for system_time as of source.proctime AS L "
                        + "ON source.id1 = L.id1 and source.id2 = L.id2";

        Iterator<Row> collected = tEnv.executeSql(sqlQuery).collect();

        List<String> result =
                Lists.newArrayList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<Row> expectedRows = new ArrayList<>();
        expectedRows.add(Row.of(1, "1", 500.21, "11-c1-v1", "11-c2-v1"));
        expectedRows.add(Row.of(1, "1", 500.21, "11-c1-v1", "11-c2-v1"));
        expectedRows.add(Row.of(1, "1", 500.22, "11-c1-v2", "11-c2-v2"));
        expectedRows.add(Row.of(1, "1", 500.22, "11-c1-v2", "11-c2-v2"));
        expectedRows.add(Row.of(2, "3", 500.23, "23-c1", "23-c2"));
        expectedRows.add(Row.of(2, "5", 500.24, "25-c1", "25-c2"));
        expectedRows.add(Row.of(3, "8", 500.25, "38-c1", "38-c2"));

        List<String> expected =
                expectedRows.stream().map(Row::toString).sorted().collect(Collectors.toList());

        assertEquals(expected, result);
    }

    @Test
    public void testLookupTableCaseSensitive() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        EnvironmentSettings.Builder envBuilder =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envBuilder.build());

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(2, "3"),
                                        new Tuple2<>(2, "5"),
                                        new Tuple2<>(3, "5"),
                                        new Tuple2<>(3, "8"))),
                        $("id1"),
                        $("id2"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        // test lookup function
        tEnv.executeSql(
                "CREATE TABLE lookupTable ("
                        + "  id1 INT,"
                        + "  id2 VARCHAR,"
                        + "  price DECIMAL(5,2) ,"
                        + "  comment1 VARCHAR,"
                        + "  comment2 VARCHAR,"
                        + "  comment3 VARCHAR"
                        + ") WITH ("
                        + "  'connector' = '"
                        + connector
                        + "',"
                        + "  'url' = '"
                        + url
                        + "',"
                        + "  'username' = '"
                        + userName
                        + "',"
                        + "  'password' = '"
                        + password
                        + "',"
                        + "  'tablename' = '"
                        + lookupTable
                        + "',"
                        + "  'cache' = '"
                        + cacheStrategy
                        + "',"
                        + "  'cacheSize' = '10000"
                        + "',"
                        + "  'exceptionmode' = 'ignore"
                        + "',"
                        + "  'casesensitive' = '1'"
                        + ")");

        String sqlQuery =
                "SELECT source.id1, source.id2, L.price, L.comment1, L.comment2 FROM T AS source "
                        + "JOIN lookupTable for system_time as of source.proctime AS L "
                        + "ON source.id1 = L.id1 and source.id2 = L.id2";

        Iterator<Row> collected = tEnv.executeSql(sqlQuery).collect();

        List<String> result =
                Lists.newArrayList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<Row> expectedRows = new ArrayList<>();
        expectedRows.add(Row.of(1, "1", 500.21, "11-c1-v1", "11-c2-v1"));
        expectedRows.add(Row.of(1, "1", 500.21, "11-c1-v1", "11-c2-v1"));
        expectedRows.add(Row.of(1, "1", 500.22, "11-c1-v2", "11-c2-v2"));
        expectedRows.add(Row.of(1, "1", 500.22, "11-c1-v2", "11-c2-v2"));
        expectedRows.add(Row.of(2, "3", 500.23, "23-c1", "23-c2"));
        expectedRows.add(Row.of(2, "5", 500.24, "25-c1", "25-c2"));
        expectedRows.add(Row.of(3, "8", 500.25, "38-c1", "38-c2"));

        List<String> expected =
                expectedRows.stream().map(Row::toString).sorted().collect(Collectors.toList());

        assertEquals(expected, result);
    }
}

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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.connector.jdbc.table.util.JdbcTestUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import static org.apache.flink.table.api.Expressions.$;

/** The ITCase for JdbcDynamicTableSink. */
public abstract class JdbcTableSinkITCaseBase {

    public final String outputTable1;
    public final String outputTable2;
    protected final String url;
    protected final String userName;
    protected final String password;
    protected final String driverClass;
    protected String connector;

    public JdbcTableSinkITCaseBase(
            String connector, String url, String userName, String password, String driverClass) {
        this.connector = connector;
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.driverClass = driverClass;

        String postfix = RandomStringUtils.randomAlphabetic(16);
        this.outputTable1 = "sinkTableForUpsert_" + postfix;
        this.outputTable2 = "sinkTableForAppend_" + postfix;
    }

    public static List<Tuple3<Integer, Long, String>> get3TupleData() {
        List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, 1L, "Comment#1"));
        data.add(new Tuple3<>(2, 2L, "Comment#2"));
        data.add(new Tuple3<>(3, 2L, "Comment#3"));
        data.add(new Tuple3<>(4, 3L, "Comment#4"));
        data.add(new Tuple3<>(5, 3L, "Comment#5"));
        data.add(new Tuple3<>(6, 3L, "Comment#6"));
        data.add(new Tuple3<>(7, 4L, "Comment#7"));
        data.add(new Tuple3<>(8, 4L, "Comment#8"));
        data.add(new Tuple3<>(9, 4L, "Comment#9"));
        data.add(new Tuple3<>(10, 4L, "Comment#10"));
        data.add(new Tuple3<>(11, 5L, "Comment#11"));
        data.add(new Tuple3<>(12, 5L, "Comment#12"));
        data.add(new Tuple3<>(13, 5L, "Comment#13"));
        data.add(new Tuple3<>(14, 5L, "Comment#14"));
        data.add(new Tuple3<>(15, 5L, "Comment#15"));

        return data;
    }

    @Before
    public void before() throws Exception {
        clearOutputTable();

        Class.forName(driverClass);
        try (Connection conn = DriverManager.getConnection(url, userName, password);
                Statement stat = conn.createStatement()) {
            stat.executeUpdate(getCreateTableSql(outputTable1, true));
            stat.executeUpdate(getCreateTableSql(outputTable2, false));
        }
    }

    protected String getCreateTableSql(String tableName, boolean hasPrimaryKey) {
        if (hasPrimaryKey) {
            return "CREATE TABLE "
                    + tableName
                    + " ("
                    + "id INT NOT NULL DEFAULT 0,"
                    + "num BIGINT NOT NULL DEFAULT 0,"
                    + "text VARCHAR(30) NOT NULL,"
                    + "PRIMARY KEY (num))";
        } else {
            return "CREATE TABLE "
                    + tableName
                    + " ("
                    + "id INT NOT NULL DEFAULT 0,"
                    + "num BIGINT NOT NULL DEFAULT 0,"
                    + "text VARCHAR(30) NOT NULL)";
        }
    }

    @After
    public void clearOutputTable() throws Exception {
        Class.forName(driverClass);
        try (Connection conn = DriverManager.getConnection(url, userName, password);
                Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE " + outputTable1);
            stat.execute("DROP TABLE " + outputTable2);
        } catch (SQLException e) {
            // do nothing
        }
    }

    @Test
    public void testUpsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(get3TupleData()), $("id"), $("num"), $("text"));

        tEnv.registerTable("T", t);
        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  num BIGINT,"
                        + "  text VARCHAR,"
                        + "  PRIMARY KEY (num) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='"
                        + connector
                        + "',"
                        + "  'url'='"
                        + url
                        + "',"
                        + "  'userName' = '"
                        + userName
                        + "',"
                        + "  'password' = '"
                        + password
                        + "',"
                        + "  'tableName'='"
                        + outputTable1
                        + "'"
                        + ")");

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO upsertSink SELECT id, num, text FROM T WHERE num IN (4, 5)");

        // wait to finish
        tableResult.getJobClient().get().getJobExecutionResult().get();

        String[] expectRows = new String[] {"10,4,Comment#10", "15,5,Comment#15"};

        // check result
        JdbcTestUtil.checkResultWithTimeout(
                expectRows,
                outputTable1,
                new String[] {"id", "num", "text"},
                url,
                userName,
                password,
                60000L);
    }

    @Test
    public void testAppend() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(get3TupleData()), $("id"), $("num"), $("text"));

        tEnv.registerTable("T", t);
        tEnv.executeSql(
                "CREATE TABLE appendSink ("
                        + "  id INT,"
                        + "  num BIGINT,"
                        + "  text VARCHAR"
                        + ") WITH ("
                        + "  'connector'='"
                        + connector
                        + "',"
                        + "  'url'='"
                        + url
                        + "',"
                        + "  'userName' = '"
                        + userName
                        + "',"
                        + "  'password' = '"
                        + password
                        + "',"
                        + "  'tableName'='"
                        + outputTable2
                        + "'"
                        + ")");

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO appendSink SELECT id, num, text FROM T WHERE num IN (4, 5)");

        // wait to finish
        tableResult.getJobClient().get().getJobExecutionResult().get();

        String[] expectRows =
                new String[] {
                    "7,4,Comment#7",
                    "8,4,Comment#8",
                    "9,4,Comment#9",
                    "10,4,Comment#10",
                    "11,5,Comment#11",
                    "12,5,Comment#12",
                    "13,5,Comment#13",
                    "14,5,Comment#14",
                    "15,5,Comment#15"
                };

        // check result
        JdbcTestUtil.checkResultWithTimeout(
                expectRows,
                outputTable2,
                new String[] {"id", "num", "text"},
                url,
                userName,
                password,
                60000L);
    }
}

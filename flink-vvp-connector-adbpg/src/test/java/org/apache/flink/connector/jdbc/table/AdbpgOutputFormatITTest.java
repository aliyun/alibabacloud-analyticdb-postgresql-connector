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

package org.apache.flink.connector.jdbc.table;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.jdbc.table.base.LegacyJdbcSinkFunctionITCaseBase;
import org.apache.flink.connector.jdbc.table.util.AdbpgTestConfParser;
import org.apache.flink.connector.jdbc.table.utils.AdbpgOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONNECTOR_TYPE;

/** Tests the adbpg sink. */
public class AdbpgOutputFormatITTest {

    private static final String TEST_TABLE_NAME = "dynamic_sink_"
            +  new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
            + "_"
            + RandomStringUtils.randomAlphabetic(4).toLowerCase();

    private Statement statement;

    protected void initSinkTable() throws Exception {
        Class.forName("org.postgresql.Driver");

        Connection connection =
                DriverManager.getConnection(
                        AdbpgTestConfParser.INSTANCE.getURL(),
                        AdbpgTestConfParser.INSTANCE.getUsername(),
                        AdbpgTestConfParser.INSTANCE.getPassword());
        statement = connection.createStatement();
        String createTableSql = "CREATE TABLE "
                        + TEST_TABLE_NAME
                        + " ( id INT, "
                        + " _value TEXT, "
                        + " _comment TEXT,"
                        + " PRIMARY KEY (id))";
        statement.executeUpdate(createTableSql);
    }

    @Before
    public void before() throws Exception {
        // 初始化数据库连接并创建表
        initSinkTable();
    }

    protected ResultSet querySinkTableResult() throws SQLException {
        return statement.executeQuery("select * from " + TEST_TABLE_NAME + " order by id asc");
    }

    @Test
    public void testAdbpgOutputFormat() throws Exception {
        // 测试不同封闭符
        String[] quotes = new String[]{"\"", "*", "|"};
        for (String quote : quotes) {
            Map<String, String> config = createBaseConfig();
            config.put(AdbpgOptions.COPY_QUOTE.key(), quote);
            testAdbpgOutputFormatWithOptions(config);
        }

        // 测试不同分隔符
        String[] delimiters = new String[]{"\t", "^", "|"};
        for (String delimiter : delimiters) {
            Map<String, String> config = createBaseConfig();
            config.put(AdbpgOptions.DELIMITER.key(), delimiter);
            testAdbpgOutputFormatWithOptions(config);
        }
    }

    private Map<String, String> createBaseConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("connector", CONNECTOR_TYPE);
        config.put(AdbpgOptions.URL.key(), AdbpgTestConfParser.INSTANCE.getURL());
        config.put(AdbpgOptions.USERNAME.key(), AdbpgTestConfParser.INSTANCE.getUsername());
        config.put(AdbpgOptions.PASSWORD.key(), AdbpgTestConfParser.INSTANCE.getPassword());
        config.put(AdbpgOptions.TABLE_NAME.key(), TEST_TABLE_NAME);
        config.put(AdbpgOptions.BATCH_SIZE.key(), "20");
        config.put(AdbpgOptions.WRITE_MODE.key(), "1");
        config.put(AdbpgOptions.EXCEPTION_MODE.key(), "strict");
        config.put(AdbpgOptions.CONFLICT_MODE.key(), "upsert"); // 默认写入方式 COPY ... DO on conflict DO update
        config.put(AdbpgOptions.DELIMITER.key(), "\t"); // 分隔符默认值，制表符
        config.put(AdbpgOptions.COPY_FORMAT.key(), "csv"); // COPY写入默认格式，csv
        config.put(AdbpgOptions.COPY_QUOTE.key(), "\""); // 封闭符默认值，引号
        return config;
    }

    private void testAdbpgOutputFormatWithOptions(Map<String, String> sinkOptions) throws Exception {
        // 清空 ADBPG侧 的目标表数据
        statement.execute("TRUNCATE " + TEST_TABLE_NAME);
        // 搭建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(100);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        String copyQuote = sinkOptions.get(AdbpgOptions.COPY_QUOTE.key());
        String delimiter = sinkOptions.get(AdbpgOptions.DELIMITER.key());
        // 生成自定义数据，包含特殊字符
        DataStream<Row> sourceData = env.fromElements(
                Row.of(0, "\u0000", "文本包含Postgres非法字符 \\u0000"), // 可以替换掉控制字符\x00为空字符串
                Row.of(1, "\u0001", "文本包含控制字符 \\u0001"), // 可以写入控制字符\x01
                Row.of(2, "\n", "文本包含换行符 \\n"),  // \n 实际为换行符
                Row.of(3, "\\", "文本包含转义符 反斜杠\\"),    // "\\" 表示一个\
                Row.of(4, "\"", "文本包含默认封闭符 双引号\""),     // 双引号需转义
                Row.of(5, "\t", "文本包含默认分隔符 tab\\t"),        // \t 是制表符
                Row.of(6, copyQuote, "文本包含封闭符 " + copyQuote),
                Row.of(7, delimiter, "文本包含分隔符 " + delimiter),
                Row.of(8, delimiter + copyQuote + copyQuote + delimiter, "文本包含分隔符+封闭符+封闭符+分隔符 " + delimiter + copyQuote + copyQuote + delimiter),
                Row.of(9, null, "文本包含NULL值"),
                Row.of(10, "", "文本包含空字符串")
        );

        // 注册为临时视图，覆盖原source表
        Table sourceTable = bsTableEnv.fromDataStream(sourceData)
                .as("id", "_value", "_comment");  // 字段名需与原表结构一致
        bsTableEnv.createTemporaryView("source", sourceTable);

        String sinkTableSql =
                String.format(
                        "CREATE TABLE %s ("
                                + " id INT, "
                                + " _value STRING, "
                                + " _comment STRING,"
                                + " primary key(id) not enforced"
                                + ") WITH ("
                                + "%s"
                                + ")",
                        TEST_TABLE_NAME,
                        LegacyJdbcSinkFunctionITCaseBase.createOptions(sinkOptions));
        bsTableEnv.executeSql(sinkTableSql).print();

        bsTableEnv
                .executeSql(String.format("insert into %s select * from source", TEST_TABLE_NAME))
                .print();

        List<Row> data = new ArrayList<>();
        bsTableEnv.sqlQuery("select * from source").execute().collect().forEachRemaining(data::add);

        // 预处理期望数据：将 _value 元素中 "\u0000" 替换为空字符串
        data.replaceAll(row -> {
            if (row.getField(1) != null && row.getField(1).equals("\u0000")) {
                // 修改第二个字段（_value）为空字符串""
                return Row.of(row.getField(0), "", row.getField(2));
            }
            return row;
        });

        LegacyJdbcSinkFunctionITCaseBase.compareResultStr(data, querySinkTableResult(), true);
    }

    protected void cleanup() throws SQLException {
        if (statement != null) {
            statement.execute("DROP TABLE " + TEST_TABLE_NAME);
            statement.close();
        }
    }

    @After
    public void after() throws Exception {
        cleanup();
    }
}

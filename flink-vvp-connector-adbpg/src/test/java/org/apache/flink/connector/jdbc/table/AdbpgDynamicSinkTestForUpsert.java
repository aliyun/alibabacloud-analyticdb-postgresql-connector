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

import org.apache.flink.connector.jdbc.table.base.LegacyJdbcSinkFunctionITCaseBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.connector.jdbc.table.util.AdbpgTestConfParser;
import org.apache.flink.connector.jdbc.table.utils.AdbpgOptions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONNECTOR_TYPE;

/** Tests the adbpg sink. */
@RunWith(Parameterized.class)
public class AdbpgDynamicSinkTestForUpsert extends LegacyJdbcSinkFunctionITCaseBase {

    private static final String TEST_TABLE_NAME =
            "dynamic_sink_" + RandomStringUtils.randomAlphabetic(16).toLowerCase();

    private Statement statement;

    public AdbpgDynamicSinkTestForUpsert(boolean mockNullFieldValues) {
        super(
                Arrays.asList("INTEGER", "VARCHAR", "DECIMAL(10,2)", "DOUBLE", "DATE", "FLOAT"),
                mockNullFieldValues,
                TEST_TABLE_NAME,
                new HashMap<String, String>() {
                    {
                        this.put("connector", CONNECTOR_TYPE);
                        this.put(AdbpgOptions.URL.key(), AdbpgTestConfParser.INSTANCE.getURL());
                        this.put(
                                AdbpgOptions.USERNAME.key(),
                                AdbpgTestConfParser.INSTANCE.getUsername());
                        this.put(
                                AdbpgOptions.PASSWORD.key(),
                                AdbpgTestConfParser.INSTANCE.getPassword());
                        this.put(AdbpgOptions.TABLE_NAME.key(), TEST_TABLE_NAME);
                        this.put(AdbpgOptions.BATCH_SIZE.key(), "20");
                        this.put(AdbpgOptions.WRITE_MODE.key(), "2");
                        this.put(AdbpgOptions.EXCEPTION_MODE.key(), "strict");
                    }
                },
                new HashMap<String, String>() {
                    {
                        this.put("connector", CONNECTOR_TYPE);
                        this.put(AdbpgOptions.URL.key(), AdbpgTestConfParser.INSTANCE.getURL());
                        this.put(
                                AdbpgOptions.USERNAME.key(),
                                AdbpgTestConfParser.INSTANCE.getUsername());
                        this.put(
                                AdbpgOptions.PASSWORD.key(),
                                AdbpgTestConfParser.INSTANCE.getPassword());
                        this.put(AdbpgOptions.TABLE_NAME.key(), TEST_TABLE_NAME);
                        this.put(AdbpgOptions.BATCH_SIZE.key(), "20");
                        this.put(AdbpgOptions.WRITE_MODE.key(), "2");
                        this.put(AdbpgOptions.CASE_SENSITIVE.key(), "1");
                        this.put(AdbpgOptions.EXCEPTION_MODE.key(), "strict");
                    }
                },
                new HashMap<String, String>() {
                    {
                        this.put("connector", CONNECTOR_TYPE);
                        this.put(AdbpgOptions.URL.key(), AdbpgTestConfParser.INSTANCE.getURL());
                        this.put(
                                AdbpgOptions.USERNAME.key(),
                                AdbpgTestConfParser.INSTANCE.getUsername());
                        this.put(
                                AdbpgOptions.PASSWORD.key(),
                                AdbpgTestConfParser.INSTANCE.getPassword());
                        this.put(AdbpgOptions.TABLE_NAME.key(), TEST_TABLE_NAME);
                        this.put(AdbpgOptions.WRITE_MODE.key(), "2");
                        this.put(AdbpgOptions.BATCH_SIZE.key(), "20");
                        this.put(AdbpgOptions.EXCEPTION_MODE.key(), "strict");
                    }
                });
    }

    @Parameterized.Parameters(name = "has null = {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Override
    protected void initSinkTable() throws Exception {
        Class.forName("org.postgresql.Driver");

        Connection connection =
                DriverManager.getConnection(
                        AdbpgTestConfParser.INSTANCE.getURL(),
                        AdbpgTestConfParser.INSTANCE.getUsername(),
                        AdbpgTestConfParser.INSTANCE.getPassword());
        statement = connection.createStatement();
        statement.executeUpdate(getCreateTableSql(TEST_TABLE_NAME));
    }

    @Override
    protected ResultSet querySinkTableResult() throws SQLException {
        return statement.executeQuery("select * from " + TEST_TABLE_NAME + " order by id asc");
    }

    @Override
    protected void cleanup() throws SQLException {
        if (statement != null) {
            statement.execute("DROP TABLE " + TEST_TABLE_NAME);
            statement.close();
        }
    }
}

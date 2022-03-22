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
import org.apache.flink.table.api.TableEnvironment;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/** Test configs. */
public class AdbpgScanITCaseBase {
    private static final String ADBPG_URL = "ADBPG_URL";
    private static final String ADBPG_USERNAME = "ADBPG_USERNAME";
    private static final String ADBPG_PASSWORD = "ADBPG_PASSWORD";


    public String url;
    public String database;
    public String username;
    public String password;

    public AdbpgScanITCaseBase() throws IOException {
        Properties properties = new Properties();


        url = System.getenv(ADBPG_URL);
        database = properties.getProperty("database");
        username = System.getenv(ADBPG_USERNAME);
        password = System.getenv(ADBPG_PASSWORD);
    }

    public void cleanTable(String tableName) {
        executeSql("DELETE FROM " + tableName, true);
    }

    public void createTable(String createSql) {
        executeSql(createSql, false);
    }

    public void dropTable(String tableName) {
        executeSql(String.format("DROP TABLE IF EXISTS \"%s\"", tableName), true);
    }

    public void executeSql(String sql, boolean ignoreException) {
        try (Connection connection =
                     DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            if (!ignoreException) {
                throw new RuntimeException("Failed to execute: " + sql, e);
            }
        }
    }
}

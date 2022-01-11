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

package org.apache.flink.connector.jdbc.table.util;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;

/** Config util. */
public class JdbcTestUtil {

    public static Properties getConfig(String config) {
        try {
            Properties properties = new Properties();
            properties.load(JdbcTestUtil.class.getClassLoader().getResourceAsStream((config)));
            return properties;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void checkResult(
            String[] expectedResult,
            String table,
            String[] fields,
            String url,
            String userName,
            String password)
            throws SQLException {

        try (Connection dbConn = DriverManager.getConnection(url, userName, password);
                PreparedStatement statement = dbConn.prepareStatement("select * from " + table);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                List<String> result = new ArrayList<>();
                for (int i = 0; i < fields.length; i++) {
                    result.add(resultSet.getObject(fields[i]).toString());
                }

                results.add(StringUtils.join(result, ","));
            }
            String[] sortedResult = results.toArray(new String[0]);
            Arrays.sort(expectedResult);
            Arrays.sort(sortedResult);
            assertArrayEquals(expectedResult, sortedResult);
        }
    }

    public static void checkResultWithTimeout(
            String[] expectedResult,
            String table,
            String[] fields,
            String url,
            String userName,
            String password,
            long timeout)
            throws SQLException, InterruptedException {

        long endTimeout = System.currentTimeMillis() + timeout;
        boolean result = false;
        while (System.currentTimeMillis() < endTimeout) {
            try {
                checkResult(expectedResult, table, fields, url, userName, password);
                result = true;
                break;
            } catch (AssertionError | SQLException throwable) {
                Thread.sleep(1000L);
            }
        }
        if (!result) {
            checkResult(expectedResult, table, fields, url, userName, password);
        }
    }
}

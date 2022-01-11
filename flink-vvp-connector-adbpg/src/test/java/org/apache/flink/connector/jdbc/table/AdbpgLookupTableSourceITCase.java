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

package org.apache.flink.connector.jdbc.table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.connector.jdbc.table.base.JdbcLookupTableSourceITCaseBase;
import org.apache.flink.connector.jdbc.table.util.AdbpgTestConfParser;
import org.apache.flink.connector.jdbc.table.util.JdbcTestUtil;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Itcase for dim join. */
@RunWith(Parameterized.class)
public class AdbpgLookupTableSourceITCase extends JdbcLookupTableSourceITCaseBase {

    public AdbpgLookupTableSourceITCase(
            String connector,
            String url,
            String userName,
            String password,
            String driverClass,
            String cacheStrategy) {
        super(connector, url, userName, password, driverClass, cacheStrategy);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        List<Object[]> paramList = new ArrayList<>();

        // Add adbpg dim join itcase
        String adbpgURL = AdbpgTestConfParser.INSTANCE.getURL();
        String adbpgUserName = AdbpgTestConfParser.INSTANCE.getUsername();
        String adbpgPassword = AdbpgTestConfParser.INSTANCE.getPassword();
        String adbpgDriverClass = "org.postgresql.Driver";

        paramList.add(
                new Object[] {
                    "adbpg-nightly", adbpgURL, adbpgUserName, adbpgPassword, adbpgDriverClass, "all"
                });
        paramList.add(
                new Object[] {
                    "adbpg-nightly", adbpgURL, adbpgUserName, adbpgPassword, adbpgDriverClass, "lru"
                });
        paramList.add(
                new Object[] {
                    "adbpg-nightly", adbpgURL, adbpgUserName, adbpgPassword, adbpgDriverClass, "none"
                });

        return paramList;
    }

    @Override
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

            Statement stat = conn.createStatement();
            stat.execute(
                    String.format(
                            "insert into %s (id1, id2, comment1, comment2) values "
                                    + "(1, '1', '11-c1-v1', '11-c2-v1'), "
                                    + "(1, '1', '11-c1-v2', '11-c2-v2'), "
                                    + "(2, '3', '23-c1', '23-c2'), "
                                    + "(2, '5', '25-c1', '25-c2'), "
                                    + "(3, '8', '38-c1', '38-c2')",
                            lookupTable));
            String[] expectRows =
                    Arrays.stream(data)
                            .map(
                                    row -> {
                                        return Arrays.stream(row)
                                                .map(Object::toString)
                                                .collect(Collectors.joining(","));
                                    })
                            .toArray(String[]::new);
            JdbcTestUtil.checkResultWithTimeout(
                    expectRows, lookupTable, fieldNames, url, userName, password, 60000L);
        }
    }
}

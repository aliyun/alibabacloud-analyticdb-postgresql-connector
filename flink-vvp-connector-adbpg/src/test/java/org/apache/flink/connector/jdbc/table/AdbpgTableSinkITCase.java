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

import org.apache.flink.connector.jdbc.table.base.JdbcTableSinkITCaseBase;
import org.apache.flink.connector.jdbc.table.util.AdbpgTestConfParser;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Sink itcase. */
@RunWith(Parameterized.class)
public class AdbpgTableSinkITCase extends JdbcTableSinkITCaseBase {

    public AdbpgTableSinkITCase(
            String connector, String url, String userName, String password, String driverClass) {
        super(connector, url, userName, password, driverClass);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        List<Object[]> paramList = new ArrayList<>();

        // Add adbpg sink test
        String adbpgURL = AdbpgTestConfParser.INSTANCE.getURL();
        String adbpgUserName = AdbpgTestConfParser.INSTANCE.getUsername();
        String adbpgPassword = AdbpgTestConfParser.INSTANCE.getPassword();
        paramList.add(
                new Object[] {
                    "adbpg-nightly-1.13", adbpgURL, adbpgUserName, adbpgPassword, "org.postgresql.Driver"
                });

        return paramList;
    }
}

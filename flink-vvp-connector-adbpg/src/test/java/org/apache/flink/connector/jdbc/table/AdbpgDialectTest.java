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

import org.apache.flink.connector.jdbc.table.utils.AdbpgDialect;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class AdbpgDialectTest {

    private AdbpgDialect adbpgDialect;
    private final String schema = "public";
    private final String table = "test_table";
    private final String[] fields = {"id", "name", "age"};
    private final String stdin = "STDIN";
    private final String delimiter = "\t";

    @Before
    public void setup() {
        // 初始化 dialect，caseSensitive 设为 true 以测试字段引号
        adbpgDialect = new AdbpgDialect(schema, true);
    }

    //------------------ 测试不同冲突模式 (conflictMode) -------------------
    @Test
    public void testCopyStatementWithUpsertConflict() {
        String conflictMode = "upsert";
        String sql = adbpgDialect.getCopyStatement(
                table, fields, stdin, conflictMode, delimiter, "csv", "\""
        );

        String expected = "COPY \"public\".\"test_table\"(\"id\", \"name\", \"age\") "
                + "FROM STDIN DELIMITER '\t' NULL 'null' csv ESCAPE '\\' quote '\"' DO on conflict DO update";
        assertEquals(expected, sql);
    }

    @Test
    public void testCopyStatementWithIgnoreConflict() {
        String conflictMode = "ignore";
        String sql = adbpgDialect.getCopyStatement(
                table, fields, stdin, conflictMode, delimiter, "text", "'"
        );

        String expected = "COPY \"public\".\"test_table\"(\"id\", \"name\", \"age\") "
                + "FROM STDIN DELIMITER '\t' NULL 'null'";
        assertEquals(expected, sql);
    }

    //------------------ 测试不同 COPY 格式 (csv vs text) -------------------
    @Test
    public void testCsvFormatWithQuote() {
        String sql = adbpgDialect.getCopyStatement(
                table, fields, stdin, "strict", delimiter, "csv", "'"
        );

        String expected = "COPY \"public\".\"test_table\"(\"id\", \"name\", \"age\") "
                + "FROM STDIN DELIMITER '\t' NULL 'null' csv ESCAPE '\\' quote '''";
        assertEquals(expected, sql);
    }

    @Test
    public void testTextFormatNoQuote() {
        String sql = adbpgDialect.getCopyStatement(
                table, fields, stdin, "update", delimiter, "text", "\""
        );

        String expected = "COPY \"public\".\"test_table\"(\"id\", \"name\", \"age\") "
                + "FROM STDIN DELIMITER '\t' NULL 'null'";
        assertEquals(expected, sql);
    }

    //------------------ 测试字段名大小写敏感性 -------------------
    @Test
    public void testCaseSensitiveFieldQuotes() {
        // caseSensitive=true 时字段应被双引号包裹
        String[] mixedCaseFields = {"UserID", "userName"};
        AdbpgDialect caseSensitiveDialect = new AdbpgDialect(schema, true);

        String sql = caseSensitiveDialect.getCopyStatement(
                table, mixedCaseFields, stdin, "strict", delimiter, "csv", "\""
        );

        String expectedFields = "\"UserID\", \"userName\"";
        String expected = "COPY \"public\".\"test_table\"(" + expectedFields + ") "
                + "FROM STDIN DELIMITER '\t' NULL 'null' csv ESCAPE '\\' quote '\"'";
        assertEquals(expected, sql);
    }

    @Test
    public void testCaseInsensitiveFieldQuotes() {
        // caseSensitive=false 时字段不加引号
        AdbpgDialect caseInsensitiveDialect = new AdbpgDialect(schema, false);

        String sql = caseInsensitiveDialect.getCopyStatement(
                table, fields, stdin, "upsert", delimiter, "text", "'"
        );

        String expectedFields = "id, name, age";
        String expected = "COPY public.test_table(" + expectedFields + ") "
                + "FROM STDIN DELIMITER '\t' NULL 'null' DO on conflict DO update";
        assertEquals(expected, sql);
    }

    //------------------ 测试特殊分隔符和表名处理 -------------------
    @Test
    public void testCustomDelimiterAndSchema() {
        String customDelimiter = "|";
        String customSchema = "my_schema";
        AdbpgDialect dialect = new AdbpgDialect(customSchema, true);

        String sql = dialect.getCopyStatement(
                "orders", fields, stdin, "upsert", customDelimiter, "csv", "`"
        );

        String expected = "COPY \"my_schema\".\"orders\"(\"id\", \"name\", \"age\") "
                + "FROM STDIN DELIMITER '|' NULL 'null' csv ESCAPE '\\' quote '`' DO on conflict DO update";
        assertEquals(expected, sql);
    }
}

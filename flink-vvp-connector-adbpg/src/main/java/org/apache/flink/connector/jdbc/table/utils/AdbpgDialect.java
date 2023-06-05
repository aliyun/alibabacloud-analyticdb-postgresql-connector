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

package org.apache.flink.connector.jdbc.table.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Adbpg dialect.
 */
public class AdbpgDialect implements Serializable {

    private final boolean caseSensitive;
    private final String targetSchema;

    public AdbpgDialect(String targetSchema, boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        this.targetSchema = targetSchema;
    }

    /**
     * Quotes the identifier. This is used to put quotes around the identifier in case the column
     * name is a reserved keyword, or in case it contains characters that require quotes (e.g.
     * space). Default using double quotes {@code "} to quote.
     */
    public String quoteIdentifier(String identifier) {
        if (caseSensitive) {
            return "\"" + identifier + "\"";
        } else {
            return identifier;
        }
    }

    /**
     * Get dialect upsert statement, the database has its own upsert syntax, such as Mysql using
     * DUPLICATE KEY UPDATE, and PostgresSQL using ON CONFLICT... DO UPDATE SET..
     *
     * @return None if dialect does not support upsert statement, the writer will degrade to the use
     * of select + update/insert, this performance is poor.
     */
    public String getUpsertStatement(
            String tableName,
            String[] fieldNames,
            String[] uniqueKeyFields,
            String[] UpdateFields, boolean support_upsert) {

        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateClause =
                Arrays.stream(UpdateFields)
                        .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        String conflictAction = " ON CONFLICT ("
                + uniqueColumns
                + ")"
                + " DO UPDATE SET "
                + updateClause;
        return getInsertIntoStatement(tableName, fieldNames)
                + (support_upsert ? conflictAction : "");
    }

    public String getCopyStatement(String tableName, String[] fieldNames, String file, String conflictMode,
                                   boolean support_upsert) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String conflictAction;
        if ("ignore".equalsIgnoreCase(conflictMode)     /** if conflictmode is not "upsert", use normal copy statement or insert statement */
                || "strict".equalsIgnoreCase(conflictMode)
                || "update".equalsIgnoreCase(conflictMode)) {
            conflictAction = "";
        } else {                                          /** if conflictmode is "upsert", use copy-on-conflict statement or insert-on-conflict statement */
            conflictAction = " DO on conflict DO update";
        }
        return "COPY "
                + quoteIdentifier(targetSchema)
                + "."
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " FROM "
                + file
                + " NULL 'null' "
                + (support_upsert ? conflictAction : "");
    }

    public String getDeleteStatementWithNull(
            String tableName, String[] conditionFields, Set<Integer> nullFieldIndices) {
        String[] conditions = new String[conditionFields.length];
        for (int i = 0; i < conditionFields.length; ++i) {
            if (nullFieldIndices.contains(i)) {
                conditions[i] = quoteIdentifier(conditionFields[i]) + " IS NULL";
            } else {
                conditions[i] = quoteIdentifier(conditionFields[i]) + "=?";
            }
        }
        String conditionClause = YaStringUtils.join(conditions, " AND ");
        return "DELETE FROM "
                + quoteIdentifier(targetSchema)
                + "."
                + quoteIdentifier(tableName)
                + " WHERE "
                + conditionClause;
    }

    /**
     * Get insert into statement.
     */
    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(targetSchema)
                + "."
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    /**
     * Get update statement by unique keys.
     */
    public String getUpdateStatement(
            String tableName, String[] uniqueKeyFields, String[] updateFields) {
        String setClause =
                Arrays.stream(updateFields)
                        .map(f -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(uniqueKeyFields)
                        .map(f -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        return "UPDATE "
                + quoteIdentifier(targetSchema)
                + "."
                + quoteIdentifier(tableName)
                + " SET "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    /**
     * Get delete one row statement by condition fields, default not use limit 1, because limit 1 is
     * a sql dialect.
     */
    public String getDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM "
                + quoteIdentifier(targetSchema)
                + "."
                + quoteIdentifier(tableName)
                + " WHERE "
                + conditionClause;
    }

    /**
     * Get select fields statement by condition fields. Default use SELECT.
     */
    public String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + quoteIdentifier(targetSchema)
                + "."
                + quoteIdentifier(tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    public String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }
}

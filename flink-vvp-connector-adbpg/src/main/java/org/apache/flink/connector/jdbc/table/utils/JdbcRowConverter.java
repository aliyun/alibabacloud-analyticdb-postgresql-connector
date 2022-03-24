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

package org.apache.flink.connector.jdbc.table.utils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Base class for all converters that convert between JDBC object and Flink internal object.
 */
public class JdbcRowConverter implements Serializable {

    protected final JdbcDeserializationConverter[] toInternalConverters;
    protected final JdbcSerializationConverter[] toExternalConverters;
    protected final LogicalType[] fieldLogicalTypes;

    public JdbcRowConverter(DataType[] fieldTypes) {
        this(
                Arrays.asList(fieldTypes).stream()
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new));
    }

    public JdbcRowConverter(LogicalType[] fieldLogicalTypes) {
        this.fieldLogicalTypes = fieldLogicalTypes;
        this.toInternalConverters = new JdbcDeserializationConverter[fieldLogicalTypes.length];
        this.toExternalConverters = new JdbcSerializationConverter[fieldLogicalTypes.length];
        for (int i = 0; i < fieldLogicalTypes.length; i++) {
            toInternalConverters[i] = createNullableInternalConverter(fieldLogicalTypes[i]);
            toExternalConverters[i] = createNullableExternalConverter(fieldLogicalTypes[i]);
        }
    }

    public LogicalType[] getFieldLogicalTypes() {
        return fieldLogicalTypes;
    }

    public RowData toInternal(ResultSet resultSet, String em) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(fieldLogicalTypes.length);

        for (int pos = 0; pos < fieldLogicalTypes.length; pos++) {
            try {
                Object field = resultSet.getObject(pos + 1);
                genericRowData.setField(pos, toInternalConverters[pos].deserialize(field));
            } catch (Exception e) {
                if ("strict".equalsIgnoreCase(em)) {
                    throw e;
                }
            }
        }
        return genericRowData;
    }

    public PreparedStatement toExternal(RowData rowData, PreparedStatement statement)
            throws SQLException {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, statement, index + 1);
        }
        return statement;
    }

    public PreparedStatement batchToExternal(List<RowData> rowDataList, PreparedStatement statement)
            throws SQLException {
        int sqlParameterIndex = 1;
        for (RowData rowData : rowDataList) {
            for (int index = 0; index < rowData.getArity(); index++) {
                toExternalConverters[index].serialize(rowData, index, statement, sqlParameterIndex);
                sqlParameterIndex++;
            }
        }
        return statement;
    }

    /**
     * Runtime converter to convert JDBC field to {@link RowData} type object.
     */
    @FunctionalInterface
    interface JdbcDeserializationConverter extends Serializable {
        /**
         * Convert a jdbc field object of {@link ResultSet} to the internal data structure object.
         *
         * @param jdbcField
         */
        Object deserialize(Object jdbcField) throws SQLException;
    }

    /**
     * Runtime converter to convert {@link RowData} field to java object and fill into the {@link
     * PreparedStatement}.
     */
    @FunctionalInterface
    interface JdbcSerializationConverter extends Serializable {
        void serialize(
                RowData rowData, int fieldIndex, PreparedStatement statement, int sqlParameterIndex)
                throws SQLException;
    }

    /**
     * Create a nullable runtime {@link JdbcDeserializationConverter} from given {@link
     * LogicalType}.
     */
    protected JdbcDeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createInternalConverter(type));
    }

    protected JdbcDeserializationConverter wrapIntoNullableInternalConverter(
            JdbcDeserializationConverter jdbcDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return jdbcDeserializationConverter.deserialize(val);
            }
        };
    }

    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case INTEGER:
                return val -> val;
            case BIGINT:
                return val -> val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> (int) (((Date) val).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * Create a nullable JDBC f{@link JdbcSerializationConverter} from given sql type.
     */
    protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
    }

    protected JdbcSerializationConverter wrapIntoNullableExternalConverter(
            JdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
        final int sqlType =
                JdbcTypeUtil.typeInformationToSqlType(
                        TypeConversions.fromDataTypeToLegacyInfo(
                                TypeConversions.fromLogicalToDataType(type)));
        return (val, fieldIndex, statement, sqlIndex) -> {
            if (val == null
                    || val.isNullAt(fieldIndex)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                statement.setNull(sqlIndex, sqlType);
            } else {
                jdbcSerializationConverter.serialize(val, fieldIndex, statement, sqlIndex);
            }
        };
    }

    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setBoolean(sqlIndex, val.getBoolean(fieldIndex));
            case TINYINT:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setByte(sqlIndex, val.getByte(fieldIndex));
            case SMALLINT:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setShort(sqlIndex, val.getShort(fieldIndex));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setInt(sqlIndex, val.getInt(fieldIndex));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setLong(sqlIndex, val.getLong(fieldIndex));
            case FLOAT:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setFloat(sqlIndex, val.getFloat(fieldIndex));
            case DOUBLE:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setDouble(sqlIndex, val.getDouble(fieldIndex));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setString(sqlIndex, val.getString(fieldIndex).toString());
            case BINARY:
            case VARBINARY:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setBytes(sqlIndex, val.getBinary(fieldIndex));
            case DATE:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setDate(
                                sqlIndex,
                                Date.valueOf(LocalDate.ofEpochDay(val.getInt(fieldIndex))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setTime(
                                sqlIndex,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(
                                                val.getInt(fieldIndex) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setTimestamp(
                                sqlIndex,
                                val.getTimestamp(fieldIndex, timestampPrecision).toTimestamp());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, fieldIndex, statement, sqlIndex) ->
                        statement.setBigDecimal(
                                sqlIndex,
                                val.getDecimal(fieldIndex, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}

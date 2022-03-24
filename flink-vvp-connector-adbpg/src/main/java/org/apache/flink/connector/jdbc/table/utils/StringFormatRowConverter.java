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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;

/** String format converter. */
public class StringFormatRowConverter implements Serializable {

    protected final StringFormatRowConverter.StringFormatConverter[] toStringConverters;
    protected final LogicalType[] fieldLogicalTypes;

    private String toCopyField(String fieldValue) {
        if (null == fieldValue) {
            return "null";
        }

        if (fieldValue.contains("\\")) {
            return fieldValue.replaceAll("\\\\", "\\\\\\\\");
        }
        return fieldValue;
    }

    public StringFormatRowConverter(
            LogicalType[] fieldLogicalTypes) {
        this.fieldLogicalTypes = fieldLogicalTypes;
        this.toStringConverters = new StringFormatConverter[fieldLogicalTypes.length];
        for (int i = 0; i < fieldLogicalTypes.length; i++) {
            toStringConverters[i] = createNullableStringConverter(fieldLogicalTypes[i]);
        }
    }

    /**
     * Create a nullable JDBC f{@link StringFormatRowConverter.StringFormatConverter} from given sql
     * type.
     */
    protected StringFormatRowConverter.StringFormatConverter createNullableStringConverter(
            LogicalType type) {
        return wrapIntoNullableExternalConverter(createStringFormatConverter(type), type);
    }

    protected StringFormatRowConverter.StringFormatConverter wrapIntoNullableExternalConverter(
            StringFormatRowConverter.StringFormatConverter stringConverter, LogicalType type) {
        return (val, index) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {

                return "null";
            } else {
                return stringConverter.convertToString(val, index);
            }
        };
    }

    interface StringFormatConverter extends Serializable {
        String convertToString(RowData rowData, int index);
    }

    public String[] convertToString(RowData rowData) {
        String[] fields = new String[toStringConverters.length];
        for (int index = 0; index < rowData.getArity(); index++) {
            fields[index] = toStringConverters[index].convertToString(rowData, index);
        }
        return fields;
    }

    protected StringFormatConverter createStringFormatConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index) -> val.getBoolean(index) ? "'true'" : "'false'";
            case TINYINT:
                return (val, index) -> "" + ((Byte) val.getByte(index)).intValue();
            case SMALLINT:
                return (val, index) -> String.valueOf(val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index) -> String.valueOf(val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index) -> String.valueOf(val.getLong(index));
            case FLOAT:
                return (val, index) -> String.valueOf(val.getFloat(index));
            case DOUBLE:
                return (val, index) -> String.valueOf(val.getDouble(index));
            case DECIMAL:
                return (val, index) -> {
                    final int decimalPrecision = ((DecimalType) type).getPrecision();
                    final int decimalScale = ((DecimalType) type).getScale();
                    return String.valueOf(
                            val.getDecimal(index, decimalPrecision, decimalScale)
                                    .toBigDecimal());
                };

            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index) -> toCopyField(val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index) -> new String(val.getBinary(index));
            case DATE:
                return (val, index) ->
                        String.valueOf(Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index) ->
                        String.valueOf(
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index) ->
                        String.valueOf(val.getTimestamp(index, timestampPrecision).toTimestamp());
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

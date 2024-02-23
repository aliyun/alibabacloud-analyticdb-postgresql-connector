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

import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import org.apache.flink.connector.jdbc.table.sink.api.DBValue;
import org.apache.flink.connector.jdbc.table.sink.api.GpssGrpc;
import org.apache.flink.connector.jdbc.table.sink.api.Row;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.Serializable;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for all converters that convert between StreamingServer(gRPC) object and Flink internal object.
 */
public class StreamingServerRowConverter implements Serializable {

    protected final StreamingServerDeserializationConverter[] toInternalConverters;
    protected final StreamingServerSerializationConverter[] toExternalConverters;
    protected final LogicalType[] fieldLogicalTypes;

    public StreamingServerRowConverter(DataType[] fieldTypes) {
        this(
                Arrays.asList(fieldTypes).stream()
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new));
    }

    public StreamingServerRowConverter(LogicalType[] fieldLogicalTypes) {
        this.fieldLogicalTypes = fieldLogicalTypes;
        this.toInternalConverters = new StreamingServerDeserializationConverter[fieldLogicalTypes.length];
        this.toExternalConverters = new StreamingServerSerializationConverter[fieldLogicalTypes.length];
        Row.Builder builder = Row.newBuilder();
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

    /**
     * Serialize single row data to the sink-end type
     * @param rowData The row data to be serialized
     * @param ssRows The list to store the serialized result
     */
    public int toExternal(RowData rowData, List<org.apache.flink.connector.jdbc.table.sink.api.RowData> ssRows) {
        Row.Builder rowBuilder = Row.newBuilder();
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, rowBuilder);
        }
        org.apache.flink.connector.jdbc.table.sink.api.RowData.Builder rowDataBuilder =
                org.apache.flink.connector.jdbc.table.sink.api.RowData.newBuilder().setData(rowBuilder.build().toByteString());
        org.apache.flink.connector.jdbc.table.sink.api.RowData ssRow = rowDataBuilder.build();
        ssRows.add(ssRow);
        return ssRow.getData().size();
    }

    /**
     * Serialize multiple row data to the sink-end type
     * @param rowDataList The row data list to be serialized
     */
    public List<org.apache.flink.connector.jdbc.table.sink.api.RowData> batchToExternal(List<RowData> rowDataList) {
        List<org.apache.flink.connector.jdbc.table.sink.api.RowData> ssRows = new ArrayList<>();
        for (RowData rowData : rowDataList) {
            Row.Builder rowBuilder = Row.newBuilder();
            for (int index = 0; index < rowData.getArity(); index++) {
                toExternalConverters[index].serialize(rowData, index, rowBuilder);
            }
            org.apache.flink.connector.jdbc.table.sink.api.RowData.Builder rowDataBuilder =
                    org.apache.flink.connector.jdbc.table.sink.api.RowData.newBuilder().setData(rowBuilder.build().toByteString());
            ssRows.add(rowDataBuilder.build());
        }
        return ssRows;
    }

    /**
     * Runtime converter to convert StreamingServer(gRPC) field to {@link RowData} type object.
     */
    @FunctionalInterface
    interface StreamingServerDeserializationConverter extends Serializable {
        /**
         * Convert a streaming server field object of {@link ResultSet} to the internal data structure object.
         *
         * @param ssField
         */
        Object deserialize(Object ssField) throws SQLException;
    }

    /**
     * Runtime converter to convert {@link RowData} field to java object and fill into the {@link
     * PreparedStatement}.
     */
    @FunctionalInterface
    interface StreamingServerSerializationConverter extends Serializable {
        void serialize(
                RowData rowData,
                int fieldIndex,
                Row.Builder builder
        );
    }

    /**
     * Create a nullable runtime {@link StreamingServerDeserializationConverter} from given {@link
     * LogicalType}.
     */
    protected StreamingServerDeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createInternalConverter(type));
    }

    protected StreamingServerDeserializationConverter wrapIntoNullableInternalConverter(
            StreamingServerDeserializationConverter streamingServerDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return streamingServerDeserializationConverter.deserialize(val);
            }
        };
    }

    protected StreamingServerDeserializationConverter createInternalConverter(LogicalType type) {
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
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case INTEGER:
                return val -> val;
            case BIGINT:
                return val -> val;

            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case DATE:
                // gRPC does not support data type, so we use string to represent date and cast to date type at server side
            case DECIMAL:
                // gRPC does not support decimal type, so we use string to represent date and cast to date type at server side
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * Create a nullable StreamingServer(gRPC) {@link StreamingServerSerializationConverter} from given sql type.
     */
    protected StreamingServerSerializationConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
    }

    protected StreamingServerSerializationConverter wrapIntoNullableExternalConverter(
            StreamingServerSerializationConverter ssSerializationConverter, LogicalType type) {
        final int ssType =
                StreamingServerTypeUtil.typeInformationToSSType(
                        TypeConversions.fromDataTypeToLegacyInfo(
                                TypeConversions.fromLogicalToDataType(type)));
        return (val, fieldIndex, builder) -> {
            if (val == null
                    || val.isNullAt(fieldIndex)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                builder.addColumns(DBValue.newBuilder().setNullValue(NullValue.NULL_VALUE));
            } else {
                ssSerializationConverter.serialize(val, fieldIndex, builder);
            }
        };
    }

    /**
     * Create a StreamingServer(gRPC) {@link StreamingServerSerializationConverter} from given {@link LogicalType}.
     * Datatype mapping between StreamingServer(gRPC) and ADBPG is as follows:
     * Flink type	gRPC Type	ADBPG Type
     * integer	Int32Value	integer, serial
     * bigint	Int64Value	bigint, bigserial
     * float	Float32Value	real
     * double	Float64Value	double
     * char, varchar	StringValue	text (any kind of data)
     * binary, varbinary	BytesValue	bytea
     * timestamp_without_time_zone	TimeStampValue	time, timestamp (without time zone)
     * @param type
     * @return
     */
    protected StreamingServerSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, fieldIndex, builder) ->
                        builder.addColumns(DBValue.newBuilder().setInt32Value(val.getInt(fieldIndex)));
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return (val, fieldIndex, builder) ->
                        builder.addColumns(DBValue.newBuilder().setInt64Value(val.getLong(fieldIndex)));
            case FLOAT:
                return (val, fieldIndex, builder) ->
                        builder.addColumns(DBValue.newBuilder().setFloat32Value(val.getFloat(fieldIndex)));
            case DOUBLE:
                return (val, fieldIndex, builder) ->
                        builder.addColumns(DBValue.newBuilder().setFloat64Value(val.getDouble(fieldIndex)));
            // These type can map to text type in ADBPG
            case DECIMAL:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case DATE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case CHAR:
            case VARCHAR:
                return (val, fieldIndex, builder) ->
                        builder.addColumns(DBValue.newBuilder().setStringValue(val.getString(fieldIndex).toString()));
            case BINARY:
            case VARBINARY:
                return (val, fieldIndex, builder) ->
                        builder.addColumns(DBValue.newBuilder().setBytesValue(ByteString.copyFrom(val.getBinary(fieldIndex))));
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, fieldIndex, builder) ->
                        builder.addColumns(
                                DBValue.newBuilder().setTimeStampValue(
                                        com.google.protobuf.Timestamp.newBuilder().setSeconds(
                                                val.getTimestamp(
                                                        fieldIndex,
                                                        ((TimestampType) type).getPrecision()).toTimestamp().getTime()
                                        ).setNanos(
                                                val.getTimestamp(
                                                        fieldIndex,
                                                        ((TimestampType) type).getPrecision()).toTimestamp().getNanos()
                                        )
                                )
                        );
            // Does not support
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

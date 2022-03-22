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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;


/** A simple StringUtil for blink-connectors based on org.apache.commons.lang3.StringUtils. From ververica-connectors */
public class StringUtils {

    private static final DataStructureConverter TIME_CONVERTER =
            DataStructureConverters.getConverter(DataTypes.TIME().bridgedTo(Time.class));
    private static final DataStructureConverter TIMESTAMP_CONVERTER =
            DataStructureConverters.getConverter(DataTypes.TIMESTAMP().bridgedTo(Timestamp.class));
    private static final DataStructureConverter DATE_CONVERTER =
            DataStructureConverters.getConverter(DataTypes.DATE().bridgedTo(Date.class));

    public static boolean isEmpty(String... strs) {
        for (String str : strs) {
            if (org.apache.commons.lang3.StringUtils.isEmpty(str)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isNotEmpty(String... strs) {
        return !isEmpty(strs);
    }

    public static String[] splitPreserveAllTokens(String src, String delimiter) {
        if (src == null) {
            return null;
        }
        if (delimiter == null) {
            return new String[] {src};
        }
        if (delimiter.length() == 1) {
            return org.apache.commons.lang3.StringUtils.splitPreserveAllTokens(
                    src, delimiter.charAt(0));
        } else {
            return org.apache.commons.lang3.StringUtils.splitPreserveAllTokens(src, delimiter);
        }
    }

    public static String[] split(String src, String delimiter) {
        return org.apache.commons.lang3.StringUtils.split(src, delimiter);
    }

    public static String join(String[] src) {
        return join(src, ",");
    }

    public static String join(String[] src, String delimiter) {
        return org.apache.commons.lang3.StringUtils.join(src, delimiter);
    }

    /**
     * Checks whether the given string is null, empty, or contains only whitespace/delimiter
     * character.
     */
    public static boolean isBlank(String str, String delimiter) {
        if (str == null || str.length() == 0) {
            return true;
        }
        if (null == delimiter) {
            return org.apache.commons.lang3.StringUtils.isBlank(str);
        }
        if (delimiter.length() == 1) {
            char dChar = delimiter.charAt(0);
            final int len = str.length();
            for (int i = 0; i < len; i++) {
                if (!Character.isWhitespace(str.charAt(i)) && dChar != str.charAt(i)) {
                    return false;
                }
            }
        } else {
            String[] array = org.apache.commons.lang3.StringUtils.split(str, delimiter);
            for (String s : array) {
                if (!org.apache.commons.lang3.StringUtils.isBlank(s)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isEmptyKey(Object key) {
        if (key == null) {
            return true;
        }
        String val = String.valueOf(key);
        if (org.apache.commons.lang3.StringUtils.isBlank(val)) {
            return true;
        }
        return false;
    }

    /**
     * Return the first candidate which is not a blank string, otherwise return null.
     *
     * @param candidates
     */
    public static String coalesce(String... candidates) {
        if (null != candidates && candidates.length > 0) {
            for (String c : candidates) {
                if (!org.apache.commons.lang3.StringUtils.isBlank(c)) {
                    return c;
                }
            }
        }
        return null;
    }

    // BinaryStringData
    public static byte[] toBytes(BinaryStringData string, Charset stringCharset) {
        if (stringCharset == Charset.forName("UTF-8")) {
            return string.toBytes();
        } else {
            return string.toString().getBytes(stringCharset);
        }
    }

    public static BinaryStringData fromBytes(byte[] bytes, Charset stringCharset) {
        if (stringCharset == Charset.forName("UTF-8")) {
            return BinaryStringData.fromBytes(bytes);
        } else {
            return BinaryStringData.fromString(new String(bytes, stringCharset));
        }
    }

    public static Object convertStringToInternalObject(String v, DataType dataType) {
        if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BINARY)) {
            return parseBytes(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARBINARY)) {
            return parseBytes(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.CHAR)) {
            return StringData.fromString(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
            return StringData.fromString(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.TINYINT)) {
            return Byte.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.SMALLINT)) {
            return Short.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
            return Integer.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
            return Long.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.FLOAT)) {
            return Float.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DOUBLE)) {
            return Double.valueOf(v);
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.BOOLEAN)) {
            v = v.equals("t") ? "true" : "false";
            return Boolean.valueOf(v);
        } else if (dataType.getLogicalType()
                .getTypeRoot()
                .equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            // Postgres supports TIMESTAMP_WITHOUT_TIME_ZONE and TIMESTAMP_WITH_TIME_ZONE
            // Flink doesn't support TIMESTAMP_WITH_TIME_ZONE
            String timezonePart = v.substring(v.length() - 3);
            Timestamp ts;
            if (isValidTimeZone(timezonePart)) {
                String datetimePart = v.substring(0, v.length() - 3).replace(" ", "T");
                ts =
                        Timestamp.valueOf(
                                ZonedDateTime.of(
                                                LocalDateTime.parse(datetimePart),
                                                ZoneId.of(timezonePart))
                                        .toLocalDateTime());
            } else {
                ts = Timestamp.valueOf(LocalDateTime.parse(v.replace(" ", "T")));
            }
            return TIMESTAMP_CONVERTER.toInternal(ts);
        } else if (dataType.getLogicalType()
                .getTypeRoot()
                .equals(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)) {
            return TIME_CONVERTER.toInternal(Time.valueOf(v));
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DATE)) {
            return DATE_CONVERTER.toInternal(Date.valueOf(v));
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.DECIMAL)) {
            DecimalType logicalType = (DecimalType) dataType.getLogicalType();
            return DecimalData.fromBigDecimal(
                    new BigDecimal(v), logicalType.getPrecision(), logicalType.getScale());
        } else if (dataType.getLogicalType().getTypeRoot().equals(LogicalTypeRoot.ARRAY)) {
            String[] arrayObject = v.split("\\{|}|,");
            if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.BIGINT)) {
                long[] arrLongObject = new long[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrLongObject[i - 1] = Long.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrLongObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.INTEGER)) {
                int[] arrIntObject = new int[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrIntObject[i - 1] = Integer.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrIntObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.FLOAT)) {
                float[] arrFloatObject = new float[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrFloatObject[i - 1] = Float.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrFloatObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.DOUBLE)) {
                double[] arrDoubleObject = new double[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrDoubleObject[i - 1] = Double.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrDoubleObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.VARCHAR)) {
                StringData[] arrStringObject = new StringData[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrStringObject[i - 1] = StringData.fromString(arrayObject[i]);
                }
                return new GenericArrayData(arrStringObject);
            } else if (dataType.getLogicalType()
                    .getChildren()
                    .get(0)
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.BOOLEAN)) {
                boolean[] arrBoolObject = new boolean[arrayObject.length - 1];
                for (int i = 1; i < arrayObject.length; ++i) {
                    arrayObject[i] = arrayObject[i].equals("t") ? "true" : "false";
                    arrBoolObject[i - 1] = Boolean.valueOf(arrayObject[i]);
                }
                return new GenericArrayData(arrBoolObject);
            } else {
                throw new IllegalArgumentException(
                        "Currently does not support type: " + dataType);
            }
        }
        throw new IllegalArgumentException("Unknown type: " + dataType);
    }

    /** Postgres presents zone id in ZoneOffset, e.g. "+08". */
    private static boolean isValidTimeZone(String zoneId) {
        try {
            ZoneOffset.of(zoneId);
        } catch (DateTimeException e) {
            return false;
        }
        return true;
    }

    private static byte[] parseBytes(String hex) {
        try {
            // Postgres uses the Hex format to store the bytes.
            // The input string hex is beginning with "\\x". Please refer to
            // https://www.postgresql.org/docs/11/datatype-binary.html?spm=a2c4g.11186623.2.9.3a907ad7ihlkEI
            return Hex.decodeHex(hex.substring(3));
        } catch (DecoderException e) {
            throw new RuntimeException(
                    String.format("Failed to parse the bytes from the '%s'.", hex), e);
        }
    }

}

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

import org.apache.flink.table.data.binary.BinaryStringData;

import java.nio.charset.Charset;

/** A simple StringUtil for blink-connectors based on org.apache.commons.lang3.StringUtils. From ververica-connectors */
public class StringUtils {

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
}

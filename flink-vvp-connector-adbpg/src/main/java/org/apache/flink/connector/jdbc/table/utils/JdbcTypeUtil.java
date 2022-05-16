
package org.apache.flink.connector.jdbc.table.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

/** copy from ververica-connectors to support JdbcRowConverter **/
@Internal
public final class JdbcTypeUtil {
    private static final Map<TypeInformation<?>, Integer> TYPE_MAPPING;
    private static final Map<Integer, String> SQL_TYPE_NAMES;

    private JdbcTypeUtil() {
    }

    public static int typeInformationToSqlType(TypeInformation<?> type) {
        if (TYPE_MAPPING.containsKey(type)) {
            return (Integer) TYPE_MAPPING.get(type);
        } else if (!(type instanceof ObjectArrayTypeInfo) && !(type instanceof PrimitiveArrayTypeInfo)) {
            throw new IllegalArgumentException("Unsupported type: " + type);
        } else {
            return 2003;
        }
    }

    public static String getTypeName(int type) {
        return (String) SQL_TYPE_NAMES.get(type);
    }

    public static String getTypeName(TypeInformation<?> type) {
        return (String) SQL_TYPE_NAMES.get(typeInformationToSqlType(type));
    }

    static {
        HashMap<TypeInformation<?>, Integer> m = new HashMap();
        m.put(BasicTypeInfo.STRING_TYPE_INFO, 12);
        m.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, 16);
        m.put(BasicTypeInfo.BYTE_TYPE_INFO, -6);
        m.put(BasicTypeInfo.SHORT_TYPE_INFO, 5);
        m.put(BasicTypeInfo.INT_TYPE_INFO, 4);
        m.put(BasicTypeInfo.LONG_TYPE_INFO, -5);
        m.put(BasicTypeInfo.FLOAT_TYPE_INFO, 7);
        m.put(BasicTypeInfo.DOUBLE_TYPE_INFO, 8);
        m.put(SqlTimeTypeInfo.DATE, 91);
        m.put(SqlTimeTypeInfo.TIME, 92);
        m.put(SqlTimeTypeInfo.TIMESTAMP, 93);
        m.put(LocalTimeTypeInfo.LOCAL_DATE, 91);
        m.put(LocalTimeTypeInfo.LOCAL_TIME, 92);
        m.put(LocalTimeTypeInfo.LOCAL_DATE_TIME, 93);
        m.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, 3);
        m.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, -2);
        TYPE_MAPPING = Collections.unmodifiableMap(m);
        HashMap<Integer, String> names = new HashMap();
        names.put(12, "VARCHAR");
        names.put(16, "BOOLEAN");
        names.put(-6, "TINYINT");
        names.put(5, "SMALLINT");
        names.put(4, "INTEGER");
        names.put(-5, "BIGINT");
        names.put(6, "FLOAT");
        names.put(8, "DOUBLE");
        names.put(1, "CHAR");
        names.put(91, "DATE");
        names.put(92, "TIME");
        names.put(93, "TIMESTAMP");
        names.put(3, "DECIMAL");
        names.put(-2, "BINARY");
        SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
    }
}


package org.apache.flink.connector.jdbc.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** copy from ververica-connectors to support StreamingServerRowConverter **/
@Internal
public final class StreamingServerTypeUtil {
    private static final Map<TypeInformation<?>, Integer> TYPE_MAPPING;
    private static final Map<Integer, String> SQL_TYPE_NAMES;

    private StreamingServerTypeUtil() {
    }

    public static int typeInformationToSSType(TypeInformation<?> type) {
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
        return (String) SQL_TYPE_NAMES.get(typeInformationToSSType(type));
    }

    public static TableSchema normalizeTableSchema(TableSchema schema) {
        Builder physicalSchemaBuilder = TableSchema.builder();
        schema.getTableColumns().forEach((c) -> {
            if (c.isPhysical()) {
                DataType type = DataTypeUtils.transform(c.getType(), new TypeTransformation[]{TypeTransformations.timeToSqlTypes()});
                physicalSchemaBuilder.field(c.getName(), type);
            }
        });
        return physicalSchemaBuilder.build();
    }

    static {
        HashMap<TypeInformation<?>, Integer> m = new HashMap<>();
        m.put(BasicTypeInfo.STRING_TYPE_INFO, java.sql.Types.VARCHAR);
        m.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, java.sql.Types.BOOLEAN);
        m.put(BasicTypeInfo.BYTE_TYPE_INFO, java.sql.Types.TINYINT);
        m.put(BasicTypeInfo.SHORT_TYPE_INFO, java.sql.Types.SMALLINT);
        m.put(BasicTypeInfo.INT_TYPE_INFO, java.sql.Types.INTEGER);
        m.put(BasicTypeInfo.LONG_TYPE_INFO, java.sql.Types.BIGINT);
        m.put(BasicTypeInfo.FLOAT_TYPE_INFO, java.sql.Types.REAL);
        m.put(BasicTypeInfo.DOUBLE_TYPE_INFO, java.sql.Types.DOUBLE);
        m.put(SqlTimeTypeInfo.DATE, java.sql.Types.DATE);
        m.put(SqlTimeTypeInfo.TIME, java.sql.Types.TIME);
        m.put(SqlTimeTypeInfo.TIMESTAMP, java.sql.Types.TIMESTAMP);
        m.put(LocalTimeTypeInfo.LOCAL_DATE, java.sql.Types.DATE);
        m.put(LocalTimeTypeInfo.LOCAL_TIME, java.sql.Types.TIME);
        m.put(LocalTimeTypeInfo.LOCAL_DATE_TIME, java.sql.Types.TIMESTAMP);
        m.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, java.sql.Types.DECIMAL);
        m.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, java.sql.Types.BINARY);
        TYPE_MAPPING = Collections.unmodifiableMap(m);

        HashMap<Integer, String> names = new HashMap<>();
        names.put(java.sql.Types.VARCHAR, "VARCHAR");
        names.put(java.sql.Types.BOOLEAN, "BOOLEAN");
        names.put(java.sql.Types.TINYINT, "TINYINT");
        names.put(java.sql.Types.SMALLINT, "SMALLINT");
        names.put(java.sql.Types.INTEGER, "INTEGER");
        names.put(java.sql.Types.BIGINT, "BIGINT");
        names.put(java.sql.Types.FLOAT, "FLOAT");
        names.put(java.sql.Types.DOUBLE, "DOUBLE");
        names.put(java.sql.Types.CHAR, "CHAR");
        names.put(java.sql.Types.DATE, "DATE");
        names.put(java.sql.Types.TIME, "TIME");
        names.put(java.sql.Types.TIMESTAMP, "TIMESTAMP");
        names.put(java.sql.Types.DECIMAL, "DECIMAL");
        names.put(java.sql.Types.BINARY, "BINARY");
        SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
    }
}

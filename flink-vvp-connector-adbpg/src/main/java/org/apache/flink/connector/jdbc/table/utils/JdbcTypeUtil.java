
package org.apache.flink.connector.jdbc.table.utils;

import java.sql.Types;
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
        m.put(BasicTypeInfo.STRING_TYPE_INFO, Types.VARCHAR);
        m.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, Types.BOOLEAN);
        m.put(BasicTypeInfo.BYTE_TYPE_INFO, Types.TINYINT);
        m.put(BasicTypeInfo.SHORT_TYPE_INFO, Types.SMALLINT);
        m.put(BasicTypeInfo.INT_TYPE_INFO, Types.INTEGER);
        m.put(BasicTypeInfo.LONG_TYPE_INFO, Types.BIGINT);
        m.put(BasicTypeInfo.FLOAT_TYPE_INFO, Types.REAL);
        m.put(BasicTypeInfo.DOUBLE_TYPE_INFO, Types.DOUBLE);
        m.put(SqlTimeTypeInfo.DATE, Types.DATE);
        m.put(SqlTimeTypeInfo.TIME, Types.TIME);
        m.put(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP);
        m.put(LocalTimeTypeInfo.LOCAL_DATE, Types.DATE);
        m.put(LocalTimeTypeInfo.LOCAL_TIME, Types.TIME);
        m.put(LocalTimeTypeInfo.LOCAL_DATE_TIME, Types.TIMESTAMP);
        m.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, Types.DECIMAL);
        m.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY);
        TYPE_MAPPING = Collections.unmodifiableMap(m);

        HashMap<Integer, String> names = new HashMap<>();
        names.put(Types.VARCHAR, "VARCHAR");
        names.put(Types.BOOLEAN, "BOOLEAN");
        names.put(Types.TINYINT, "TINYINT");
        names.put(Types.SMALLINT, "SMALLINT");
        names.put(Types.INTEGER, "INTEGER");
        names.put(Types.BIGINT, "BIGINT");
        names.put(Types.FLOAT, "FLOAT");
        names.put(Types.DOUBLE, "DOUBLE");
        names.put(Types.CHAR, "CHAR");
        names.put(Types.DATE, "DATE");
        names.put(Types.TIME, "TIME");
        names.put(Types.TIMESTAMP, "TIMESTAMP");
        names.put(Types.DECIMAL, "DECIMAL");
        names.put(Types.BINARY, "BINARY");
        SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
    }
}

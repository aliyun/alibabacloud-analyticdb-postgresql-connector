package com.alibaba.analyticdb.postgresql.client;

import com.alibaba.analyticdb.postgresql.MutationType;

import java.io.Serializable;
import java.sql.Types;
import java.util.BitSet;
import java.util.List;
import org.postgresql.jdbc.ArrayUtil;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;

public class Record implements Serializable {
	protected final TableMetadata metadata;
	TableName tableName;
	BitSet alreadyBitSet;				// 列是否已经被set

	Object[] values;

	MutationType type = MutationType.INSERT;

	long byteSize = 0;

	public Record(TableMetadata schema) {
		this.metadata = schema;
		this.tableName = schema.getTableNameObj();
		values = new Object[schema.getColumnSchema().length];
	}

	private Record(TableMetadata metadata, TableName tableName, Object[] values, MutationType type, long byteSize) {
		this.metadata = metadata;
		this.tableName = tableName;
		this.values = values;
		this.type = type;
		this.byteSize = byteSize;
	}

	public static Record build(TableMetadata schema) {
		return new Record(schema);
	}

	public void setType(MutationType type) {
		this.type = type;
	}

	public int[] getKeyIndex() {
		return metadata.getKeyIndex();
	}

	public TableMetadata getMetadata() {
		return metadata;
	}

	public void setObject(int index, Object obj) {
		Object old = values[index];
		long oldByteSize = 0L;
		if (isSet(index)) {
			oldByteSize = getObjByteSize(index, old);			// 如果之前已经set过该列，减去先前的byteSize，加上新byteSize
		}
		long newByteSize = getObjByteSize(index, obj);
		byteSize = byteSize - oldByteSize + newByteSize ;
		values[index] = obj;
		alreadyBitSet.set(index);

	}

	public Object getObject(int index) {
		return values[index];
	}

	public boolean isSet(int index) {
		return alreadyBitSet.get(index);
	}


	public long getByteSize() {
		return byteSize;
	}

	private long getObjByteSize(int index, Object obj) {
		if (obj == null) {
			return 4;
		}
		long ret = 0;
		Column column = metadata.getColumnSchema()[index];
		switch (column.getType()) {
			case Types.BOOLEAN:
			case Types.TINYINT:
			case Types.BIT:
				ret = 1;
				break;
			case Types.SMALLINT:
				ret = 2;
				break;
			case Types.BIGINT:
			case Types.DOUBLE:
				ret = 8;
				break;
			case Types.TIMESTAMP:
			case Types.TIME_WITH_TIMEZONE:
				ret = 12;
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				ret = 24;
				break;
			case Types.CHAR:
			case Types.VARCHAR:
				ret = String.valueOf(obj).length();
				break;
			case Types.ARRAY:
				if (obj instanceof int[]) {
					ret = ((int[]) obj).length * 4L;
				} else if (obj instanceof long[]) {
					ret = ((long[]) obj).length * 8L;
				} else if (obj instanceof float[]) {
					ret = ((float[]) obj).length * 4L;
				} else if (obj instanceof double[]) {
					ret = ((double[]) obj).length * 8L;
				} else if (obj instanceof boolean[]) {
					ret = ((boolean[]) obj).length;
				} else if (obj instanceof String[]) {
					ret = ArrayUtil.getArrayLength((String[]) obj);
				} else if (obj instanceof Object[]) {
					ret = ArrayUtil.getArrayLength((Object[]) obj, column.getTypeName());
				} else if (obj instanceof List) {
					ret = ArrayUtil.getArrayLength((List<?>) obj, column.getTypeName());
				} else if (obj instanceof PgArray) {
					ret = ArrayUtil.getArrayLength((PgArray) obj);
				} else {
					ret = 1024;
				}
				break;
			default:
				if ("json".equalsIgnoreCase(column.getTypeName()) || "jsonb".equalsIgnoreCase(column.getTypeName())) { // json, jsonb 等类型
					ret = String.valueOf(obj).length();
					break;
				}
				if (obj instanceof PGobject) { // PGmoney 等类型
					PGobject pObj = (PGobject) obj;
					if (pObj.getValue() != null) {
						ret = pObj.getValue().length();
					}
				} else if (obj instanceof byte[]) { // RoaringBitmap, bytea 等类型
					ret = ((byte[]) obj).length;
				} else {
					ret = 4;
				}
		}
		return ret;
	}
}

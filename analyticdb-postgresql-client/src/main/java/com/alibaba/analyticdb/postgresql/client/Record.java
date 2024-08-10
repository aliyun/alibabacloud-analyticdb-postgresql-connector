package com.alibaba.analyticdb.postgresql.client;

import java.io.Serializable;

public class Record implements Serializable {
	protected final TableMetadata schema;
	TableName tableName;

	Object[] values;

	Put.MutationType type = Put.MutationType.INSERT;

	long byteSize = 0;

	private Record(TableMetadata schema, TableName tableName, Object[] values, Put.MutationType type, long byteSize) {
		this.schema = schema;
		this.tableName = tableName;
		this.values = values;
		this.type = type;
		this.byteSize = byteSize;
	}

}

package com.alibaba.analyticdb.postgresql.client;

import java.io.Closeable;
import java.io.IOException;

public class ADBClient implements Closeable {

	public TableMetadata getTableMetadata(String tableName) {
		return null;
	}

	public TableMetadata getTableMetadata(TableName tableName) {
		return null;
	}

	public TableMetadata getTableMetadata(String schemaName, boolean noCache) {
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}

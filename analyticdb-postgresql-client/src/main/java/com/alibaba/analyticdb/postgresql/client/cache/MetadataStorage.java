package com.alibaba.analyticdb.postgresql.client.cache;

import com.alibaba.analyticdb.postgresql.client.TableMetadata;
import com.alibaba.analyticdb.postgresql.client.TableName;

public class MetadataStorage {
	public final LRUCache<TableName, TableMetadata> tableMetadataCache;
	// TODO partition metadata cache here?

	public MetadataStorage(long tableCacheTTL) {
		this.tableMetadataCache = new LRUCache<>(tableCacheTTL, null);
	}

}

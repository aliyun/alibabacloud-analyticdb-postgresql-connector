/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.analyticdb.postgresql.client.action;

import com.alibaba.analyticdb.postgresql.client.TableMetadata;
import com.alibaba.analyticdb.postgresql.client.TableName;

public class MetadataAction extends AbstractAction<TableMetadata> {

	TableName tableName;

	public MetadataAction(TableName tableName) {
		this.tableName = tableName;
	}

	public TableName getTableName() {
		return tableName;
	}
}

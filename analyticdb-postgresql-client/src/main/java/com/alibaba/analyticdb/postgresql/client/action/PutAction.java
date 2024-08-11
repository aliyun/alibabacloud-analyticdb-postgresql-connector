/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.analyticdb.postgresql.client.action;


import com.alibaba.analyticdb.postgresql.client.TableMetadata;
import com.alibaba.analyticdb.postgresql.client.WriteMode;
import com.alibaba.analyticdb.postgresql.client.Record;

import java.security.InvalidParameterException;
import java.util.List;

/**
 * pa.
 */
public class PutAction extends AbstractAction<Void> {

	final List<Record> recordList;
	final long byteSize;
	TableMetadata metadata;
	WriteMode writeMode;

	/**
	 * 提供的recordList必须都是相同tableSchema下的.
	 *
	 * @param recordList
	 * @param byteSize
	 */
	public PutAction(List<Record> recordList, long byteSize, WriteMode mode) {
		this.recordList = recordList;
		this.byteSize = byteSize;
		this.writeMode = mode;
		if (recordList.size() > 0) {
			metadata = recordList.get(0).getMetadata();
			for (Record record : recordList) {
				if (!record.getMetadata().equals(metadata)) {
					throw new InvalidParameterException("Records in PutAction must for the same table. the first table is " + metadata.getTableNameObj().getFullName() + " but found another table " + record.getMetadata().getTableNameObj().getFullName());
				}
			}
		} else {
			throw new InvalidParameterException("Empty records in PutAction is invalid");
		}
	}

	public List<Record> getRecordList() {
		return recordList;
	}

	public WriteMode getWriteMode() {
		return writeMode;
	}

	public long getByteSize() {
		return byteSize;
	}

	public TableMetadata getMetadata() {
		return metadata;
	}
}

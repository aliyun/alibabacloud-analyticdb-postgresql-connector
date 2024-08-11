package com.alibaba.analyticdb.postgresql.client;

import com.alibaba.analyticdb.postgresql.MutationType;

import java.security.InvalidParameterException;
import java.util.BitSet;

public class Delete extends Mutation{
	Record record;



	public Delete(TableMetadata schema) {
		this.record = Record.build(schema);
		record.setType(MutationType.DELETE);
	}

	public Delete(Record record) {
		this.record = record;
	}

	public Record getRecord() {
		return record;
	}

	public Delete setObject(int colIndex, Object obj) {
		record.setObject(colIndex, obj);
		return this;
	}

	public Delete setObject(String columnName, Object obj) {
		Integer i = record.getMetadata().getColumnIndex(columnName);
		if (i == null) {
			throw new InvalidParameterException("can not found column named " + columnName);
		}
		setObject(i, obj);
		return this;
	}


	public boolean isSet(int i) {
		return record.isSet(i);
	}

	@Override
	public long heapSize() {
		return 0;
	}

}

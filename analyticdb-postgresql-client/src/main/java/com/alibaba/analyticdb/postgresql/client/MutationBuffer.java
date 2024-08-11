package com.alibaba.analyticdb.postgresql.client;

import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 缓存所有表的Put, Delete等Mutation
 */
public class MutationBuffer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MutationBuffer.class);
	Map<TableName, TableMutationBuffer> tableMutationMap;
	private ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();

	public MutationBuffer() {
		tableMutationMap = new java.util.concurrent.ConcurrentHashMap<>();
	}

	public void addMutationBuffer(TableName tableName, TableMutationBuffer buffer) {
		tableMutationMap.put(tableName, buffer);
	}

	public TableMutationBuffer getTableMutationBuffer(TableName tableName) {
		return tableMutationMap.get(tableName);
	}

	public void tryFlush() {
		flushLock.readLock().lock();
		try {
			for (Iterator<Map.Entry<TableName, TableMutationBuffer>> iter = tableMutationMap.entrySet().iterator(); iter.hasNext(); ) {
				TableMutationBuffer array = iter.next().getValue();
				try {
					array.flush(false);
				} catch (ADBClientException e) {
					LOGGER.error("try flush fail", e);
				}
			}
		} finally {
			flushLock.readLock().unlock();
		}
	}

	public void flush(boolean internal) throws ADBClientException {
		flushLock.writeLock().lock();
		try {
			ADBClientException exception = null;
			int doneCount = 0;
			AtomicInteger uncommittedActionCount = new AtomicInteger(0);
			boolean async = true;
			while (true) {
				doneCount = 0;
				uncommittedActionCount.set(0);
				// 遍历所有的TableMutationBuffer
				for (Iterator<Map.Entry<TableName, TableMutationBuffer>> iter = tableMutationMap.entrySet().iterator(); iter.hasNext(); ) {
					TableMutationBuffer array = iter.next().getValue();
					if (array.flush(true, async, uncommittedActionCount)) {
						++doneCount;
					}
				}
				if (uncommittedActionCount.get() == 0) {
					async = false;
				}
			}
		} finally {
			flushLock.writeLock().unlock();
		}
	}

}

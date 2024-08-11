package com.alibaba.analyticdb.postgresql.client;

import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于存储针对于某张表的mutation
 */
public class TableMutationBuffer {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableMutationBuffer.class);

	List<Mutation> mutations;
	BitSet deleteBitSet;			// 识别整个put list中哪列是delete，这样可以在delete对先前的数据进行commit

	public boolean flush(boolean force) throws ADBClientException {
		return flush(force, true);
	}

	public boolean flush(boolean force, boolean async) throws ADBClientException {
		return flush(force, async, null);
	}

	public boolean flush(boolean force, boolean async, AtomicInteger uncommittedActionCount) throws ADBClientException {

		int doneCount = 0;
		for (int i = 0; i < mutations.size(); i++) {
			Mutation mutation = mutations.get(i);
			if (mutation instanceof com.alibaba.analyticdb.postgresql.client.Delete) {
				// TODO
				System.out.println("~~~~~delete");
			} else if (mutation instanceof com.alibaba.analyticdb.postgresql.client.Put) {
				// TODO
				System.out.println("~~~~~put");
			}
		}

		return true;
	}
}

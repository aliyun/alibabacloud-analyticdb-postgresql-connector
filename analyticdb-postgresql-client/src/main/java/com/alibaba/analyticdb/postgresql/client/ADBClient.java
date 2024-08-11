package com.alibaba.analyticdb.postgresql.client;

import com.alibaba.analyticdb.postgresql.client.action.MetadataAction;
import com.alibaba.analyticdb.postgresql.client.cache.MetadataStorage;
import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;
import com.alibaba.analyticdb.postgresql.client.task.TaskExecutorPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ADBClient implements Closeable {
	public static final Logger LOGGER = LoggerFactory.getLogger(ADBClient.class);

	private TaskExecutorPool pool = null;
	private ADBConfiguration config;
	private MetadataStorage metadataStorage;

	static {
		// Load DriverManager first to avoid deadlock between DriverManager's
		// static initialization block and specific driver class's static
		// initialization block when two different driver classes are loading
		// concurrently using Class.forName while DriverManager is uninitialized
		// before.
		//
		// This could happen in JDK 8 but not above as driver loading has been
		// moved out of DriverManager's static initialization block since JDK 9.
		DriverManager.getDrivers();
	}

	private MutationBuffer mutationBuffer;

	public ADBClient(ADBConfiguration config) throws ADBClientException {
		try {
			DriverManager.getDrivers();
			Class.forName("org.postgresql.Driver");
		} catch (Exception e) {
			throw new ADBClientException("Errors happened when loading driver", e);
		}
		checkConfiguration(config);
		this.config = config;
	}

	private void checkConfiguration(ADBConfiguration config) throws ADBClientException {
		if (config == null) {
			throw new ADBClientException("Configuration can't be null");
		}
		if (config.getJdbcURL() == null || config.getJdbcURL().isEmpty()) {
			throw new ADBClientException("JDBC URL can't be null or empty");
		}
		if (config.getUsername() == null || config.getUsername().isEmpty()) {
			throw new ADBClientException("Username can't be null or empty");
		}
		if (config.getPassword() == null || config.getPassword().isEmpty()) {
			throw new ADBClientException("Password can't be null or empty");
		}
	}

	public ADBConfiguration getConfiguration() {
		return this.config;
	}

	public TableMetadata getTableMetadata(String tableName) throws ADBClientException {
		ensurePoolOpen();
		TableName tbl = new TableName(tableName);
		return pool.getOrSubmitTableSchema(tbl, false);
	}

	public TableMetadata getTableMetadata(TableName tableName) throws ADBClientException {
		return pool.getOrSubmitTableSchema(tableName, false);
	}

	public TableMetadata getTableMetadata(String schemaName, boolean noCache) {
		return null;
	}

	public void put(Put put) throws ADBClientException {
		checkOrLunchExecutor();
		checkPut(put);



	}

	private void checkOrLunchExecutor() throws ADBClientException {
		if (this.pool == null) {
			synchronized (this) {
				TaskExecutorPool pool = new TaskExecutorPool("ADBClient task execution pool", this.config);
				this.pool = pool;
			}
		}
	}

	public void flush() {

	}

	public void checkPut(Put put) throws ADBClientException {
		if (put == null) {
			throw new ADBClientException("Put cant't be null");
		}
		for (int index: put.getRecord().getKeyIndex()) {

		}
	}

	private void ensurePoolOpen() throws ADBClientException {
		if (pool == null) {
			synchronized (this) {
				if (pool == null) {
					TaskExecutorPool temp = new TaskExecutorPool("embedded-" + config.getApplicationName(), config);
					// 当useFixedFe为true 则不赋值新的collector，调用这个函数仅仅为了把pool标记为started.
					MutationBuffer buffer = temp.register(this, config);
					pool = temp;

				}
			}
		}
	}

	@Override
	public void close() throws IOException {

	}
}

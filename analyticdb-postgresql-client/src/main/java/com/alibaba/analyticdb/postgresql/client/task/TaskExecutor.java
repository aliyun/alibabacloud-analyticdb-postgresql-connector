package com.alibaba.analyticdb.postgresql.client.task;

import com.alibaba.analyticdb.postgresql.client.ADBConfiguration;
import com.alibaba.analyticdb.postgresql.client.ObjectChan;
import com.alibaba.analyticdb.postgresql.client.action.AbstractAction;
import com.alibaba.analyticdb.postgresql.client.action.EmptyAction;
import com.alibaba.analyticdb.postgresql.client.action.MetadataAction;
import com.alibaba.analyticdb.postgresql.client.action.PutAction;
import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;
import com.alibaba.analyticdb.postgresql.client.handler.ActionHandler;
import com.alibaba.analyticdb.postgresql.client.handler.MetaActionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TaskExecutor implements Runnable {
	public static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

	final Connection conn;		// TODO manage connection like connectionHolder 
	ObjectChan<AbstractAction> recordCollector = new ObjectChan<>();
	final AtomicBoolean started;
	final ADBConfiguration config;
	AtomicReference<Throwable> fatal = new AtomicReference<>(null);
	private final String name;
	Map<Class, ActionHandler> handlerMap = new HashMap<>();

	public TaskExecutor(ADBConfiguration config, AtomicBoolean started, int index, boolean isShadingEnv) throws ADBClientException {
		this(config, started, index, isShadingEnv, false);
	}

	public TaskExecutor(ADBConfiguration config, AtomicBoolean started, int index, boolean isShadingEnv, boolean isFixed) throws ADBClientException {
		this.config = config;
		try {
			conn = DriverManager.getConnection(config.getJdbcURL(), config.getUsername(), config.getPassword());
		} catch (SQLException e) {
			throw new ADBClientException("Error when create conn to db:", e);
		}
		this.started = started;
		this.name = (isFixed ? "Fixed-" : "") + "Worker-" + index;
		handlerMap.put(MetadataAction.class, new MetaActionHandler(conn, config));
//		handlerMap.put(PutAction.class, new PutActionHandler(conn, config));
	}

	public boolean offer(AbstractAction action) throws ADBClientException {
		if (fatal.get() != null) {
			throw new ADBClientException("Internal, fatal", fatal.get());
		}
		if (action != null) {
			if (!started.get()) {
				throw new ADBClientException("Internal, executor is already close");
			}
			return this.recordCollector.set(action);
		} else {
			return this.recordCollector.set(new EmptyAction());
		}
	}

	protected  <T extends AbstractAction> void handle(T action) throws ADBClientException {
		String metricsName = null;
		long start = System.nanoTime();
		try {
			ActionHandler<T> handler = handlerMap.get(action.getClass());
			if (handler == null) {
				throw new ADBClientException("Internal, Unknown action:" + action.getClass().getName());
			}
			metricsName = handler.getCostMsMetricName();
			handler.handle(action);
		} catch (Throwable e) {
			if (action.getFuture() != null && !action.getFuture().isDone()) {
				action.getFuture().completeExceptionally(e);
			}
			throw e;
		} finally {
			// TODO record metrics here
		}
	}

	@Override
	public void run() {
		LOGGER.info("worker:{} start", this);
		while (started.get()) {
			try {
				AbstractAction action = recordCollector.get(2000L, TimeUnit.MILLISECONDS);
				/*
				 * 每个循环做2件事情：
				 * 1 有action就执行action
				 * 2 根据connectionMaxIdleMs释放空闲connection
				 * 3 根据connectionMaxAliveMs释放存活时间比较久的connection
				 * */
				if (null != action) {
					try {
						handle(action);
					} finally {
						recordCollector.clear();
						if (action.getSemaphore() != null) {
							action.getSemaphore().release();
						}

					}
				}
			} catch (Throwable e) {
				LOGGER.error("should not happen", e);
				fatal.set(e);
				break;
			}

		}
		LOGGER.info("worker:{} stop", this);
		try {
			conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public String toString() {
		return name;
	}
}


package com.alibaba.analyticdb.postgresql.client.task;

import com.alibaba.analyticdb.postgresql.client.*;
import com.alibaba.analyticdb.postgresql.client.action.*;
import com.alibaba.analyticdb.postgresql.client.cache.LRUCache;
import com.alibaba.analyticdb.postgresql.client.cache.MetadataStorage;
import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskExecutorPool {
	public static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorPool.class);

	// 用于确保对于给定的配置或名称，只创建一个ExecutionPool实例
	static final Map<String, TaskExecutorPool> POOL_MAP = new ConcurrentHashMap<>();
	private final Map<ADBClient, MutationBuffer> clientMap;


	// 用于如metadata刷新等后台任务
	private Runnable backgroundJob;

	private AtomicBoolean started;
	private AtomicBoolean executorStated;

	private ADBConfiguration configuration;
	private String name;

	private TaskExecutor[] executors;

	private Semaphore writeSemaphore;
	private Semaphore readSemaphore;				// TODO

	private final MetadataStorage metadataStorage;
	final long refreshMetaTimeout;

	Thread shutdownHandler = null;

	ThreadFactory executorThreadFactory;			// 用于执行数据读写相关action
	ThreadFactory maintainerThreadFactory;			// 用于执行像meta刷新类action

	ExecutorService executorExecutorService;
	ExecutorService maintainerExecutorService;

	int writeThreadSize;
	int readThreadSize;



	public TaskExecutorPool(String name, ADBConfiguration config) {
		this.clientMap = new ConcurrentHashMap<>();
		this.configuration = config;
		started = new AtomicBoolean(false);
		executorStated = new AtomicBoolean(false);
		this.metadataStorage = new MetadataStorage(config.getMetadataTTL());
		executorThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(TaskExecutorPool.this.name + "-executor");
				return t;
			}
		};

		maintainerThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(TaskExecutorPool.this.name + "-maintainer");
				return t;
			}
		};

		this.writeThreadSize = 0;
		this.refreshMetaTimeout = config.getRefreshMetadataTimeout();
		executors = new TaskExecutor[0];
	}


	public TableMetadata getOrSubmitTableSchema(TableName tableName, boolean noCache) throws ADBClientException {

		try {
			return metadataStorage.tableMetadataCache.get(tableName, (tn) -> {
				try {
					MetadataAction metaAction = new MetadataAction(tableName);
					while (!submit(metaAction)) {
					}
					return metaAction.getResult();
				} catch (ADBClientException e) {
					throw new SQLException(e);
				}
			}, noCache ? LRUCache.MODE_NO_CACHE : LRUCache.MODE_LOCAL_THEN_REMOTE);
		} catch (SQLException e) {
			throw new ADBClientException("RefreshMetadataError:", e);
		}
	}

	/**
	 * @param action action
	 * @return 提交成功返回true；所有worker都忙时返回false
	 */
	public boolean submit(AbstractAction action) throws ADBClientException {
		if (!started.get()) {
			throw new ADBClientException("submit fail, executor pool is not started");
		}
		Semaphore semaphore = null;
		int start = -1;
		int end = -1;
		if (action instanceof PutAction) {
			semaphore = writeSemaphore;
			start = 0;
			end = Math.min(writeThreadSize, executors.length);
		} else if (action instanceof GetAction || action instanceof ScanAction) {
			semaphore = readSemaphore;
			throw new ADBClientException("Unsupport feature: action not support for yet");
		} else {
			start = 0;
			end = executors.length;
		}

		//如果有信号量，尝试获取信号量，否则返回submit失败
		if (semaphore != null) {
			try {
				boolean acquire = semaphore.tryAcquire(2000L, TimeUnit.MILLISECONDS);
				if (!acquire) {
					return false;
				}
			} catch (InterruptedException e) {
				throw new ADBClientException("Internal, submit Interrupted", e);
			}
			action.setSemaphore(semaphore);
		}
		//尝试提交
		for (int i = start; i < end; ++i) {
			TaskExecutor executor = executors[i];
			if (executor.offer(action)) {
				return true;
			}
		}
		//提交失败则释放,提交成功Worker会负责释放
		if (semaphore != null) {
			semaphore.release();
		}
		return false;
	}

	private synchronized void start() throws ADBClientException {
		if (started.compareAndSet(false, true)) {
			LOGGER.info("HoloClient ExecutionPool[{}] start", name);
			executorStated.set(true);
			executorExecutorService = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1), executorThreadFactory, new ThreadPoolExecutor.AbortPolicy());
			maintainerExecutorService = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1), maintainerThreadFactory, new ThreadPoolExecutor.AbortPolicy());
			for (int i = 0; i < executors.length; ++i) {
				executorExecutorService.execute(executors[i]);
			}

			maintainerExecutorService.execute(backgroundJob);
			writeSemaphore = new Semaphore(writeThreadSize);
		}
	}

	public synchronized MutationBuffer register(ADBClient client, ADBConfiguration config) throws ADBClientException {
		boolean needStart = false;
		MutationBuffer buffer = null;
		synchronized (clientMap) {
			boolean empty = clientMap.isEmpty();
			buffer = clientMap.get(client);
			if (buffer == null) {
				LOGGER.info("register client {}, client size {}->{}", client, clientMap.size(), clientMap.size() + 1);
				buffer = new MutationBuffer();
				clientMap.put(client, buffer);
				if (empty) {
					needStart = true;
				}
			}
			if (needStart) {
				start();
			}
		}

		return buffer;
	}


	class BackgroundJob implements Runnable {

		long tableSchemaRemainLife;
		long forceFlushInterval;
		long metadataTTL;
		AtomicInteger pendingRefreshTableSchemaActionCount;

		public BackgroundJob(ADBConfiguration config) {
			forceFlushInterval = config.getForceFlushInterval();
			metadataTTL = config.getMetadataTTL();
			pendingRefreshTableSchemaActionCount = new AtomicInteger(0);
		}

		long lastForceFlushMs = -1L;

		private void triggerTryFlush() {
			synchronized (clientMap) {
				boolean force = false;

				if (forceFlushInterval > 0L) {
					long current = System.currentTimeMillis();
					if (current - lastForceFlushMs > forceFlushInterval) {
						force = true;
						lastForceFlushMs = current;
					}
				}
				for (MutationBuffer buffer : clientMap.values()) {
					try {
						if (force) {
							buffer.flush(true);
						} else {
							buffer.tryFlush();
						}
					} catch (ADBClientException e) {
//						fatalException = new Tuple<>(LocalDateTime.now().toString(), e);
						LOGGER.error("flush fail", e);
						break;
					}
				}
			}
		}

		private void refreshTableSchema() {
			//避免getTableSchema返回太慢的时候，同个表重复刷新TableSchema
			if (pendingRefreshTableSchemaActionCount.get() == 0) {
				// 万一出现非预期的异常也不会导致线程结束工作
				try {
					metadataStorage.tableMetadataCache.filterKeys(tableSchemaRemainLife).forEach(tableNameWithState -> {
						LRUCache.ItemState state = tableNameWithState.l;
						TableName tableName = tableNameWithState.r;
						switch (state) {
							case EXPIRE:
								LOGGER.info("remove expire tableSchema for {}", tableName);
								metadataStorage.tableMetadataCache.remove(tableName);
								break;
							case NEED_REFRESH:
								try {
									getOrSubmitTableSchema(tableName, true);
									MetadataAction metaAction = new MetadataAction(tableName);
									if (submit(metaAction)) {
										LOGGER.info("refresh tableSchema for {}, because remain lifetime < {} ms", tableName, tableSchemaRemainLife);
										pendingRefreshTableSchemaActionCount.incrementAndGet();
										metaAction.getFuture().whenCompleteAsync((tableSchema, exception) -> {
											pendingRefreshTableSchemaActionCount.decrementAndGet();
											if (exception != null) {
												LOGGER.warn("refreshTableSchema fail", exception);
												if (exception.getMessage() != null && exception.getMessage().contains("can not found table")) {
													metadataStorage.tableMetadataCache.remove(tableName);
												}
											} else {
												metadataStorage.tableMetadataCache.put(tableName, tableSchema);
											}
										});
									}
								} catch (Exception e) {
									LOGGER.warn("refreshTableSchema fail", e);
								}
								break;
							default:
								LOGGER.error("undefine item state {}", state);
						}

					});
				} catch (Throwable e) {
					LOGGER.warn("refreshTableSchema unexpected fail", e);
				}
			}
		}

		@Override
		public void run() {
			while (started.get()) {
				triggerTryFlush();
				refreshTableSchema();
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignore) {
					break;
				}
			}
		}

		@Override
		public String toString() {
			return "CommitJob";
		}
	}

}

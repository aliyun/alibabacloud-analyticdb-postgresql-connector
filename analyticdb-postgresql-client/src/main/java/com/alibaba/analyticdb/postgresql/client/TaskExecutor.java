package com.alibaba.analyticdb.postgresql.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskExecutor {
	public static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

	// 用于确保对于给定的配置或名称，只创建一个ExecutionPool实例
	static final Map<String, TaskExecutor> POOL_MAP = new ConcurrentHashMap<>();

	// 用于如metadata刷新等后台任务
	private Runnable backgroundJob;


}

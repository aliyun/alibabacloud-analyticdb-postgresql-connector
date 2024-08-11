/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.analyticdb.postgresql.client.handler;

import com.alibaba.analyticdb.postgresql.client.ADBConfiguration;
import com.alibaba.analyticdb.postgresql.client.TableMetadata;
import com.alibaba.analyticdb.postgresql.client.action.MetadataAction;
import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;
import com.alibaba.analyticdb.postgresql.client.util.ConnectionUtil;


import java.sql.Connection;
import java.sql.SQLException;

/**
 * MetaAction处理类.
 */
public class MetaActionHandler extends ActionHandler<MetadataAction> {

	private static final String NAME = "meta";
	private final ADBConfiguration config;
	private final Connection conn;

	public MetaActionHandler(Connection conn, ADBConfiguration config) {
		super(config);
		this.config = config;
		this.conn = conn;
	}

	@Override
	public void handle(MetadataAction action) throws ADBClientException {
		try {
			TableMetadata metadata = ConnectionUtil.getTableMetadata(conn, action.getTableName());
			action.getFuture().complete(metadata);
		} catch (SQLException e) {
			throw new ADBClientException("Error when refresh metadata", e);
		}
	}

	@Override
	public String getCostMsMetricName() {
		return NAME + METRIC_COST_MS;
	}
}

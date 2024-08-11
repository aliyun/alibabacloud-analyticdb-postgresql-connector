/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.analyticdb.postgresql.client.handler;


import com.alibaba.analyticdb.postgresql.client.ADBConfiguration;
import com.alibaba.analyticdb.postgresql.client.action.EmptyAction;

/**
 * EmptyAction处理类.
 */
public class EmptyActionHandler extends ActionHandler<EmptyAction> {

	private static final String NAME = "empty";

	public EmptyActionHandler(ADBConfiguration config) {
		super(config);
	}

	@Override
	public void handle(EmptyAction action) {

	}

	@Override
	public String getCostMsMetricName() {
		return NAME + METRIC_COST_MS;
	}
}

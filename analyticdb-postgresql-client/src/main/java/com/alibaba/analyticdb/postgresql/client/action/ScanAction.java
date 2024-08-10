/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.analyticdb.postgresql.client.action;

import com.alibaba.hologres.client.Scan;
import com.alibaba.hologres.client.model.RecordScanner;

/**
 * ga.
 */
public class ScanAction extends AbstractAction<RecordScanner> {

	Scan scan;

	public ScanAction(Scan scan) {
		this.scan = scan;
	}

	public Scan getScan() {
		return scan;
	}
}

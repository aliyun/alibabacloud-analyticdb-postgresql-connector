package com.alibaba.analyticdb.postgresql.client;

public enum WriteMode {
	INSERT_OR_REPORT,		// insert or report duplicate key
	INSERT_OR_IGNORE,		// insert on conflict do nothing
	INSERT_OR_UPDATE,		// insert on conflict do update
	INSERT_OR_UPDATEALL
}

package com.alibaba.analyticdb.postgresql.client.exception;

public class ADBClientException extends Exception {

	// todo error code here?

	public ADBClientException(String message, Exception e) {
		super(message, e);
	}

	public ADBClientException(String message, Throwable t) {
		super(message, t);
	}

	public ADBClientException(String message) {
		super(message);
	}
}

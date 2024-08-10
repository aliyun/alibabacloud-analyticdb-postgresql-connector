package com.alibaba.analyticdb.postgresql.client.exception;

public class InvalidRelationException extends RuntimeException {
	public InvalidRelationException() {
	}

	public InvalidRelationException(String message) {
		super(message);
	}

	public InvalidRelationException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidRelationException(Throwable cause) {
		super(cause);
	}
}

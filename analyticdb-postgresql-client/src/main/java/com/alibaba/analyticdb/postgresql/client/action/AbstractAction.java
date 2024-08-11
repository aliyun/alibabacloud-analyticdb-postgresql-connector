/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.analyticdb.postgresql.client.action;

import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

/**
 * ca.
 *
 * @param <T> t
 */
public abstract class AbstractAction<T> {
	CompletableFuture<T> future;

	Semaphore semaphore;

	public AbstractAction() {
		this.future = new CompletableFuture<>();
	}

	public CompletableFuture<T> getFuture() {
		return future;
	}

	public T getResult() throws ADBClientException {
		try {
			return future.get();
		} catch (InterruptedException e) {
			throw new ADBClientException("Internal error, interrupt", e);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof ADBClientException) {
				throw (ADBClientException) cause;
			} else {
				throw new ADBClientException("Internal error", cause);
			}
		}
	}

	public Semaphore getSemaphore() {
		return semaphore;
	}

	public void setSemaphore(Semaphore semaphore) {
		this.semaphore = semaphore;
	}
}

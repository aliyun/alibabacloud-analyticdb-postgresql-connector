package com.alibaba.analyticdb.postgresql.client;

import java.io.Serializable;

public abstract class Mutation implements Serializable, HeapSize {
	public enum Type {
		INSERT,
		DELETE
	}
}

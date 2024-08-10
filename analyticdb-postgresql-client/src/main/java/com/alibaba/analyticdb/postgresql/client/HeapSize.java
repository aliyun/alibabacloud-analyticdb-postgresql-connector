package com.alibaba.analyticdb.postgresql.client;

/**
 * Implementations can be asked for an estimate of their size in bytes.
 * <p>
 * Useful for sizing caches. Its a given that implementation approximations do not account for 32 vs
 * 64 bit nor for different VM implementations.
 * <p>
 * An Object's size is determined by the non-static data members in it, as well as the fixed
 * {@link Object} overhead.
 * <p>
 * For example:
 *
 * <pre>
 * public class SampleObject implements HeapSize {
 *
 *   int[] numbers;
 *   int x;
 * }
 * </pre>
 */
public interface HeapSize {
	/**
	 * Return the approximate 'exclusive deep size' of implementing object. Includes count of payload
	 * and hosting object sizings.
	 */
	long heapSize();
}

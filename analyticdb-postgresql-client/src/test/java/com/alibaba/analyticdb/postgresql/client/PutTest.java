package com.alibaba.analyticdb.postgresql.client;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PutTest {


	@BeforeAll
	public static void setUp() {
		// get $ADBPG_HOST, $ADBPG_PORT, $ADBPG_USER, $ADBPG_PASS from environment variables
		String host = System.getenv("ADBPG_HOST");
		String port = System.getenv("ADBPG_PORT");
		String user = System.getenv("ADBPG_USER");
		String pass = System.getenv("ADBPG_PASS");
	}

	@Test
	public void testPut() {
	}
}

package com.alibaba.analyticdb.postgresql.client;

import com.alibaba.analyticdb.postgresql.client.exception.ADBClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ADBClientTest {
	private static final String HOST = System.getenv("ADBPG_HOST");
	private static final String PORT = System.getenv("ADBPG_PORT");
	private static final String USER = System.getenv("ADBPG_USER");
	private static final String PASS = System.getenv("ADBPG_PASS");
	private static final String JDBC_URL = "jdbc:postgresql://" + HOST + ":" + PORT + "/postgres";

	@BeforeAll
	public static void createTestTable() {
		// create a test table in target database with postgresql jdbc sql
		String sql = "CREATE TABLE IF NOT EXISTS tbl (id int, name varchar(20));";
		try {
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASS);
			Statement statement = connection.createStatement();
			statement.executeUpdate(sql);
		} catch (ClassNotFoundException | SQLException e) {
			throw new RuntimeException(e);
		}



	}


	@Test
	public void TestPut() throws ADBClientException {
		ADBClient client = getClient();

		TableMetadata metadata = client.getTableMetadata("tbl");
		System.out.println(metadata);
	}

	private ADBClient getClient() {
		ADBClient client;

		// get $ADBPG_HOST, $ADBPG_PORT, $ADBPG_USER, $ADBPG_PASS from environment variables
		String host = System.getenv("ADBPG_HOST");
		String port = System.getenv("ADBPG_PORT");
		String user = System.getenv("ADBPG_USER");
		String pass = System.getenv("ADBPG_PASS");
		ADBConfiguration conf = new ADBConfiguration();
		conf.setJdbcURL("jdbc:postgresql://" + host + ":" + port + "/postgres");
		conf.setUsername(user);
		conf.setPassword(pass);
		try {
			client = new ADBClient(conf);
		} catch (ADBClientException e) {
			throw new RuntimeException(e);
		}

		return client;
	}

}


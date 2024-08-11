package com.alibaba.analyticdb.postgresql.client;

public class ADBConfiguration {

	String jdbcURL;
	String username;
	String password;
	String applicationName;
	WriteMode writeMode;
	long refreshMetadataTimeout;
	long forceFlushInterval;
	long metadataTTL;

	public long getForceFlushInterval() {
		return forceFlushInterval;
	}

	public void setForceFlushInterval(long forceFlushInterval) {
		this.forceFlushInterval = forceFlushInterval;
	}

	public void setRefreshMetadataTimeout(long refreshMetadataTimeout) {
		this.refreshMetadataTimeout = refreshMetadataTimeout;
	}

	public long getMetadataTTL() {
		return metadataTTL;
	}

	public void setMetadataTTL(long metadataTTL) {
		this.metadataTTL = metadataTTL;
	}

	public WriteMode getWriteMode() {
		return writeMode;
	}

	public void setWriteMode(WriteMode writeMode) {
		this.writeMode = writeMode;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getJdbcURL() {
		return jdbcURL;
	}

	public void setJdbcURL(String jdbcURL) {
		this.jdbcURL = jdbcURL;
	}

	public long getRefreshMetadataTimeout() {
		return refreshMetadataTimeout;
	}

	public void setRefreshMetadataTimeout(int refreshMetadataTimeout) {
		this.refreshMetadataTimeout = refreshMetadataTimeout;
	}

	@Override
	public String toString() {
		return "ADBConfiguration{" +
				"jdbcURL='" + jdbcURL + '\'' +
				", username='" + username + '\'' +
				", password='" + password + '\'' +
				", applicationName='" + applicationName + '\'' +
				", writeMode=" + writeMode +
				'}';
	}
}

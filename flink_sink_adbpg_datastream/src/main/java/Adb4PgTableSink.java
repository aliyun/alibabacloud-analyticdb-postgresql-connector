import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Adb4PgTableSink extends RichSinkFunction<Row> {
	private transient static final Logger LOG = LoggerFactory.getLogger(Adb4PgTableSink.class);

	// meta
	private String url;
	private String tableName;
	private String userName;
	private String password;
	private Set<String> primaryKeys;
	private String fieldNames = null;
	private String fieldNamesCaseSensitive = null;
	private String primaryFieldNames = null;
	private String nonPrimaryFieldNames = null;
	private String primaryFieldNamesCaseSensitive = null;
	private String nonPrimaryFieldNamesCaseSensitive = null;
	private String excludedNonPrimaryFieldNames = null;
	private String excludedNonPrimaryFieldNamesCaseSensitive = null;

	// write policy
	private int maxRetryTime = 3;
	private int batchSize = 500;
	private long batchWriteTimeout = 5000;
	private long lastWriteTime = 0;
	private List<Tuple2<Boolean, Row>> mapBuffer = new ArrayList();
	private String insertClause = "INSERT INTO ";
	private String timeZone;
	private final String DELETE_WITH_KEY_SQL_TPL = "DELETE FROM %s WHERE %s ";
	private long inputCount = 0;

	// datasource
	private String driverClassName = "org.postgresql.Driver";
	private int connectionMaxActive = 40;
	private int connectionInitialSize = 5;
	private int connectionMinIdle = 5;
	private int maxWait = 60000;
	private int removeAbandonedTimeout = 3 * 60;
	private boolean connectionTestWhileIdle = true;

	private static ConnectionPool<DruidDataSource> dataSourcePool = new ConnectionPool<>();
	private transient DruidDataSource dataSource = null;
	private transient ScheduledExecutorService executorService;
	private String dataSourceKey = "";
	private transient Connection connection;
	private transient Statement statement;
	private boolean reserveMs = false;
	private static volatile boolean existsPrimaryKeys = false;
	private String conflictMode = "ignore";
	private int useCopy = 0;
	private String targetSchema = "public";
	private String exceptionMode = "ignore";
	private boolean caseSensitive = false;
	private int writeMode = 0;
	private AdbpgTableSchema schema;
	private int retryWaitTime = 100;

	public Adb4PgTableSink(
			String url,
			String tableName,
			String userName,
			String password,
			AdbpgTableSchema schema,
			List<String> primaryKeys){
		this.url = url;
		this.tableName = tableName;
		this.userName = userName;
		this.password = password;
		this.primaryKeys = new HashSet<String>(primaryKeys);
		this.schema = schema;
		Joiner joinerOnComma = Joiner.on(",").useForNull("null");
		String[] fieldNamesStr = new String[schema.getLength()];
		for (int i = 0; i < fieldNamesStr.length; i++) {
			fieldNamesStr[i] = "\"" + schema.getFieldNames().get(i) + "\"";
		}
		this.fieldNames = joinerOnComma.join(fieldNamesStr);
	}

	public Adb4PgTableSink(
			String url,
			String tableName,
			String userName,
			String password,
			AdbpgTableSchema schema,
			List<String> primaryKeys,
			int batchSize,
			int maxRetryTime,
			String driverClassName,
			int connectionMaxActive,
			int connectionInitialSize,
			int connectionMinIdle,
			boolean connectionTestWhileIdle,
			int maxWait,
			int removeAbandonedTimeout,
			long batchWriteTimeout) {
		this.url = url;
		this.tableName = tableName;
		this.userName = userName;
		this.password = password;
		this.primaryKeys = new HashSet<String>(primaryKeys);
		this.batchSize = batchSize;
		this.maxRetryTime = maxRetryTime;
		this.driverClassName = driverClassName;
		this.connectionMaxActive = connectionMaxActive;
		this.connectionInitialSize = connectionInitialSize;
		this.connectionMinIdle = connectionMinIdle;
		this.connectionTestWhileIdle = connectionTestWhileIdle;
		this.maxWait = maxWait;
		this.removeAbandonedTimeout = removeAbandonedTimeout;
		this.batchWriteTimeout = batchWriteTimeout;
		Joiner joinerOnComma = Joiner.on(",").useForNull("null");
		String[] fieldNamesStr = new String[schema.getLength()];
		for (int i = 0; i < fieldNamesStr.length; i++) {
			fieldNamesStr[i] = "\"" + schema.getFieldNames().get(i) + "\"";
		}
		this.fieldNames = joinerOnComma.join(fieldNamesStr);
	}

	public void setMaxRetryTime(int maxRetryTime) {
		this.maxRetryTime = maxRetryTime;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public void setBatchWriteTimeout(long batchWriteTimeout) {
		this.batchWriteTimeout = batchWriteTimeout;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	public void setConnectionMaxActive(int connectionMaxActive) {
		this.connectionMaxActive = connectionMaxActive;
	}

	public void setConnectionInitialSize(int connectionInitialSize) {
		this.connectionInitialSize = connectionInitialSize;
	}

	public void setConnectionMinIdle(int connectionMinIdle) {
		this.connectionMinIdle = connectionMinIdle;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
		this.removeAbandonedTimeout = removeAbandonedTimeout;
	}

	public void setConnectionTestWhileIdle(boolean connectionTestWhileIdle) {
		this.connectionTestWhileIdle = connectionTestWhileIdle;
	}

	public void setReserveMs(boolean reserveMs) {
		this.reserveMs = reserveMs;
	}

	public void setConflictMode(String conflictMode) {
		this.conflictMode = conflictMode;
	}

	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}

	public void setExceptionMode(String exceptionMode) {
		this.exceptionMode = exceptionMode;
	}

	public void setCaseSensitive(boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}

	public void setWriteMode(int writeMode) {
		this.writeMode = writeMode;
	}

	public void setRetryWaitTime(int retryWaitTime) {
		this.retryWaitTime = retryWaitTime;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		synchronized (Adb4PgTableSink.class) {
			dataSourceKey = url + userName + password + tableName;
			if (dataSourcePool.contains(dataSourceKey)) {
				dataSource = dataSourcePool.get(dataSourceKey);
			} else {
				dataSource = new DruidDataSource();
				dataSource.setUrl(url);
				dataSource.setUsername(userName);
				dataSource.setPassword(password);
				dataSource.setDriverClassName(driverClassName);
				dataSource.setMaxActive(connectionMaxActive);
				dataSource.setInitialSize(connectionInitialSize);
				dataSource.setMaxWait(maxWait);
				dataSource.setMinIdle(connectionMinIdle);
				dataSource.setTimeBetweenEvictionRunsMillis(2000);
				dataSource.setMinEvictableIdleTimeMillis(600000);
				dataSource.setMaxEvictableIdleTimeMillis(900000);
				dataSource.setValidationQuery("select 1");
				dataSource.setTestOnBorrow(false);
				dataSource.setTestWhileIdle(connectionTestWhileIdle);
				dataSource.setRemoveAbandoned(true);
				dataSource.setRemoveAbandonedTimeout(removeAbandonedTimeout);
				try {
					dataSource.init();
				} catch (SQLException e) {
					LOG.error("Init DataSource Or Get Connection Error!", e);
					throw new RuntimeException("cannot get connection for url: " + url +", userName: " + userName +", password: " + password, e);
				}
				dataSourcePool.put(dataSourceKey, dataSource);
			}
			if (primaryKeys == null || primaryKeys.isEmpty()) {
				existsPrimaryKeys = false;
				if (2 == this.writeMode) {
					throw new RuntimeException("primary key cannot be empty when setting write mode to 2:upsert.");
				}
			}
			else {
				existsPrimaryKeys = true;
				Joiner joinerOnComma = Joiner.on(",").useForNull("null");
				String[] primaryFieldNamesStr = new String[primaryKeys.size()];
				String[] nonPrimaryFieldNamesStr = new String[schema.getLength() - primaryKeys.size()];
				String[] primaryFieldNamesStrCaseSensitive = new String[primaryKeys.size()];
				String[] nonPrimaryFieldNamesStrCaseSensitive = new String[schema.getLength() - primaryKeys.size()];
				String[] excludedNonPrimaryFieldNamesStr = new String[schema.getLength() - primaryKeys.size()];
				String[] excludedNonPrimaryFieldNamesStrCaseSensitive = new String[schema.getLength() - primaryKeys.size()];
				int primaryIndex = 0;
				int excludedIndex = 0;
				for (int i = 0; i < schema.getLength(); i++) {
					String fileName = schema.getFieldNames().get(i);
					if (primaryKeys.contains(fileName)) {
						primaryFieldNamesStr[primaryIndex] = fileName;
						primaryFieldNamesStrCaseSensitive[primaryIndex++] = "\"" + fileName + "\"";
					}
					else {
						nonPrimaryFieldNamesStr[excludedIndex] = fileName;
						nonPrimaryFieldNamesStrCaseSensitive[excludedIndex] = "\"" + fileName + "\"";
						excludedNonPrimaryFieldNamesStr[excludedIndex] = "excluded." + fileName;
						excludedNonPrimaryFieldNamesStrCaseSensitive[excludedIndex++] = "excluded.\"" + fileName + "\"";
					}
				}
				this.primaryFieldNames = joinerOnComma.join(primaryFieldNamesStr);
				this.nonPrimaryFieldNames = joinerOnComma.join(nonPrimaryFieldNamesStr);
				this.primaryFieldNamesCaseSensitive = joinerOnComma.join(primaryFieldNamesStrCaseSensitive);
				this.nonPrimaryFieldNamesCaseSensitive = joinerOnComma.join(nonPrimaryFieldNamesStrCaseSensitive);
				this.excludedNonPrimaryFieldNames = joinerOnComma.join(excludedNonPrimaryFieldNamesStr);
				this.excludedNonPrimaryFieldNamesCaseSensitive = joinerOnComma.join(excludedNonPrimaryFieldNamesStrCaseSensitive);
			}
			executorService = new ScheduledThreadPoolExecutor(1,
					new BasicThreadFactory.Builder().namingPattern("adbpg-flusher-%d").daemon(true).build());
			executorService.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						if(System.currentTimeMillis() - lastWriteTime >= batchWriteTimeout){
							sync();
						}
					} catch (Exception e) {
						LOG.error("flush buffer to ADBPG failed", e);
					}
				}
			}, batchWriteTimeout, batchWriteTimeout, TimeUnit.MILLISECONDS);
		}
		LOG.info("connector created using url=" + url + ", " +
				"tableName=" + tableName + ", " +
				"userName=" + userName + ", " +
				"password=" + password + ", " +
				"maxRetries=" + maxRetryTime + ", " +
				"batchSize=" + batchSize + ", " +
				"connectionMaxActive=" + connectionMaxActive + ", " +
				"batchWriteTimeoutMs=" + batchWriteTimeout + ", " +
				"conflictMode=" + conflictMode + ", " +
				"timeZone=" + timeZone + ", " +
				"useCopy=" + useCopy + ", " +
				"targetSchema=" + targetSchema + ", " +
				"exceptionMode=" + exceptionMode + ", " +
				"reserveMs=" + reserveMs + ", " +
				"caseSensitive=" + caseSensitive +", " +
				"writeMode=" + writeMode);
	}

	public void invoke(Row row, Context context) throws Exception{
		this.writeAddRecord(row);
	}

	public void writeAddRecord(Row row) throws IOException {
		inputCount++;
		if (null != row) {
			synchronized (mapBuffer) {
				mapBuffer.add(new Tuple2<Boolean, Row>(true, row));
			}
		}

		if (inputCount >= batchSize) {
			sync();
		} else if (System.currentTimeMillis() - this.lastWriteTime > this.batchWriteTimeout) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("reach timeout: " + this.batchWriteTimeout + "ms, buffer=" + mapBuffer.size());
			}
			sync();
		}
	}

	public void sync(){
		synchronized (mapBuffer) {
			List<Row> addBuffer = new ArrayList();
			List<Row> deleteBuffer = new ArrayList();
			Collection<Tuple2<Boolean, Row>> buffer = mapBuffer;
			if (buffer.size() > 0) {
				for (Tuple2<Boolean, Row> rowTuple2 : buffer) {
					if (rowTuple2.f0) {
						addBuffer.add(rowTuple2.f1);
					} else {
						deleteBuffer.add(rowTuple2.f1);
					}
				}
				batchWrite(addBuffer);
				if (existsPrimaryKeys) {
					batchDelete(deleteBuffer);
				} else {
					batchDeleteWithoutPk(deleteBuffer);
				}
			}
			mapBuffer.clear();
			inputCount = 0;
			lastWriteTime = System.currentTimeMillis();
		}
	}

	private void batchWrite(List<Row> rows) {
		if (null == rows || rows.size() == 0){
			return ;
		}
		long start = System.currentTimeMillis();
		try {
			if (writeMode == 1) {
				StringBuilder stringBuilder = new StringBuilder();
				for (Row row : rows) {
					String[] fields = writeCopyFormat(schema, row, timeZone, reserveMs);
					for(int i = 0; i < fields.length; i++) {
						stringBuilder.append(fields[i]);
						stringBuilder.append(i == fields.length - 1 ? "\r\n" : "\t");
					}
				}
				byte[] data = stringBuilder.toString().getBytes(Charsets.UTF_8);
				InputStream inputStream = new ByteArrayInputStream(data);
				executeCopy(inputStream);
			}
			else if (writeMode == 2){
				List<String> valueList = new ArrayList<String>();
				String[] fields;
				for (Row row : rows) {
					fields = writeFormat(schema, row, timeZone, reserveMs);
					valueList.add("(" + StringUtils.join(fields, ",") + ")");
				}

				StringBuilder sb = new StringBuilder();
				if (caseSensitive) {
					sb.append(insertClause).append("\"").append(targetSchema).append("\"").append(".").append("\"").append(tableName).append("\"").append(" (" + fieldNamesCaseSensitive + " ) values ");
				}
				else {
					sb.append(insertClause).append(targetSchema).append(".").append(tableName).append(" (" + fieldNames + " ) values ");
				}

				sb.append(StringUtils.join(valueList, ","));
				if (caseSensitive) {
					sb.append(" on conflict(").append(primaryFieldNamesCaseSensitive).append(") ").append(" do update set (").append(nonPrimaryFieldNamesCaseSensitive).append(")=(").append(excludedNonPrimaryFieldNamesCaseSensitive).append(")");
				}
				else {
					sb.append(" on conflict(").append(primaryFieldNames).append(") ").append(" do update set (").append(nonPrimaryFieldNames).append(")=(").append(excludedNonPrimaryFieldNames).append(")");
				}
				executeSql(sb.toString());
			}
			else {
				List<String> valueList = new ArrayList<String>();
				String[] fields;
				for (Row row : rows) {
					fields = writeFormat(schema, row, timeZone, reserveMs);
					valueList.add("(" + StringUtils.join(fields, ",") + ")");
				}

				StringBuilder sb = new StringBuilder();
				if (caseSensitive) {
					sb.append(insertClause).append("\"").append(targetSchema).append("\"").append(".").append("\"").append(tableName).append("\"").append(" (" + fieldNamesCaseSensitive + " ) values ");
				}
				else {
					sb.append(insertClause).append(targetSchema).append(".").append(tableName).append(" (" + fieldNames + " ) values ");
				}
				String sql = sb.toString() + StringUtils.join(valueList, ",");
				executeSql(sql);
			}
		} catch (Exception e) {
			LOG.warn("execute sql error:", e);
			if (existsPrimaryKeys
					&& e.getMessage() != null
					&& e.getMessage().indexOf("duplicate key") != -1
					&& e.getMessage().indexOf("violates unique constraint") != -1
					&& "upsert".equalsIgnoreCase(conflictMode)){
				LOG.warn("batch insert failed in upsert mode, will try to upsert msgs one by one", e);
				for (Row row : rows) {
					upsertRow(row);
				}
			}
			else {
				LOG.warn("batch insert failed, will try to insert msgs one by one", e);
				for (Row row : rows) {
					String insertSQL = getInsertSQL(row);
					try {
						executeSql(insertSQL);
					} catch (SQLException insertException) {
						//insertException.printStackTrace();
						LOG.warn("Exception in insert sql: " + insertSQL, insertException);
						if (existsPrimaryKeys
								&& insertException.getMessage() != null
								&& insertException.getMessage().indexOf("duplicate key") != -1
								&& insertException.getMessage().indexOf("violates unique constraint") != -1) {
							if ("strict".equalsIgnoreCase(conflictMode)) {
								throw new RuntimeException("duplicate key value violates unique constraint");
							} else if ("update".equalsIgnoreCase(conflictMode)) {
								updateRow(row);
							}
							else if ("upsert".equalsIgnoreCase(conflictMode) || (2 == writeMode)) {
								upsertRow(row);
							}
						}
						else {
							if ("strict".equalsIgnoreCase(exceptionMode)) {
								throw new RuntimeException(insertException);
							}
						}
					}
				}
			}
		}
		long end = System.currentTimeMillis();
	}

	private String getInsertSQL(Row row) {
		StringBuilder sb1 = new StringBuilder();
		String[] singleFields = writeFormat(schema, row, timeZone, reserveMs);
		if (caseSensitive) {
			sb1.append(insertClause).append("\"").append(targetSchema).append("\"").append(".").append("\"").append(tableName).append("\"").append(" (" + fieldNamesCaseSensitive + " ) values ");
		}
		else {
			sb1.append(insertClause).append(targetSchema).append(".").append(tableName).append(" (" + fieldNames + " ) values ");
		}
		sb1.append("(" + StringUtils.join(singleFields, ",") + ")");
		return sb1.toString();
	}

	private void batchDelete(List<Row> buffers) {
		for (Row row : buffers) {
			StringBuilder sb = new StringBuilder();
			if (caseSensitive) {
				sb.append("DELETE FROM ").append("\"").append(targetSchema).append("\".\"").append(tableName).append("\" where ");
			}
			else {
				sb.append("DELETE FROM ").append(targetSchema).append(".").append(tableName).append(" where ");
			}
			Object[] output = deleteFormat(schema, row, new HashSet<String>(primaryKeys), timeZone, reserveMs);
			sb.append(org.apache.commons.lang3.StringUtils.join(output, " and "));
			String sql = sb.toString();
			try {
				executeSql(sql);
			}
			catch (SQLException e) {
				LOG.warn("Exception in delete sql: " + sql, e);
			}
		}
	}

	private void batchDeleteWithoutPk(List<Row> buffers) {
		for (Row row : buffers) {
			Joiner joinerOnComma = Joiner.on(" AND ").useForNull("null");
			List<String> sub = new ArrayList<String>();
			for (int i = 0; i < row.getArity(); i++) {
				if (caseSensitive){
					if (row.getField(i) == null) {
						sub.add(" \"" + schema.getFieldNames().get(i) + "\" is null ");
					}
					else {
						sub.add(" \"" + schema.getFieldNames().get(i) + "\" = " +
								toField(row.getField(i)));
					}
				}
				else {
					if (row.getField(i) == null) {
						sub.add(" " + schema.getFieldNames().get(i) + " is null ");
					}
					else {
						sub.add(" " + schema.getFieldNames().get(i) + " = " +
								toField(row.getField(i)));
					}
				}
			}
			String sql = null;
			if (caseSensitive) {
				sql = String.format(DELETE_WITH_KEY_SQL_TPL, "\"" + targetSchema + "\".\"" + tableName + "\"", joinerOnComma.join(sub));
			}
			else {
				sql = String.format(DELETE_WITH_KEY_SQL_TPL, targetSchema + "." + tableName, joinerOnComma.join(sub));
			}
			try {
				executeSql(sql);
			}
			catch (SQLException e) {
				LOG.warn("Exception in delete sql: " + sql, e);
			}
		}
	}

	private void executeSql(String sql) throws SQLException {
		int retryTime = 0;
		while (retryTime++ < maxRetryTime) {
			connect();
			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug(sql);
				}
				statement.execute(sql);
				break;
			} catch (SQLException e) {
				//e.printStackTrace();
				closeConnection();
				if ((e.getMessage() != null
						&& e.getMessage().indexOf("duplicate key") != -1
						&& e.getMessage().indexOf("violates unique constraint") != -1)
						|| retryTime >= maxRetryTime - 1) {
					throw e;
				}
				try {
					Thread.sleep(retryWaitTime);
				} catch (Exception e1) {
					LOG.error("Thread sleep exception in AdbpgOutputFormat class", e1);
				}
			}
		}
		closeConnection();
	}

	private void executeCopy(InputStream inputStream) throws SQLException {
		if (inputStream == null){
			return;
		}
		inputStream.mark(0);
		int retryTime = 0;
		while (retryTime++ < maxRetryTime) {
			connect();
			try {
				inputStream.reset();
				inputStream.mark(0);
				CopyManager manager = new CopyManager((BaseConnection) (dataSource.getConnection().getConnection()));
				if (caseSensitive) {
					manager.copyIn("COPY \"" + targetSchema + "\".\"" + tableName + "\"( " +fieldNamesCaseSensitive +" )" + " from STDIN", inputStream);
				}
				else {
					manager.copyIn("COPY " + targetSchema + "." + tableName + "( " +fieldNames +" )" + " from STDIN", inputStream);
				}
				break;
			} catch (SQLException e) {
				closeConnection();
				if ((e.getMessage() != null
						&& e.getMessage().indexOf("duplicate key") != -1
						&& e.getMessage().indexOf("violates unique constraint") != -1)
						|| retryTime >= maxRetryTime - 1) {
					throw e;
				}
				try {
					Thread.sleep(retryWaitTime);
				} catch (Exception e1) {
					LOG.error("Thread sleep exception in AdbpgOutputFormat class", e1);
				}
			}catch (IOException e) {
				closeConnection();
				if ((e.getMessage() != null
						&& e.getMessage().indexOf("duplicate key") != -1
						&& e.getMessage().indexOf("violates unique constraint") != -1)
						|| retryTime >= maxRetryTime - 1) {
					throw new SQLException("cannot execute copy.");
				}
				try {
					Thread.sleep(retryWaitTime);
				} catch (Exception e1) {
					LOG.error("Thread sleep exception in AdbpgOutputFormat class", e1);
				}
			}
		}
		closeConnection();
	}

	private void updateRow(Row row){
		//get non primary keys
		ArrayList<String> allFields = schema.getFieldNames();
		Set<String> nonPrimaryKeys = new HashSet<String>();
		for(String field : allFields){
			if(!primaryKeys.contains(field)){
				nonPrimaryKeys.add(field);
			}
		}
		if(nonPrimaryKeys.size() == 0){
			return;
		}
		String whereStatement = StringUtils.join(
				deleteFormat(schema, row, primaryKeys, timeZone, reserveMs), " AND "
		);
		String setStatement = StringUtils.join(
				deleteFormat(schema, row, nonPrimaryKeys, timeZone, reserveMs), ","
		);

		StringBuilder sql = new StringBuilder();
		if (caseSensitive) {
			sql.append("UPDATE \"").append(targetSchema).append("\".\"").append(tableName).append("\" SET ").append(setStatement).append(" WHERE ").append(whereStatement);

		}
		else {
			sql.append("UPDATE ").append(targetSchema).append(".").append(tableName).append(" SET ").append(setStatement).append(" WHERE ").append(whereStatement);
		}
		try{
			executeSql(sql.toString());
		} catch (SQLException updateException){
			LOG.error("Exception in update sql: "+sql.toString(), updateException);
			try {
				executeSql(getInsertSQL(row));
			}
			catch (SQLException e) {

			}
		}
	}

	private void upsertRow(Row row) {
		StringBuffer sb = new StringBuffer();
		sb.append(getInsertSQL(row));
		if (caseSensitive) {
			sb.append(" on conflict(").append(primaryFieldNamesCaseSensitive).append(") ").append(" do update set (").append(nonPrimaryFieldNamesCaseSensitive).append(")=(").append(excludedNonPrimaryFieldNamesCaseSensitive).append(")");
		}
		else {
			sb.append(" on conflict(").append(primaryFieldNames).append(") ").append(" do update set (").append(nonPrimaryFieldNames).append(")=(").append(excludedNonPrimaryFieldNames).append(")");
		}
		try{
			executeSql(sb.toString());
		} catch (SQLException upsertException){
			LOG.error("Exception in upsert sql: " + sb.toString(), upsertException);
			if ("strict".equalsIgnoreCase(exceptionMode)) {
				throw new RuntimeException(upsertException);
			}
		}
	}

	private String[] writeFormat(AdbpgTableSchema schema, Row row, String timeZone, boolean reserveMs) {
		String[] output = new String[row.getArity()];
		for (int i = 0; i < row.getArity(); i++) {
			Object value = row.getField(i);
			Class<?> colType = schema.getTypes().get(i);
			try {
				if (value == null) {
					output[i] = "null";
					continue;
				} else {
					if(colType.equals(String.class)) {
						value = toField(value);
					}
					else if(colType.equals(Byte.class)) {
						value = "" + ((Byte) value).intValue();
					}
					else if(colType.equals(byte[].class)) {
						value = "'" + new String((byte[]) value) + "'";
					}
					else if(colType.equals(Short.class)){
						value = Long.parseLong("" + value);
					}
					else if(colType.equals(Integer.class)){
						value = Long.parseLong("" + value);
					}
					else if(colType.equals(Long.class)){
						value = Long.parseLong("" + value);
					}
					else if(colType.equals(Float.class)){
						value = Double.parseDouble("" + value);
					}
					else if(colType.equals(Double.class)){
						value = Double.parseDouble("" + value);
					}
					else if(colType.equals(Boolean.class)){
						value = (Boolean)value == true ? "'true'" : "'false'";
					}
					else if (Timestamp.class.equals(colType)) {
						value = "'" + DateUtil.timeStamp2String((Timestamp)value, timeZone, reserveMs) + "'";
					} else if (Date.class.equals(colType)) {
						value = "'" + DateUtil.date2String((java.sql.Date)value, timeZone) + "'";
					} else if (Time.class.equals(colType)) {
						value = "'" + DateUtil.time2String((Time)value, timeZone) + "'";
					} else if (BigDecimal.class.equals(colType)) {
						value = Double.parseDouble("" + value);
					}
					else{
						throw new IllegalArgumentException("invalid parm");
					}
				}
				output[i] = "" + value;
			} catch (Exception e) {
				throw new RuntimeException("", e);
			}
		}
		return output;
	}

	@Override
	public void close() throws IOException {
		sync();
		synchronized (Adb4PgTableSink.class) {
			if (dataSourcePool.remove(dataSourceKey) && dataSource != null && !dataSource.isClosed()){
				dataSource.close();
				dataSource = null;
			}
		}
	}

	private void connect() {
		try {
			if (statement != null) {
				statement.close();
				statement = null;
			}
			if (connection != null) {
				connection.close();
				dataSource.discardConnection(connection);
				connection = null;
			}
			connection = dataSource.getConnection();
			statement = connection.createStatement();
		} catch (SQLException e) {
			LOG.error("Init DataSource Or Get Connection Error!" , e);
			throw new RuntimeException(e);
		}
	}

	private void closeConnection() {
		try {
			if (statement != null) {
				statement.close();
				statement = null;
			}
			if (connection != null) {
				if (!connection.isClosed()) {
					connection.close();
					dataSource.discardConnection(connection);
				}
				connection = null;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			statement = null;
			connection = null;
		}
	}

	public String constructPKFilterKey(Row row) {
		if (null != primaryKeys && !primaryKeys.isEmpty()) {
			String pkFilter = "";
			for (String key : primaryKeys) {
				int index = schema.getFieldIndex(key);
				pkFilter += (null == row.getField(index) ? "null" : row.getField(index));
				pkFilter += "#";
			}
			return pkFilter;
		}
		return null;
	}

	private static String toField(Object o) {
		if (null == o) {
			return "null";
		}
		String str = o.toString();
		if (str.indexOf("'") >= 0) {
			str = str.replaceAll("'", "''");
		}
		return "'" + str + "'";
	}

	private Object[] deleteFormat(AdbpgTableSchema schema, Row row, Set<String> primaryKeys, String timeZone,
								  boolean reserveMs) {
		Object[] output = new Object[primaryKeys.size()];
		ArrayList<String> fieldNames = schema.getFieldNames();
		int keyIndex = 0;
		for (int i = 0; i < row.getArity(); i++) {
			String colName = fieldNames.get(i);
			if (!primaryKeys.contains(colName)) {
				continue;
			}

			Object value = row.getField(i);
			Class<?> colType = schema.getTypes().get(i);

			try {
				if (value == null) {
					throw new RuntimeException("get null");
				} else {
						if(colType.equals(String.class)) {
							value = toField(value);
						}
						else if(colType.equals(Byte.class)) {
							value = "" + ((Byte) value).intValue();
						}
						else if(colType.equals(byte[].class)) {
							value = "'" + new String((byte[]) value) + "'";
						}
						else if(colType.equals(Short.class)){
							value = Long.parseLong("" + value);
						}
						else if(colType.equals(Integer.class)){
							value = Long.parseLong("" + value);
						}
						else if(colType.equals(Long.class)){
							value = Long.parseLong("" + value);
						}
						else if(colType.equals(Float.class)){
							value = Double.parseDouble("" + value);
						}
						else if(colType.equals(Double.class)){
							value = Double.parseDouble("" + value);
						}
						else if(colType.equals(Boolean.class)){
							value = (Boolean)value == true ? "'true'" : "'false'";
						}
						else if (Timestamp.class.equals(colType)) {
							value = "'" + DateUtil.timeStamp2String((Timestamp)value, timeZone, reserveMs) + "'";
						} else if (Date.class.equals(colType)) {
							value = "'" + DateUtil.date2String((java.sql.Date)value, timeZone) + "'";
						} else if (Time.class.equals(colType)) {
							value = "'" + DateUtil.time2String((Time)value, timeZone) + "'";
						} else if (BigDecimal.class.equals(colType)) {
							value = Double.parseDouble("" + value);
						}
						else{
						    throw new IllegalArgumentException("invalid parm");
					    }
				}
				if (caseSensitive) {
					output[keyIndex] = "\"" + colName + "\" = " + value;
				}
				else {
					output[keyIndex] = colName + " = " + value;
				}
				keyIndex++;
			} catch (Exception e) {
				throw new RuntimeException("", e);
			}
		}

		if (keyIndex != primaryKeys.size()) {
			throw new RuntimeException("invalid parm");
		}

		return output;
	}

	private static String toCopyField(Object o) {
		if (null == o) {
			return "null";
		}
		String str = o.toString();
		if (str.indexOf("\\") >= 0) {
			str = str.replaceAll("\\\\", "\\\\\\\\");
		}
		return str;
	}

	private String[] writeCopyFormat(AdbpgTableSchema schema, Row row, String timeZone, boolean reserveMs) {
		String[] output = new String[row.getArity()];
		for (int i = 0; i < row.getArity(); i++) {
			Object value = row.getField(i);
			Class<?> colType = schema.getTypes().get(i);
			try {
				if (value == null) {
					output[i] = "null";
					continue;
				} else {
					if(colType.equals(String.class)) {
						value = toCopyField(value);
					}
					else if(colType.equals(Byte.class)) {
						value = "" + ((Byte) value).intValue();
					}
					else if(colType.equals(byte[].class)) {
						value = "'" + new String((byte[]) value) + "'";
					}
					else if(colType.equals(Short.class)){
						value = Long.parseLong("" + value);
					}
					else if(colType.equals(Integer.class)){
						value = Long.parseLong("" + value);
					}
					else if(colType.equals(Long.class)){
						value = Long.parseLong("" + value);
					}
					else if(colType.equals(Float.class)){
						value = Double.parseDouble("" + value);
					}
					else if(colType.equals(Double.class)){
						value = Double.parseDouble("" + value);
					}
					else if(colType.equals(Boolean.class)){
						value = (Boolean)value == true ? "true" : "false";
					}
					else if (Timestamp.class.equals(colType)) {
						value = "'" + DateUtil.timeStamp2String((Timestamp)value, timeZone, reserveMs) + "'";
					} else if (Date.class.equals(colType)) {
						value = "'" + DateUtil.date2String((java.sql.Date)value, timeZone) + "'";
					} else if (Time.class.equals(colType)) {
						value = "'" + DateUtil.time2String((Time)value, timeZone) + "'";
					} else if (BigDecimal.class.equals(colType)) {
						value = Double.parseDouble("" + value);
					}
					else{
						throw new IllegalArgumentException("invalid parm");
					}
				}
				output[i] = "" + value;
			} catch (Exception e) {
				throw new RuntimeException("", e);
			}
		}
		return output;
	}
}

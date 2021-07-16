package org.apache.flink.connector.jdbc.table;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

/*
 * AdbpgOutputFormat implementation
 */
public class AdbpgOutputFormat extends RichOutputFormat<RowData> {
    private transient static final Logger LOG = LoggerFactory.getLogger(AdbpgOutputFormat.class);
    private String url;
    private String tableName;
    private String userName;
    private String password;

    private Set<String> primaryKeys;
    private List<String> pkFields = new ArrayList<String>();
    private List<Integer> pkIndex = new ArrayList<>();
    String[] fieldNamesStr;
    LogicalType[] lts;
    private int fieldNum;
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
    private int retryWaitTime = 100;
    private int batchSize = 500;
    private long batchWriteTimeout = 5000;
    private long lastWriteTime = 0;
    // Use HashMap mapBuffer for data with primary key to preserve order
    private Map<String, Tuple2<Boolean, RowData>> mapBuffer = new HashMap<>();
    // Use HashMap mapBufferWithoutPk for data without primary key
    private List<Tuple2<Boolean, RowData>> mapBufferWithoutPk = new ArrayList<>();
    private String insertClause = "INSERT INTO ";
    private String timeZone = "Asia/Shanghai";
    private long inputCount = 0;

    // datasource
    private String driverClassName = "org.postgresql.Driver";
    private int connectionMaxActive = 5;
    private int connectionInitialSize = 1;
    private int connectionMinIdle = 1;
    private int maxWait = 60000;
    private int removeAbandonedTimeout = 3 * 60;
    private boolean connectionTestWhileIdle = true;

    private transient DruidDataSource dataSource = null;
    private transient ScheduledExecutorService executorService;
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
    private int verbose = 1;

    public AdbpgOutputFormat(
        String url,
        String tablename,
        String username,
        String password,
        int fieldNum,
        String[] fieldNamesStr,
        String[] keyFields,
        LogicalType[] lts,
        int retryWaitTime,
        int batchSize,
        int batchWriteTimeoutMs,
        int maxRetryTime,
        int connectionMaxActive,
        String conflictMode,
        int useCopy,
        String targetSchema,
        String exceptionMode,
        int reserveMS,
        int caseSensitive,
        int writeMode,
        int verbose
     ) {
        this.url = url;
        this.tableName = tablename;
        this.userName = username;
        this.password = password;
        this.fieldNum = fieldNum;

        Joiner joinerOnComma = Joiner.on(",").useForNull("null");
        this.fieldNamesStr = fieldNamesStr;
        this.fieldNames = joinerOnComma.join(fieldNamesStr);
        this.lts = lts;
        this.retryWaitTime = retryWaitTime;
        this.batchSize = batchSize;
        this.batchWriteTimeout = batchWriteTimeoutMs;
        this.maxRetryTime = maxRetryTime;
        this.connectionMaxActive = connectionMaxActive;
        this.conflictMode = conflictMode;
        this.useCopy = useCopy;
        this.targetSchema = targetSchema;
        this.exceptionMode = exceptionMode;
        this.reserveMs = (reserveMS != 0);
        this.caseSensitive = (caseSensitive != 0);
        this.writeMode = writeMode;
        this.verbose = verbose;

        if (keyFields != null) {
            for (int i = 0; i < keyFields.length; i++) {
                pkFields.add(keyFields[i]);
                int t = 0;
                for (; t < fieldNamesStr.length; t++) {
                    if (keyFields[i].equals(fieldNamesStr[t])) {
                        pkIndex.add(t);
                        break;
                    }
                }
                if (fieldNamesStr.length == t) {
                    throw new RuntimeException("Key cannot found in filenames.");
                }
            }
            this.primaryKeys = new HashSet<>(pkFields);
        } else {
            this.primaryKeys = null;
        }
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int a, int b) throws IOException {
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
            throw new IOException("cannot get connection for url: " + url +", userName: " + userName +", password: " + password, e);
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
            String[] nonPrimaryFieldNamesStr = new String[fieldNum - primaryKeys.size()];
            String[] primaryFieldNamesStrCaseSensitive = new String[primaryKeys.size()];
            String[] nonPrimaryFieldNamesStrCaseSensitive = new String[fieldNum - primaryKeys.size()];
            String[] excludedNonPrimaryFieldNamesStr = new String[fieldNum - primaryKeys.size()];
            String[] excludedNonPrimaryFieldNamesStrCaseSensitive = new String[fieldNum - primaryKeys.size()];
            int primaryIndex = 0;
            int excludedIndex = 0;
            for (int i = 0; i < fieldNum; i++) {
                String fileName = fieldNamesStr[i];
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

        LOG.info("sink connector created using url=" + url + ", " +
                "tableName=" + tableName + ", " +
                "userName=" + userName + ", " +
                "password=" + password + ", " +
                "maxRetries=" + maxRetryTime + ", " +
                "retryWaitTime=" + retryWaitTime + ", " +
                "batchSize=" + batchSize + ", " +
                "connectionMaxActive=" + connectionMaxActive + ", " +
                "batchWriteTimeoutMs=" + batchWriteTimeout + ", " +
                "conflictMode=" + conflictMode + ", " +
                "timeZone=" + timeZone + ", " +
                "useCopy=" + useCopy + ", " +
                "targetSchema=" + targetSchema + ", " +
                "exceptionMode=" + exceptionMode + ", " +
                "reserveMs=" + reserveMs + ", " +
                "caseSensitive=" + caseSensitive + ", " +
                "writeMode=" + writeMode + ", " +
                "fieldNum=" + fieldNum + ", " +
                "fieldNamesStr=" + Arrays.asList(fieldNamesStr).toString() + ", " +
                "keyFields=" + pkFields.toString() + ", " +
                "verbose=" + verbose + ", " +
                "lts=" + Arrays.asList(lts).toString());
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        if (null == record) {
            return;
        }
        RowData  rowData = copyRowData(record);
        inputCount++;
        if (existsPrimaryKeys) {
            synchronized (mapBuffer) {
                // Construct primary key string as map key
                String dupKey = constructDupKey(rowData, pkIndex);
                mapBuffer.put(dupKey, new Tuple2<>(true, rowData));
            }
        }
        else {
            synchronized (mapBufferWithoutPk) {
                // Add row to list when primary key does not exist
                mapBufferWithoutPk.add(new Tuple2<>(true, rowData));
            }
        }

        if (inputCount >= batchSize) {
            sync();
        } else if (System.currentTimeMillis() - this.lastWriteTime > this.batchWriteTimeout) {
            sync();
        }
    }

    private RowData copyRowData(RowData row) {
        //LOG.info("Data received:" + row.toString());
        GenericRowData rowData = new GenericRowData(row.getArity());
        for (int i = 0; i < row.getArity(); i++){
            LogicalType t = lts[i];
            if (t instanceof BooleanType) {
                rowData.setField(i, new Boolean(row.getBoolean(i)));
            }
            else if (t instanceof TimestampType){
                TimestampType datatype  = (TimestampType) t;
                TimestampData timedata = row.getTimestamp(i, datatype.getPrecision());
                rowData.setField(i, TimestampData.fromTimestamp(timedata.toTimestamp()));
            }
            else if (t instanceof VarCharType || t instanceof  CharType) {
                rowData.setField(i, row.getString(i));
            }
            else if (t instanceof FloatType) {
                rowData.setField(i, new Float(row.getFloat(i)));
            }
            else if (t instanceof DoubleType) {
                rowData.setField(i, new Double(row.getDouble(i)));
            }
            else if (t instanceof IntType) {
                rowData.setField(i, new Integer(row.getInt(i)));
            }
            else if (t instanceof SmallIntType) {
                rowData.setField(i, new Short(row.getShort(i)));
            }
            else if (t instanceof TinyIntType) {
                rowData.setField(i, new Byte(row.getByte(i)));
            }
            else if (t instanceof BigIntType) {
                rowData.setField(i, new Long(row.getLong(i)));
            }
            else if (t instanceof DateType)
            {
                rowData.setField(i, new Integer(row.getInt(i)));
            }
            else if (t instanceof DecimalType)
            {
                DecimalType data  = (DecimalType) t;
                DecimalData decimalData = row.getDecimal(i, data.getPrecision(), data.getScale());
                rowData.setField(i, decimalData.copy());
            }
            else {
                throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:huaxi.shx@alibaba-inc.com");
            }
        }
        return rowData;
    }

    private String constructDupKey(RowData row, List<Integer> pkIndex) {
        String dupKey = "";
        for (int i: pkIndex) {
            if (row.isNullAt(i)) {
                dupKey += "null#";
                continue;
            }
            LogicalType t = lts[i];
            String valuestr;
            if (t instanceof BooleanType) {
                boolean value = row.getBoolean(i);
                valuestr = value ? "'true'" : "'false'";
            }
            else if (t instanceof TimestampType){
                Timestamp value = row.getTimestamp(i, 8).toTimestamp();
                valuestr = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            }
            else if (t instanceof VarCharType || t instanceof  CharType) {
                valuestr = toField(row.getString(i).toString());
            }
            else if (t instanceof FloatType) {
                valuestr = row.getFloat(i) + "";
            }
            else if (t instanceof DoubleType) {
                valuestr = row.getDouble(i) + "";
            }
            else if (t instanceof IntType) {
                valuestr = row.getInt(i) + "";
            }
            else if (t instanceof SmallIntType) {
                valuestr = row.getShort(i) + "";
            }
            else if (t instanceof TinyIntType) {
                valuestr = row.getByte(i) + "";
            }
            else if (t instanceof BigIntType) {
                valuestr = row.getLong(i) + "";
            }
            else if (t instanceof DecimalType)
            {
                DecimalType dt = (DecimalType) t;
                valuestr = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            }
            else {
                throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:huaxi.shx@alibaba-inc.com");
            }
            dupKey += valuestr + "#";
        }
        return dupKey;
    }

    @Override
    public void close() throws IOException {
        sync();
        if (dataSource != null && !dataSource.isClosed()){
                dataSource.close();
                dataSource = null;
        }
    }
    public void sync(){
        if (1 == verbose) {
            LOG.info("start to sync " + (mapBuffer.size() + mapBufferWithoutPk.size()) + " records.");
        }
        // Synchronized mapBuffer or mapBufferWithoutPk according to existsPrimaryKeys
        synchronized (existsPrimaryKeys ? mapBuffer : mapBufferWithoutPk) {
            List<RowData> addBuffer = new ArrayList<>();
            List<RowData> deleteBuffer = new ArrayList<>();
            Collection<Tuple2<Boolean, RowData>> buffer = existsPrimaryKeys ? mapBuffer.values() : mapBufferWithoutPk;
            if (buffer.size() > 0) {
                for (Tuple2<Boolean, RowData> rowTuple2 : buffer) {
                    if (rowTuple2.f0) {
                        addBuffer.add(rowTuple2.f1);
                    } else {
                        deleteBuffer.add(rowTuple2.f1);
                    }
                }
                batchWrite(addBuffer);
            }
            // Clear mapBuffer and mapBufferWithoutPk
            mapBuffer.clear();
            mapBufferWithoutPk.clear();
            inputCount = 0;
            lastWriteTime = System.currentTimeMillis();
        }
    }

    private void batchWrite(List<RowData> rows) {
        if (null == rows || rows.size() == 0){
            return ;
        }
        try {
            if (writeMode == 1) {
                StringBuilder stringBuilder = new StringBuilder();
                for (RowData row : rows) {
                    String[] fields = writeCopyFormat(lts, row, timeZone, reserveMs);
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
                for (RowData row : rows) {
                    fields = writeFormat(lts, row, timeZone, reserveMs);
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
                for (RowData row : rows) {
                    fields = writeFormat(lts, row, timeZone, reserveMs);
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
                LOG.info("Sql generate:" + sql);
                executeSql(sql);
            }
        } catch (Exception e) {
            LOG.warn("execute sql error:", e);
            // Batch upsert demotes to single upsert when conflictMode='upsert' or writeMode=2
            if (existsPrimaryKeys &&
                    (writeMode == 2 || (
                            e.getMessage() != null
                                    && e.getMessage().indexOf("duplicate key") != -1
                                    && e.getMessage().indexOf("violates unique constraint") != -1
                                    && "upsert".equalsIgnoreCase(conflictMode)))) {
                LOG.warn("batch insert failed in upsert mode, will try to upsert msgs one by one");
                for (RowData row : rows) {
                    upsertRow(row);
                }
            }
            else {
                LOG.warn("batch insert failed, will try to insert msgs one by one");
                for (RowData row : rows) {
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
    }
    private String getInsertSQL(RowData row) {
        StringBuilder sb1 = new StringBuilder();
        String[] singleFields = writeFormat(lts, row, timeZone, reserveMs);
        if (caseSensitive) {
            sb1.append(insertClause).append("\"").append(targetSchema).append("\"").append(".").append("\"").append(tableName).append("\"").append(" (" + fieldNamesCaseSensitive + " ) values ");
        }
        else {
            sb1.append(insertClause).append(targetSchema).append(".").append(tableName).append(" (" + fieldNames + " ) values ");
        }
        sb1.append("(" + StringUtils.join(singleFields, ",") + ")");
        return sb1.toString();
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
                if ((e.getMessage() != null && e.getMessage().indexOf("duplicate key") != -1 &&
                        e.getMessage().indexOf("violates unique constraint") != -1) ||retryTime >= maxRetryTime - 1) {
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

    private void executeCopy(InputStream inputStream) throws SQLException, IOException {
        if (inputStream == null){
            return;
        }
        inputStream.mark(0);
        int retryTime = 0;
        while (retryTime++ < maxRetryTime) {
            BaseConnection baseConn = null;
            DruidPooledConnection rawConn = null;
            try {
                inputStream.reset();
                inputStream.mark(0);
                rawConn = dataSource.getConnection();
                baseConn = (BaseConnection) (rawConn.getConnection());
                CopyManager manager = new CopyManager(baseConn);
                if (caseSensitive) {
                    manager.copyIn("COPY \"" + targetSchema + "\".\"" + tableName + "\"( " +fieldNamesCaseSensitive +" )" + " from STDIN", inputStream);
                }
                else {
                    manager.copyIn("COPY " + targetSchema + "." + tableName + "( " +fieldNames +" )" + " from STDIN", inputStream);
                }
                break;
            } catch (SQLException e) {
                if ((e.getMessage() != null && e.getMessage().indexOf("duplicate key") != -1 &&
                        e.getMessage().indexOf("violates unique constraint") != -1) ||
                        retryTime >= maxRetryTime - 1) {
                    throw e;
                }
                try {
                    Thread.sleep(retryWaitTime);
                } catch (Exception e1) {
                    LOG.error("Thread sleep exception in AdbpgOutputFormat class", e1);
                }
            } catch (IOException e) {
                if ((e.getMessage() != null && e.getMessage().indexOf("duplicate key") != -1 &&
                        e.getMessage().indexOf("violates unique constraint") != -1) || retryTime >= maxRetryTime - 1) {
                    throw e;
                }
                try {
                    Thread.sleep(retryWaitTime);
                } catch (Exception e1) {
                    LOG.error("Thread sleep exception in AdbpgOutputFormat class", e1);
                }
            }
            finally {
                if (rawConn != null) {
                    if (!rawConn.isClosed()) {
                        rawConn.close();
                    }
                    if (dataSource != null) {
                        dataSource.discardConnection(rawConn);
                    }
                }
                // We should close and discard druid connection firstly, then close base connection.
                // Otherwise, druid will try to recycle the closed base connection, and print unusable log.
                if (baseConn != null) {
                    if (!baseConn.isClosed()){
                        baseConn.close();
                    }
                }
            }
        }
    }

    private void updateRow(RowData row){
        //get non primary keys
        String[] allFields = fieldNamesStr;
        Set<String> nonPrimaryKeys = new HashSet<String>();
        for(String field : allFields){
            if(!primaryKeys.contains(field)){
                nonPrimaryKeys.add(field);
            }
        }
        if(nonPrimaryKeys.size() == 0){
            return;
        }
        // Throw exception when primary filed in null
        String whereStatement = StringUtils.join(
                deleteFormat(lts, row, primaryKeys, timeZone, reserveMs, true), " AND "
        );
        // Null field is allowed in set clause
        String setStatement = StringUtils.join(
                deleteFormat(lts, row, nonPrimaryKeys, timeZone, reserveMs, false), ","
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

    private void upsertRow(RowData row) {
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

    private String[] writeFormat(LogicalType[] lts, RowData row, String timeZone, boolean reserveMs) {
        String[] output = new String[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i)) {
                output[i] = "null";
                continue;
            }
            LogicalType t = lts[i];
            if (t instanceof BooleanType) {
                    boolean value = row.getBoolean(i);
                    output[i] = value ? "'true'" : "'false'";
            }
            else if (t instanceof TimestampType){
                    TimestampType dt = (TimestampType) t;
                    Timestamp value = row.getTimestamp(i, dt.getPrecision()).toTimestamp();
                    output[i] = "'" + DateUtil.timeStamp2String((Timestamp)value, timeZone, reserveMs) + "'";
            }
            else if (t instanceof DateType){
                    int datanum = row.getInt(i);
                    DateFormat ymdhmsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    output[i] = "'" +  ymdhmsFormat.format(datanum * 1000).split(" ")[0] + "'";
            }
            else if (t instanceof VarCharType || t instanceof  CharType) {
                    output[i] = toField(row.getString(i).toString());
            }
            else if (t instanceof FloatType) {
                    output[i] = row.getFloat(i) + "";
            }
            else if (t instanceof DoubleType) {
                    output[i] = row.getDouble(i) + "";
            }
            else if (t instanceof IntType) {
                    output[i] = row.getInt(i) + "";
            }
            else if (t instanceof SmallIntType) {
                    output[i] = row.getShort(i) + "";
            }
            else if (t instanceof TinyIntType) {
                    output[i] = row.getByte(i) + "";
            }
            else if (t instanceof BigIntType) {
                    output[i] = row.getLong(i) + "";
            }
            else if (t instanceof DecimalType)
            {
                    DecimalType dt = (DecimalType) t;
                    output[i] = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            }
            else {
                    throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:huaxi.shx@alibaba-inc.com");
            }
        }
        return output;
    }

    private String[] writeCopyFormat(LogicalType[] lts, RowData row, String timeZone, boolean reserveMs) {
        String[] output = new String[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i)) {
                output[i] = "null";
                continue;
            }
            LogicalType t = lts[i];
            if (t instanceof BooleanType) {
                    boolean value = row.getBoolean(i);
                    output[i] = value ? "'true'" : "'false'";
            }
            else if (t instanceof TimestampType){
                    TimestampType dt = (TimestampType) t;
                    Timestamp value = row.getTimestamp(i, dt.getPrecision()).toTimestamp();
                    output[i] = "'" + DateUtil.timeStamp2String((Timestamp)value, timeZone, reserveMs) + "'";
            }
            else if (t instanceof DateType){
                    int datanum = row.getInt(i);
                    DateFormat ymdhmsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    output[i] = "'" +  ymdhmsFormat.format(datanum * 1000).split(" ")[0] + "'";
            }
            else if (t instanceof VarCharType || t instanceof  CharType) {
                    output[i] = toCopyField(row.getString(i).toString());
            }
            else if (t instanceof FloatType) {
                    output[i] = row.getFloat(i) + "";
            }
            else if (t instanceof DoubleType) {
                    output[i] = row.getDouble(i) + "";
            }
            else if (t instanceof IntType) {
                    output[i] = row.getInt(i) + "";
            }
            else if (t instanceof SmallIntType) {
                    output[i] = row.getShort(i) + "";
            }
            else if (t instanceof TinyIntType) {
                    output[i] = row.getByte(i) + "";
            }
            else if (t instanceof BigIntType) {
                    output[i] = row.getLong(i) + "";
            }
            else if (t instanceof DecimalType)
            {
                DecimalType dt = (DecimalType) t;
                output[i] = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            }
            else {
                    throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:huaxi.shx@alibaba-inc.com");
            }
        }
        return output;
    }

    // Use strict to control whether to allow primary field is null or not
    private Object[] deleteFormat(LogicalType[] lts, RowData row, Set<String> primaryKeys, String timeZone,
                                  boolean reserveMs, boolean strict) {
        Object[] output = new Object[primaryKeys.size()];
        String[] fieldNames = fieldNamesStr;
        int keyIndex = 0;
        for (int i = 0; i < row.getArity(); i++) {
            String colName = fieldNames[i];
            if (!primaryKeys.contains(colName)) {
                continue;
            }
            if (row.isNullAt(i)) {
                throw new RuntimeException("primary fileds cannot be null.");
            }
            LogicalType t = lts[i];
            String valuestr;
            if (t instanceof BooleanType) {
                    boolean value = row.getBoolean(i);
                    valuestr = value ? "'true'" : "'false'";
            }
            else if (t instanceof TimestampType){
                    TimestampType dt = (TimestampType) t;
                    Timestamp value = row.getTimestamp(i, dt.getPrecision()).toTimestamp();
                    valuestr = "'" + DateUtil.timeStamp2String((Timestamp)value, timeZone, reserveMs) + "'";
            }
            else if (t instanceof DateType){
                    int datanum = row.getInt(i);
                    DateFormat ymdhmsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    valuestr = "'" +  ymdhmsFormat.format(datanum * 1000).split(" ")[0] + "'";
            }
            else if (t instanceof VarCharType || t instanceof  CharType) {
                    valuestr = toField(row.getString(i).toString());
            }
            else if (t instanceof FloatType) {
                    valuestr = row.getFloat(i) + "";
            }
            else if (t instanceof DoubleType) {
                    valuestr = row.getDouble(i) + "";
            }
            else if (t instanceof IntType) {
                    valuestr = row.getInt(i) + "";
            }
            else if (t instanceof SmallIntType) {
                    valuestr = row.getShort(i) + "";
            }
            else if (t instanceof TinyIntType) {
                    valuestr = row.getByte(i) + "";
            }
            else if (t instanceof BigIntType) {
                    valuestr = row.getLong(i) + "";
            }
            else if (t instanceof DecimalType)
            {
                    DecimalType dt = (DecimalType) t;
                    valuestr = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            }
            else {
                    throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:huaxi.shx@alibaba-inc.com");
            }
            if (caseSensitive) {
                    output[keyIndex] = "\"" + colName + "\" = " + valuestr;
            }
            else {
                    output[keyIndex] = colName  + " = " + valuestr;
            }
            keyIndex++;
        }

        if (keyIndex != primaryKeys.size()) {
            throw new RuntimeException("primary key invalid.");
        }
        return output;
    }
}

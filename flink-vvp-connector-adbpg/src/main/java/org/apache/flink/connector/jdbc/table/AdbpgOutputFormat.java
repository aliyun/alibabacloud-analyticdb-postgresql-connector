package org.apache.flink.connector.jdbc.table;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.table.metric.MetricUtils;
import org.apache.flink.connector.jdbc.table.metric.SimpleGauge;
import org.apache.flink.connector.jdbc.table.utils.AdbpgOptions;
import org.apache.flink.connector.jdbc.table.utils.DateUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.BATCH_SIZE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.BATCH_WRITE_TIMEOUT_MS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CASE_SENSITIVE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONFLICT_MODE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONNECTOR_TYPE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.EXCEPTION_MODE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.MAX_RETRY_TIMES;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.RESERVEMS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.TARGET_SCHEMA;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.URL;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.USE_COPY;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.VERBOSE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.WRITE_MODE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.RETRY_WAIT_TIME;

/**
 * ADBPG sink Implementation.
 * create AdbpgOutputFormat for detail implementation
 */
public class AdbpgOutputFormat extends RichOutputFormat<RowData> implements CleanupWhenUnsuccessful {

    private final ReadableConfig config;
    private static final transient Logger LOG = LoggerFactory.getLogger(AdbpgOutputFormat.class);
    private static volatile boolean existsPrimaryKeys = false;
    String[] fieldNamesStr;
    LogicalType[] lts;
    private String url;
    private String tableName;
    private String userName;
    private String password;
    private Set<String> primaryKeys;
    private List<String> pkFields = new ArrayList<String>();
    private List<Integer> pkIndex = new ArrayList<>();
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
    private int maxRetryTime;
    private int retryWaitTime;
    private int batchSize;
    private long batchWriteTimeout;
    private long lastWriteTime = 0;
    // Use HashMap mapBuffer for data with primary key to preserve order
    private Map<String, RowData> mapBuffer = new HashMap<>();
    // Use HashMap mapBufferWithoutPk for data without primary key
    private List<RowData> mapBufferWithoutPk = new ArrayList<>();
    private String insertClause = "INSERT INTO ";
    private String timeZone = "Asia/Shanghai";
    private final String DELETE_WITH_KEY_SQL_TPL = "DELETE FROM %s WHERE %s ";
    private long inputCount = 0;
    // datasource
    private transient DruidDataSource dataSource = null;
    private transient ScheduledExecutorService executorService;
    private transient Connection connection;
    private transient Statement statement;
    // connector parameter
    private boolean reserveMs;
    private String conflictMode;
    private int useCopy;
    private String targetSchema;
    private String exceptionMode;
    private boolean caseSensitive;
    private int writeMode;
    private int verbose;

    //metric
    private Meter outTps;
    private Meter outBps;
    private Counter sinkSkipCounter;
    private SimpleGauge latencyGauge;
    private transient Counter deleteCounter;

    protected final RowDataSerializer rowDataSerializer;

    public AdbpgOutputFormat(
            int fieldNum,
            String[] fieldNamesStr,
            String[] keyFields,
            LogicalType[] lts,
            ReadableConfig config
    ) {
        this.config = config;
        this.url = config.get(URL);
        this.tableName = config.get(TABLE_NAME);
        this.userName = config.get(USERNAME);
        this.password = config.get(PASSWORD);
        this.batchWriteTimeout = config.get(BATCH_WRITE_TIMEOUT_MS);
        this.reserveMs = AdbpgOptions.isConfigOptionTrue(config, RESERVEMS);
        this.conflictMode = config.get(CONFLICT_MODE);
        this.useCopy = config.get(USE_COPY);
        this.maxRetryTime = config.get(MAX_RETRY_TIMES);
        this.batchSize = config.get(BATCH_SIZE);
        this.targetSchema = config.get(TARGET_SCHEMA);
        this.exceptionMode = config.get(EXCEPTION_MODE);
        this.caseSensitive = AdbpgOptions.isConfigOptionTrue(config, CASE_SENSITIVE);
        this.writeMode = config.get(WRITE_MODE);
        this.verbose = config.get(VERBOSE);
        this.retryWaitTime = config.get(RETRY_WAIT_TIME);
        this.fieldNum = fieldNum;
        this.lts = lts;
        this.rowDataSerializer = new RowDataSerializer(this.lts);
        Joiner joinerOnComma = Joiner.on(",").useForNull("null");
        this.fieldNamesStr = fieldNamesStr;
        this.fieldNames = joinerOnComma.join(fieldNamesStr);

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

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        dataSource = AdbpgOptions.buildDataSourceFromOptions(config);
        try {
            dataSource.init();
        } catch (SQLException e) {
            LOG.error("Init DataSource Or Get Connection Error!", e);
            throw new IOException("cannot get connection for url: " + url + ", userName: " + userName + ", password: " + password, e);
        }
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            existsPrimaryKeys = false;
            if (2 == this.writeMode) {
                throw new RuntimeException("primary key cannot be empty when setting write mode to 2:upsert.");
            }
        } else {
            existsPrimaryKeys = true;
            Joiner joinerOnComma = Joiner.on(",").useForNull("null");
            String[] primaryFieldNamesStr = new String[primaryKeys.size()];
            String[] nonPrimaryFieldNamesStr = new String[fieldNum - primaryKeys.size()];
            String[] primaryFieldNamesStrCaseSensitive = new String[primaryKeys.size()];
            String[] nonPrimaryFieldNamesStrCaseSensitive = new String[fieldNum - primaryKeys.size()];
            String[] excludedNonPrimaryFieldNamesStr = new String[fieldNum - primaryKeys.size()];
            String[] excludedNonPrimaryFieldNamesStrCaseSensitive = new String[fieldNum - primaryKeys.size()];
            String[] fieldNamesStrCaseSensitive = new String[this.fieldNum];
            int primaryIndex = 0;
            int excludedIndex = 0;
            for (int i = 0; i < fieldNum; i++) {
                String fileName = fieldNamesStr[i];
                fieldNamesStrCaseSensitive[i] = "\"" + fileName + "\"";
                if (primaryKeys.contains(fileName)) {
                    primaryFieldNamesStr[primaryIndex] = fileName;
                    primaryFieldNamesStrCaseSensitive[primaryIndex++] = "\"" + fileName + "\"";
                } else {
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
            this.fieldNamesCaseSensitive = joinerOnComma.join((Object[]) fieldNamesStrCaseSensitive);
        }
        executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("adbpg-flusher-%d").daemon(true).build());
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (System.currentTimeMillis() - lastWriteTime >= batchWriteTimeout) {
                        sync();
                    }
                } catch (Exception e) {
                    LOG.error("flush buffer to ADBPG failed", e);
                }
            }
        }, batchWriteTimeout, batchWriteTimeout, TimeUnit.MILLISECONDS);

        outTps = MetricUtils.registerNumRecordsOutRate(getRuntimeContext());
        outBps = MetricUtils.registerNumBytesOutRate(getRuntimeContext(), CONNECTOR_TYPE);
        latencyGauge = MetricUtils.registerCurrentSendTime(getRuntimeContext());
        sinkSkipCounter = MetricUtils.registerNumRecordsOutErrors(getRuntimeContext());
        deleteCounter = MetricUtils.registerSinkDeleteCounter(getRuntimeContext());
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        if (null == record) {
            return;
        }
        RowData rowData = rowDataSerializer.copy(record);
        inputCount++;
        if (existsPrimaryKeys) {
            synchronized (mapBuffer) {
                // Construct primary key string as map key
                String dupKey = constructDupKey(rowData, pkIndex);
                mapBuffer.put(dupKey, rowData);
            }
        } else {
            synchronized (mapBufferWithoutPk) {
                // Add row to list when primary key does not exist
                mapBufferWithoutPk.add(rowData);
            }
        }

        if (inputCount >= batchSize) {
            sync();
        } else if (System.currentTimeMillis() - this.lastWriteTime > this.batchWriteTimeout) {
            sync();
        }
    }

    private String constructDupKey(RowData row, List<Integer> pkIndex) {
        String dupKey = "";
        for (int i : pkIndex) {
            if (row.isNullAt(i)) {
                dupKey += "null#";
                continue;
            }
            LogicalType t = lts[i];
            String valuestr;
            if (t instanceof BooleanType) {
                boolean value = row.getBoolean(i);
                valuestr = value ? "'true'" : "'false'";
            } else if (t instanceof TimestampType) {
                Timestamp value = row.getTimestamp(i, 8).toTimestamp();
                valuestr = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            } else if (t instanceof VarCharType || t instanceof CharType) {
                valuestr = toField(row.getString(i).toString());
            } else if (t instanceof FloatType) {
                valuestr = row.getFloat(i) + "";
            } else if (t instanceof DoubleType) {
                valuestr = row.getDouble(i) + "";
            } else if (t instanceof IntType) {
                valuestr = row.getInt(i) + "";
            } else if (t instanceof SmallIntType) {
                valuestr = row.getShort(i) + "";
            } else if (t instanceof TinyIntType) {
                valuestr = row.getByte(i) + "";
            } else if (t instanceof BigIntType) {
                valuestr = row.getLong(i) + "";
            } else if (t instanceof DecimalType) {
                DecimalType dt = (DecimalType) t;
                valuestr = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            } else {
                throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:wangheyang.why@alibaba-inc.com");
            }
            dupKey += valuestr + "#";
        }
        return dupKey;
    }

    @Override
    public void close() throws IOException {
        sync();
        LOG.info("close datasource");
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            dataSource = null;
        }
    }

    public void closeWithoutSync() throws IOException {
        LOG.info("close datasource without sync");
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            dataSource = null;
        }
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        LOG.info("clean up invoked due to unsuccessful execution");
        closeWithoutSync();
    }

    public void sync() {
        if (1 == verbose) {
            LOG.info("start to sync " + (mapBuffer.size() + mapBufferWithoutPk.size()) + " records.");
        }
        // Synchronized mapBuffer or mapBufferWithoutPk according to existsPrimaryKeys
        synchronized (existsPrimaryKeys ? mapBuffer : mapBufferWithoutPk) {
            List<RowData> addBuffer = new ArrayList<>();
            List<RowData> deleteBuffer = new ArrayList<>();
            Collection<RowData> buffer = existsPrimaryKeys ? mapBuffer.values() : mapBufferWithoutPk;
            if (buffer.size() > 0) {
                for (RowData row : buffer) {
                    switch (row.getRowKind()) {
                        case INSERT:
                        case UPDATE_AFTER:
                            addBuffer.add(row);
                            break;
                        case DELETE:
                        case UPDATE_BEFORE:
                            deleteBuffer.add(row);
                            break;
                        default:
                            throw new RuntimeException(
                                    "Not supported row kind " + row.getRowKind());
                    }
                }
                batchWrite(addBuffer);
                if (existsPrimaryKeys) {
                    batchDelete(deleteBuffer);
                } else {
                    batchDeleteWithoutPk(deleteBuffer);
                }
            }
            if (1 == verbose) {
                LOG.info("finished syncing " + (mapBuffer.size() + mapBufferWithoutPk.size()) + " records.");
            }
            // Clear mapBuffer and mapBufferWithoutPk
            mapBuffer.clear();
            mapBufferWithoutPk.clear();
            inputCount = 0;
            lastWriteTime = System.currentTimeMillis();
        }
    }

    private void reportMetric(List<RowData> rows, long start, long end, long bps) {
        if (latencyGauge != null) {
            latencyGauge.report(end - start, rows.size());
        }
        if (outTps != null) {
            outTps.markEvent(rows.size());
        }
        if (outBps != null) {
            outBps.markEvent(bps);
            LOG.info("reportMetric with bps =  " + bps);
        }
    }

    private void batchDeleteWithoutPk(List<RowData> buffers) {
        for (RowData row : buffers) {
            Joiner joinerOnComma = Joiner.on(" AND ").useForNull("null");
            String[] fields = writeFormat(lts, row, timeZone, reserveMs);
            List<String> sub = new ArrayList<>();
            for (int i = 0; i < row.getArity(); i++) {
                if (caseSensitive) {
                    if (row.isNullAt(i)) {
                        sub.add(" \"" + fieldNamesStr[i] + "\" is null ");
                    } else {
                        sub.add(" \"" + fieldNamesStr[i] + "\" = " + fields[i]);
                    }
                } else {
                    if (row.isNullAt(i)) {
                        sub.add(" " + fieldNamesStr[i] + " is null ");
                    } else {
                        sub.add(" " + fieldNamesStr[i] + " = " + fields[i]);
                    }
                }
            }
            String sql = null;
            if (caseSensitive) {
                sql = String.format(DELETE_WITH_KEY_SQL_TPL, "\"" + targetSchema + "\".\"" + tableName + "\"", joinerOnComma.join(sub));
            } else {
                sql = String.format(DELETE_WITH_KEY_SQL_TPL, targetSchema + "." + tableName, joinerOnComma.join(sub));
            }
            try {
                executeSql(sql);
            } catch (SQLException e) {
                LOG.warn("Exception in delete sql: " + sql, e);
            }
        }
    }

    private void batchDelete(List<RowData> buffers) {
        for (RowData row : buffers) {
            StringBuilder sb = new StringBuilder();
            if (caseSensitive) {
                sb.append("DELETE FROM ").append("\"").append(targetSchema).append("\".\"").append(tableName).append("\" where ");
            } else {
                sb.append("DELETE FROM ").append(targetSchema).append(".").append(tableName).append(" where ");
            }
            Object[] output = deleteFormat(lts, row, new HashSet<>(primaryKeys), timeZone, reserveMs, true);
            sb.append(org.apache.commons.lang3.StringUtils.join(output, " and "));
            String sql = sb.toString();
            try {
                executeSql(sql);
            } catch (SQLException e) {
                LOG.warn("Exception in delete sql: " + sql, e);
            }
        }
    }

    private void batchWrite(List<RowData> rows) {
        long bps = 0;
        if (null == rows || rows.size() == 0) {
            return;
        }
        try {
            long start = System.currentTimeMillis();
            if (writeMode == 1) {
                StringBuilder stringBuilder = new StringBuilder();
                for (RowData row : rows) {
                    String[] fields = writeCopyFormat(lts, row, timeZone, reserveMs);
                    for (int i = 0; i < fields.length; i++) {
                        stringBuilder.append(fields[i]);
                        stringBuilder.append(i == fields.length - 1 ? "\r\n" : "\t");
                    }
                }
                byte[] data = stringBuilder.toString().getBytes(Charsets.UTF_8);
                bps = executeCopy(data);
            } else if (writeMode == 2) {
                List<String> valueList = new ArrayList<String>();
                String[] fields;
                for (RowData row : rows) {
                    fields = writeFormat(lts, row, timeZone, reserveMs);
                    valueList.add("(" + StringUtils.join(fields, ",") + ")");
                }

                StringBuilder sb = new StringBuilder();
                if (caseSensitive) {
                    sb.append(insertClause).append("\"").append(targetSchema).append("\"").append(".").append("\"")
                            .append(tableName).append("\"").append(" (" + fieldNamesCaseSensitive + " ) values ");
                } else {
                    sb.append(insertClause).append(targetSchema).append(".").append(tableName)
                            .append(" (" + fieldNames + " ) values ");
                }

                sb.append(StringUtils.join(valueList, ","));
                if (caseSensitive) {
                    sb.append(" on conflict(").append(primaryFieldNamesCaseSensitive).append(") ")
                            .append(" do update set (").append(nonPrimaryFieldNamesCaseSensitive)
                            .append(")=(").append(excludedNonPrimaryFieldNamesCaseSensitive).append(")");
                } else {
                    sb.append(" on conflict(").append(primaryFieldNames).append(") ").append(" do update set (")
                            .append(nonPrimaryFieldNames).append(")=(").append(excludedNonPrimaryFieldNames)
                            .append(")");
                }
                executeSql(sb.toString());
                bps = sb.toString().getBytes().length;
            } else {
                List<String> valueList = new ArrayList<String>();
                String[] fields;
                for (RowData row : rows) {
                    fields = writeFormat(lts, row, timeZone, reserveMs);
                    valueList.add("(" + StringUtils.join(fields, ",") + ")");
                }
                StringBuilder sb = new StringBuilder();
                if (caseSensitive) {
                    sb.append(insertClause).append("\"").append(targetSchema).append("\"").append(".").append("\"")
                            .append(tableName).append("\"").append(" (" + fieldNamesCaseSensitive + " ) values ");
                } else {
                    sb.append(insertClause).append(targetSchema).append(".").append(tableName).append(" (" + fieldNames + " ) values ");
                }
                String sql = sb.toString() + StringUtils.join(valueList, ",");
                LOG.info("Sql generate:" + sql);
                executeSql(sql);
                bps = sql.getBytes().length;
            }
            long end = System.currentTimeMillis();
            reportMetric(rows, start, end, bps);
        } catch (Exception e) {
            long start = System.currentTimeMillis();
            LOG.warn("execute sql error:", e);
            // Batch upsert demotes to single upsert when conflictMode='upsert' or writeMode=2
            if (existsPrimaryKeys
                    && (writeMode == 2 || (
                    e.getMessage() != null
                            && e.getMessage().indexOf("duplicate key") != -1
                            && e.getMessage().indexOf("violates unique constraint") != -1
                            && "upsert".equalsIgnoreCase(conflictMode)))) {
                LOG.warn("batch insert failed in upsert mode, will try to upsert msgs one by one");
                for (RowData row : rows) {
                    bps = bps + upsertRow(row);
                }
            } else {
                LOG.warn("batch insert failed, will try to insert msgs one by one");
                // close connection to prevent zombie connection
                closeConnection();
                for (RowData row : rows) {
                    String insertSQL = getInsertSQL(row);
                    try {
                        bps = bps + executeSql(insertSQL);
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
                                bps = bps + updateRow(row);
                            } else if ("upsert".equalsIgnoreCase(conflictMode) || (2 == writeMode)) {
                                bps = bps + upsertRow(row);
                            }
                        } else {
                            if ("strict".equalsIgnoreCase(exceptionMode)) {
                                throw new RuntimeException(insertException);
                            }
                        }
                    }
                }
            }
            long end = System.currentTimeMillis();
            reportMetric(rows, start, end, bps);
        }
    }

    private String getInsertSQL(RowData row) {
        StringBuilder sb1 = new StringBuilder();
        String[] singleFields = writeFormat(lts, row, timeZone, reserveMs);
        if (caseSensitive) {
            sb1.append(insertClause).append("\"").append(targetSchema).append("\"").append(".").append("\"").append(tableName).append("\"").append(" (" + fieldNamesCaseSensitive + " ) values ");
        } else {
            sb1.append(insertClause).append(targetSchema).append(".").append(tableName).append(" (" + fieldNames + " ) values ");
        }
        sb1.append("(" + StringUtils.join(singleFields, ",") + ")");
        return sb1.toString();
    }

    private long executeSql(String sql) throws SQLException {
        int retryTime = 0;
        long bps = 0;
        while (retryTime++ < maxRetryTime) {
            connect();
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(sql);
                }
                statement.execute(sql);
                bps = sql.getBytes().length;
                break;
            } catch (SQLException e) {
                //e.printStackTrace();
                closeConnection();
                if ((e.getMessage() != null && e.getMessage().indexOf("duplicate key") != -1
                        && e.getMessage().indexOf("violates unique constraint") != -1) || retryTime >= maxRetryTime - 1) {
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
        return bps;
    }

    private long executeCopy(byte[] data) throws SQLException, IOException {
        long bps = data.length;
        InputStream inputStream = new ByteArrayInputStream(data);
        if (inputStream == null) {
            return 0;
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
                StringBuffer sb = new StringBuffer();
                if (caseSensitive) {
                    sb.append("COPY \"" + targetSchema + "\".\"" + tableName + "\"( " + fieldNamesCaseSensitive + " )" + " from STDIN");
                } else {
                    sb.append("COPY " + targetSchema + "." + tableName + "( " + fieldNames + " )" + " from STDIN");
                }
                if ("ignore".equalsIgnoreCase(conflictMode)) {
                    sb.append(" DO on conflict DO nothing");
                } else {
                    sb.append(" DO on conflict DO update");
                }
                manager.copyIn(sb.toString(), inputStream);
                break;
            } catch (SQLException e) {
                if ((e.getMessage() != null && e.getMessage().indexOf("duplicate key") != -1
                        && e.getMessage().indexOf("violates unique constraint") != -1)
                        || retryTime >= maxRetryTime - 1) {
                    throw e;
                }
                try {
                    Thread.sleep(retryWaitTime);
                } catch (Exception e1) {
                    LOG.error("Thread sleep exception in AdbpgOutputFormat class", e1);
                }
            } catch (IOException e) {
                if ((e.getMessage() != null && e.getMessage().indexOf("duplicate key") != -1
                        && e.getMessage().indexOf("violates unique constraint") != -1) || retryTime >= maxRetryTime - 1) {
                    throw e;
                }
                try {
                    Thread.sleep(retryWaitTime);
                } catch (Exception e1) {
                    LOG.error("Thread sleep exception in AdbpgOutputFormat class", e1);
                }
            } finally {
                LOG.info("Close connection within execute copy. ");
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
                    if (!baseConn.isClosed()) {
                        baseConn.close();
                    }
                }
            }
        }
        return bps * retryTime;
    }

    private long updateRow(RowData row) {
        long bps = 0;
        //get non primary keys
        String sqlStr;
        String[] allFields = fieldNamesStr;
        Set<String> nonPrimaryKeys = new HashSet<String>();
        for (String field : allFields) {
            if (!primaryKeys.contains(field)) {
                nonPrimaryKeys.add(field);
            }
        }
        if (nonPrimaryKeys.size() == 0) {
            return bps;
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
            sql.append("UPDATE \"").append(targetSchema).append("\".\"").append(tableName).append("\" SET ")
                    .append(setStatement).append(" WHERE ").append(whereStatement);
        } else {
            sql.append("UPDATE ").append(targetSchema).append(".").append(tableName).append(" SET ")
                    .append(setStatement).append(" WHERE ").append(whereStatement);
        }
        try {
            sqlStr = sql.toString();
            executeSql(sqlStr);
            bps = sqlStr.getBytes().length;
        } catch (SQLException updateException) {
            LOG.error("Exception in update sql: " + sql.toString(), updateException);
            try {
                sqlStr = getInsertSQL(row);
                executeSql(sqlStr);
                bps = sqlStr.getBytes().length;
            } catch (SQLException e) {
            }
        }
        return bps;
    }

    private long upsertRow(RowData row) {
        long bps = 0;
        StringBuffer sb = new StringBuffer();
        sb.append(getInsertSQL(row));
        if (caseSensitive) {
            sb.append(" on conflict(").append(primaryFieldNamesCaseSensitive).append(") ").append(" do update set (")
                    .append(nonPrimaryFieldNamesCaseSensitive).append(")=(")
                    .append(excludedNonPrimaryFieldNamesCaseSensitive).append(")");
        } else {
            sb.append(" on conflict(").append(primaryFieldNames).append(") ")
                    .append(" do update set (").append(nonPrimaryFieldNames).append(")=(")
                    .append(excludedNonPrimaryFieldNames).append(")");
        }
        try {
            executeSql(sb.toString());
            bps = sb.toString().getBytes().length;
        } catch (SQLException upsertException) {
            LOG.error("Exception in upsert sql: " + sb.toString(), upsertException);
            if ("strict".equalsIgnoreCase(exceptionMode)) {
                throw new RuntimeException(upsertException);
            }
        }
        return bps;
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
            LOG.error("Init DataSource Or Get Connection Error!", e);
            throw new RuntimeException(e);
        }
    }

    private void closeConnection() {
        try {
            LOG.info("Close connection ");
            if (statement != null) {
                statement.close();
                statement = null;
                LOG.info("statement closed ");
            }
            if (connection != null) {
                if (!connection.isClosed()) {
                    connection.close();
                    dataSource.discardConnection(connection);
                    LOG.info("connection closed and discarded ");
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
            } else if (t instanceof TimestampType) {
                TimestampType dt = (TimestampType) t;
                Timestamp value = row.getTimestamp(i, dt.getPrecision()).toTimestamp();
                output[i] = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            } else if (t instanceof DateType) {
                int datanum = row.getInt(i);
                DateFormat ymdhmsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                output[i] = "'" + ymdhmsFormat.format(datanum * 24 * 60 * 60 * 1000L).split(" ")[0] + "'";
            } else if (t instanceof VarCharType || t instanceof CharType) {
                output[i] = toField(row.getString(i).toString());
            } else if (t instanceof FloatType) {
                output[i] = row.getFloat(i) + "";
            } else if (t instanceof DoubleType) {
                output[i] = row.getDouble(i) + "";
            } else if (t instanceof IntType) {
                output[i] = row.getInt(i) + "";
            } else if (t instanceof SmallIntType) {
                output[i] = row.getShort(i) + "";
            } else if (t instanceof TinyIntType) {
                output[i] = row.getByte(i) + "";
            } else if (t instanceof BigIntType) {
                output[i] = row.getLong(i) + "";
            } else if (t instanceof TimeType) {
                Timestamp value = row.getTimestamp(i, ((TimeType) t).getPrecision()).toTimestamp();
                output[i] = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            } else if (t instanceof DecimalType) {
                DecimalType dt = (DecimalType) t;
                output[i] = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            } else {
                throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:wangheyang.why@alibaba-inc.com");
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
            } else if (t instanceof TimestampType) {
                TimestampType dt = (TimestampType) t;
                Timestamp value = row.getTimestamp(i, dt.getPrecision()).toTimestamp();
                output[i] = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            } else if (t instanceof DateType) {
                int datanum = row.getInt(i);
                DateFormat ymdhmsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                output[i] = "'" + ymdhmsFormat.format(datanum * 24 * 60 * 60 * 1000L).split(" ")[0] + "'";
            } else if (t instanceof VarCharType || t instanceof CharType) {
                output[i] = toCopyField(row.getString(i).toString());
            } else if (t instanceof FloatType) {
                output[i] = row.getFloat(i) + "";
            } else if (t instanceof DoubleType) {
                output[i] = row.getDouble(i) + "";
            } else if (t instanceof IntType) {
                output[i] = row.getInt(i) + "";
            } else if (t instanceof SmallIntType) {
                output[i] = row.getShort(i) + "";
            } else if (t instanceof TinyIntType) {
                output[i] = row.getByte(i) + "";
            } else if (t instanceof BigIntType) {
                output[i] = row.getLong(i) + "";
            } else if (t instanceof DecimalType) {
                DecimalType dt = (DecimalType) t;
                output[i] = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            } else if (t instanceof TimeType) {
                Timestamp value = row.getTimestamp(i, ((TimeType) t).getPrecision()).toTimestamp();
                output[i] = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            } else {
                throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:wangheyang.why@alibaba-inc.com");
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
            } else if (t instanceof TimestampType) {
                TimestampType dt = (TimestampType) t;
                Timestamp value = row.getTimestamp(i, dt.getPrecision()).toTimestamp();
                valuestr = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            } else if (t instanceof DateType) {
                int datanum = row.getInt(i);
                DateFormat ymdhmsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                valuestr = "'" + ymdhmsFormat.format(datanum * 24 * 60 * 60 * 1000L).split(" ")[0] + "'";
            } else if (t instanceof VarCharType || t instanceof CharType) {
                valuestr = toField(row.getString(i).toString());
            } else if (t instanceof FloatType) {
                valuestr = row.getFloat(i) + "";
            } else if (t instanceof DoubleType) {
                valuestr = row.getDouble(i) + "";
            } else if (t instanceof IntType) {
                valuestr = row.getInt(i) + "";
            } else if (t instanceof SmallIntType) {
                valuestr = row.getShort(i) + "";
            } else if (t instanceof TinyIntType) {
                valuestr = row.getByte(i) + "";
            } else if (t instanceof BigIntType) {
                valuestr = row.getLong(i) + "";
            } else if (t instanceof DecimalType) {
                DecimalType dt = (DecimalType) t;
                valuestr = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
            } else if (t instanceof TimeType) {
                Timestamp value = row.getTimestamp(i, ((TimeType) t).getPrecision()).toTimestamp();
                valuestr = "'" + DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs) + "'";
            } else {
                throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:wangheyang.why@alibaba-inc.com");
            }
            if (caseSensitive) {
                output[keyIndex] = "\"" + colName + "\" = " + valuestr;
            } else {
                output[keyIndex] = colName + " = " + valuestr;
            }
            keyIndex++;
        }

        if (keyIndex != primaryKeys.size()) {
            throw new RuntimeException("primary key invalid.");
        }
        return output;
    }
}

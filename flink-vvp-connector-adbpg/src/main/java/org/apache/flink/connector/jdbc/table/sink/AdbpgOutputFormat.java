package org.apache.flink.connector.jdbc.table.sink;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.table.metric.MetricUtils;
import org.apache.flink.connector.jdbc.table.metric.SimpleGauge;
import org.apache.flink.connector.jdbc.table.sink.api.*;
import org.apache.flink.connector.jdbc.table.utils.*;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.guava30.com.google.common.base.Joiner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.*;

/**
 * ADBPG sink Implementation.
 * create AdbpgOutputFormat for detail implementation
 */
public class AdbpgOutputFormat extends RichOutputFormat<RowData> implements CleanupWhenUnsuccessful, Syncable {

    private static final Logger LOG = LoggerFactory.getLogger(AdbpgOutputFormat.class);
    private boolean existsPrimaryKeys;
    protected final RowDataSerializer rowDataSerializer;
    private final ReadableConfig config;
    private final String DELETE_WITH_KEY_SQL_TPL = "DELETE FROM %s WHERE %s ";
    private final JdbcRowConverter rowConverter;
    private final JdbcRowConverter upsertConverter;
    private final StreamingServerRowConverter streamingServerRowConverter;
    private final StringFormatRowConverter copyModeRowConverter;
    private final AdbpgDialect adbpgDialect;
    String[] fieldNamesStrs;
    LogicalType[] logicalTypes;
    private String url;
    private String adbssHost;
    private int adbssPort;
    private String tableName;
    private String userName;
    private String password;
    private String hostname = null;
    private int port = 0;
    private String database = null;
    private Set<String> primaryKeys;
    private List<String> pkFields = new ArrayList<String>();
    private List<Integer> pkIndex = new ArrayList<>();
    private LogicalType[] pkTypes;
    private LogicalType[] updateStatementFieldTypes;
    private int fieldNum;
    private int[] updateStatementFieldIndices;
    private String fieldNames = null;
    private String fieldNamesCaseSensitive = null;
    private String primaryFieldNames = null;
    private String nonPrimaryFieldNames = null;
    private String primaryFieldNamesCaseSensitive = null;
    private String nonPrimaryFieldNamesCaseSensitive = null;
    private String excludedNonPrimaryFieldNames = null;
    private String excludedNonPrimaryFieldNamesCaseSensitive = null;
    private String[] primaryFieldNamesStr = null;
    private String[] nonPrimaryFieldNamesStr = null;
    private String[] excludedNonPrimaryFieldNamesStr = null;
    // write policy
    private int maxRetryTime;
    private int retryWaitTime;
    private int batchSize;
    private long batchWriteTimeout;
    private long lastWriteTime = 0;
    // Use HashMap mapBuffer for data with primary key to preserve order
    private final Map<String, RowData> mapBufferWithPk = new HashMap<>();
    // Use HashMap mapBufferWithoutPk for data without primary key
    private final List<RowData> mapBufferWithoutPk = new ArrayList<>();
    private String insertClause = "INSERT INTO ";
    private String timeZone = "Asia/Shanghai";
    private long inputCount = 0;
    // version after which support upsert for partitioned table
    private long adbpg_version = 6360;
    private boolean support_upsert = true;
    // datasource
    private transient DruidDataSource dataSource = null;
    private transient ScheduledExecutorService executorService;
    private transient DruidPooledConnection rawConn;
    private transient BaseConnection baseConn;
    private transient CopyManager copyManager;
    private transient Connection connection;
    private transient Statement statement;
    private JdbcRowConverter pkConverter = null;
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
    private Meter outRps;
    private Meter outBps;
    private Counter sinkSkipCounter;
    private SimpleGauge latencyGauge;
    private transient Counter deleteCounter;

    public AdbpgOutputFormat(
            int fieldNum,
            String[] fieldNamesStrs,
            String[] keyFields,
            LogicalType[] logicalTypes,
            ReadableConfig config
    ) {
        this.config = config;
        this.url = config.get(URL);
        this.adbssHost = config.get(ADBSSHOST);
        this.adbssPort = config.get(ADBSSPORT);
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
        this.logicalTypes = logicalTypes;
        this.rowDataSerializer = new RowDataSerializer(this.logicalTypes);
        Joiner joinerOnComma = Joiner.on(",").useForNull("null");
        this.fieldNamesStrs = fieldNamesStrs;

        if (keyFields != null) {
            this.pkTypes = new LogicalType[keyFields.length];
            for (int i = 0; i < keyFields.length; i++) {
                pkFields.add(keyFields[i]);
                int j = 0;
                for (; j < fieldNamesStrs.length; j++) {
                    if (keyFields[i].equals(fieldNamesStrs[j])) {
                        pkIndex.add(j);
                        break;
                    }
                }
                if (fieldNamesStrs.length == j) {
                    throw new RuntimeException("Key cannot found in filenames.");
                }
                int keyIdx = Arrays.asList(fieldNamesStrs).indexOf(keyFields[i]);
                this.pkTypes[i] = logicalTypes[keyIdx];
            }
            this.primaryKeys = new HashSet<>(pkFields);
            this.pkConverter = new JdbcRowConverter(pkTypes);
        } else {
            this.primaryKeys = null;
            this.pkTypes = null;
            this.pkConverter = null;
        }

        this.adbpgDialect = new AdbpgDialect(targetSchema, caseSensitive);

        if (primaryKeys == null || primaryKeys.isEmpty()) {
            existsPrimaryKeys = false;
            if (2 == this.writeMode) {
                throw new RuntimeException("primary key cannot be empty when setting write mode to 2:upsert.");
            }
            this.upsertConverter = null;
        } else {
            existsPrimaryKeys = true;
            this.primaryFieldNamesStr = new String[primaryKeys.size()];
            this.nonPrimaryFieldNamesStr = new String[fieldNum - primaryKeys.size()];
            String[] primaryFieldNamesStrCaseSensitive = new String[primaryKeys.size()];
            String[] nonPrimaryFieldNamesStrCaseSensitive = new String[fieldNum - primaryKeys.size()];
            this.excludedNonPrimaryFieldNamesStr = new String[fieldNum - primaryKeys.size()];
            String[] excludedNonPrimaryFieldNamesStrCaseSensitive = new String[fieldNum - primaryKeys.size()];
            String[] fieldNamesStrCaseSensitive = new String[this.fieldNum];
            int primaryIndex = 0;
            int excludedIndex = 0;
            for (int i = 0; i < fieldNum; i++) {
                String fileName = fieldNamesStrs[i];
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
            this.updateStatementFieldTypes =
                    new LogicalType[nonPrimaryFieldNamesStr.length + primaryFieldNamesStr.length];
            int j = 0;
            this.updateStatementFieldIndices = new int[nonPrimaryFieldNamesStr.length + primaryFieldNamesStr.length];
            for (int i = 0; i < logicalTypes.length; ++i) {
                if (Arrays.asList(this.primaryFieldNamesStr).contains(fieldNamesStrs[i])) {
                    continue;
                }
                updateStatementFieldIndices[j] = i;
                updateStatementFieldTypes[j] = logicalTypes[i];
                j++;
            }
            for (int i = 0; i < primaryFieldNamesStr.length; ++i) {
                updateStatementFieldTypes[j] = pkTypes[i];
                updateStatementFieldIndices[j] = pkIndex.get(i);
                j++;
            }
            this.upsertConverter = new JdbcRowConverter(updateStatementFieldTypes);
        }
        this.rowConverter = new JdbcRowConverter(logicalTypes);
        this.streamingServerRowConverter = new StreamingServerRowConverter(logicalTypes);
        this.copyModeRowConverter = new StringFormatRowConverter(logicalTypes);
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

    @Override
    public void configure(Configuration configuration) {
    }

    private long getVersion() {
        long res = 0;
        try {
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("show adbpg_version ;");
            if (rs.next()) {
                String versionStr = rs.getString("adbpg_version");
                res = Long.parseLong(versionStr.replaceAll("\\.", ""));
            }
        } catch (SQLException e) {
            LOG.warn("Find old version ADBPG", e);
        }
        return res;
    }

    // check whether the target table is a partition table, partition table does not support upsert statement.
    private boolean checkPartition() {
        boolean res = false;
        try {
            String sql = String.format("select count(*) from pg_inherits where inhparent::regclass='%s.%s'::regclass", targetSchema, tableName);
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                res = rs.getLong("count") != 0;
            }
        } catch (SQLException e) {
            LOG.warn("Error encountered during check table partiton", e);
        }
        return res;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        dataSource = AdbpgOptions.buildDataSourceFromOptions(config);
        try {
            dataSource.init();
            executeSql("set optimizer to off");
            if (checkPartition() && writeMode == 1) {     // check the target table is partitioned, if it is true, we shouldn't use upsert statement.
                support_upsert = false;
            }
            rawConn = (DruidPooledConnection) connection;
            baseConn = (BaseConnection) (rawConn.getConnection());
            copyManager = new CopyManager(baseConn);
        } catch (SQLException e) {
            LOG.error("Init DataSource Or Get Connection Error!", e);
            throw new IOException("cannot get connection for url: " + url + ", userName: " + userName + ", password: " + password, e);
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

        outRps = MetricUtils.registerNumRecordsOutRate(getRuntimeContext());
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
            synchronized (mapBufferWithPk) {
                // Construct primary key string as map key
                String dupKey = constructDupKey(rowData, pkIndex);
                mapBufferWithPk.put(dupKey, rowData);
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
            LogicalType t = logicalTypes[i];
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
        closeConnection();
        closeCopyConnection();
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
            LOG.info("start to sync " + (mapBufferWithPk.size() + mapBufferWithoutPk.size()) + " records.");
        }
        // Synchronized mapBuffer or mapBufferWithoutPk according to existsPrimaryKeys
        synchronized (existsPrimaryKeys ? mapBufferWithPk : mapBufferWithoutPk) {
            List<RowData> addBuffer = new ArrayList<>();
            List<RowData> deleteBuffer = new ArrayList<>();
            Collection<RowData> buffer = existsPrimaryKeys ? mapBufferWithPk.values() : mapBufferWithoutPk;
            if (!buffer.isEmpty()) {
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
                batchAdd(addBuffer);

                if (!deleteBuffer.isEmpty()) {
                    if (existsPrimaryKeys) {
                        batchDeleteWithPK(deleteBuffer);
                    } else {
                        batchDeleteWithoutPk(deleteBuffer);
                    }
                }
            }
            if (1 == verbose) {
                LOG.info("finished syncing " + (mapBufferWithPk.size() + mapBufferWithoutPk.size()) + " records.");
            }
            // Clear mapBuffer and mapBufferWithoutPk
            mapBufferWithPk.clear();
            mapBufferWithoutPk.clear();
            inputCount = 0;
            lastWriteTime = System.currentTimeMillis();
        }
    }

    private void reportMetric(List<RowData> rows, long start, long end, long bps) {
        if (latencyGauge != null) {
            latencyGauge.report(end - start, rows.size());
        }
        if (outRps != null) {
            outRps.markEvent(rows.size());
        }
        if (outBps != null) {
            outBps.markEvent(bps);
            LOG.info("reportMetric with bps =  " + bps);
        }
    }

    protected Object safeGet(RowData inRow, int ordinal, LogicalType type) {
        if (inRow != null && !inRow.isNullAt(ordinal)) {
            RowData.FieldGetter fieldGetter = RowData.createFieldGetter(type, ordinal);
            return fieldGetter.getFieldOrNull(inRow);
        }
        return null;
    }

    private RowData getPrimaryKey(RowData row) {
        GenericRowData keyRow = new GenericRowData(pkIndex.size());
        for (int i = 0; i < pkIndex.size(); i++) {
            Object field = safeGet(row, pkIndex.get(i), pkTypes[i]);
            keyRow.setField(i, field);
        }
        return keyRow;
    }

    private void batchDeleteWithoutPk(List<RowData> buffers) {
        try {
            for (RowData rowData : buffers) {
                Set<Integer> nullFieldsIndex = new HashSet<>();
                for (int i = 0; i < rowData.getArity(); ++i) {
                    if (rowData.isNullAt(i)) {
                        nullFieldsIndex.add(i);
                    }
                }
                String sql =
                        adbpgDialect.getDeleteStatementWithNull(tableName, fieldNamesStrs, nullFieldsIndex);

                if (!nullFieldsIndex.isEmpty()) {
                    LogicalType[] types = new LogicalType[rowData.getArity() - nullFieldsIndex.size()];
                    GenericRowData param = new GenericRowData(rowData.getArity() - nullFieldsIndex.size());
                    for (int i = 0, j = 0; i < rowData.getArity(); ++i) {
                        if (!nullFieldsIndex.contains(i)) {
                            types[j] = logicalTypes[i];
                            param.setField(
                                    j, RowData.createFieldGetter(types[j], i).getFieldOrNull(rowData));
                            j++;
                        }
                    }
                    JdbcRowConverter converter = new JdbcRowConverter(types);
                    executeSqlWithPrepareStatement(sql, Collections.singletonList(param), converter, true);
                } else {
                    executeSqlWithPrepareStatement(sql, Collections.singletonList(rowData), rowConverter, true);
                }
            }
            deleteCounter.inc(buffers.size());
        } catch (SQLException e) {
            LOG.warn("Exception in delete sql without pk: ", e);
        }
    }

    private void batchDeleteWithPK(List<RowData> buffers) {
        String sql = adbpgDialect.getDeleteStatement(tableName, pkFields.toArray(new String[0]));
        try {
            executeSqlWithPrepareStatement(sql, buffers, this.pkConverter, true);
            deleteCounter.inc(buffers.size());
        } catch (SQLException e) {
            LOG.warn("Exception in delete sql: " + sql, e);
        }
    }

    /**
     * The router of writing method to the database. Use batch write logic first which can maximizes the writing performance
     * and if batch write logic failed, will use row by row write logic with the preset 'conflict mode'.
     * There are 3 write modes for now:
     *      0. insert mode: use 'insert into' command to write data to the database.
     *      1. copy mode: use 'copy from STDIN' command to write data to the database.
     *          If conflict mode is 'upsert', the sql will be 'copy from STDIN on conflict'.
     *      2. upsert mode: use 'insert on conflict' command to write data to the database.
     * @param rows
     */
    private void batchAdd(List<RowData> rows) {
        long bps = 0;
        if (null == rows || rows.isEmpty()) {
            return;
        }
        try {
            long start = System.currentTimeMillis();
            // TODO add a "copy on conflict" mode directly to replace "copy on conflict" when writemode is "copy" and conflictmode is "upsert"
            if (writeMode == 1) {                   /** copy, default value */
                StringBuilder stringBuilder = new StringBuilder();
                for (RowData row : rows) {
                    String[] fields = copyModeRowConverter.convertToString(row);
                    for (int i = 0; i < fields.length; i++) {
                        stringBuilder.append(fields[i]);
                        stringBuilder.append(i == fields.length - 1 ? "\r\n" : "\t");
                    }
                }
                byte[] data = stringBuilder.toString().getBytes(Charsets.UTF_8);
                bps = executeCopy(data);
                long end = System.currentTimeMillis();
                reportMetric(rows, start, end, bps);
            } else if (writeMode == 2) {            /** batch upsert */
                String sql = adbpgDialect.getUpsertStatement(tableName, fieldNamesStrs, primaryFieldNamesStr, nonPrimaryFieldNamesStr, support_upsert);
                executeSqlWithPrepareStatement(sql, rows, rowConverter, false);
            } else if (writeMode == 0) {            /** batch insert */
                String insertSql = adbpgDialect.getInsertIntoStatement(tableName, fieldNamesStrs);
                executeSqlWithPrepareStatement(insertSql, rows, rowConverter, false);
            } else if (writeMode == 3) {            /** merge with streaming server api */
                executeMergeWithStreamingServer(rows, streamingServerRowConverter, false);
            } else {
                LOG.error("Unsupported write mode: " + writeMode);
                System.exit(255);
            }
            closeConnection();
        } catch (Exception e) {
            Exception exception = e;

            boolean isDuplicateKeyException =
                    exception.getMessage() != null
                            && (exception.getMessage().contains("duplicate key") && exception.getMessage().contains("violates unique constraint"))    // duplicate key on copy statement
                            || exception.getMessage().contains("ON CONFLICT DO UPDATE");                                                   // duplicate key on copy on conflict statement
            if (isDuplicateKeyException) {
                LOG.warn("Batch write failed with duplicate-key exception, will retry with preset conflict-mode.");
            } else {
                LOG.warn("Batch write failed with exception will retry with preset conflict action. The exception is:", exception);
            }

            // Batch upsert demotes to single upsert when conflictMode='upsert' or writeMode=2
            // note that exception generated by prepared statement stack have one extra layer
            for (RowData row : rows) {
                // note that exception generated by prepared statement stack have one extra layer
                if (exception instanceof BatchUpdateException) {
                    exception = ((BatchUpdateException) exception).getNextException();
                }

                if (isDuplicateKeyException) {
                    if ("strict".equalsIgnoreCase(conflictMode)) {                    /** conflictMode = 'strict', report error without any action */
                        throw new RuntimeException("duplicate key value violates unique constraint");
                    } else if ("upsert".equalsIgnoreCase(conflictMode)) {             /** conflictMode = 'upsert', use upsert sql */
                        LOG.warn("Retrying to replace record with upsert.");
                        upsertRow(row);
                    } else if ("ignore".equalsIgnoreCase(conflictMode)) {
                        LOG.warn("Batch write failed, because preset conflictmode is 'ignore', connector will skip this row");
                    } else {                                                           /** conflictMode = 'update' or any other string, use update sql */
                        updateRow(row);
                    }
                } else {
                    // exceptionMode only have "strict" and "ignore", if this is "ignore" return directly without report an expection
                    if ("strict".equalsIgnoreCase(exceptionMode)) {
                        LOG.warn("Found unexpect exception, will ignore this row.");
                        throw new RuntimeException(exception);
                    }
                }
            }
        }
    }

    private void executeSql(String sql) throws SQLException {
        int retryTime = 0;
        while (retryTime++ < maxRetryTime) {
            try {
                connection = dataSource.getConnection();
                statement = connection.createStatement();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(sql);
                }
                statement.execute(sql);
                break;
            } catch (SQLException e) {
                //e.printStackTrace();
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
    }

    public static Session buildSession(GpssGrpc.GpssBlockingStub bStub, String gpMasterHost, int gpMasterPort,
                                       String gpRoleName, String gpPasswd, String dbname) {

        /** create a connect request builder */
        LOG.info("Starting create adbss session...");
        ConnectRequest connReq = ConnectRequest.newBuilder()
                .setHost(gpMasterHost)
                .setPort(gpMasterPort)
                .setUsername(gpRoleName)
                .setPassword(gpPasswd)
                .setDB(dbname)
                .setUseSSL(false)
                .build();

        assert bStub != null;

        return bStub.connect(connReq);
    }

    public void openForMerge(GpssGrpc.GpssBlockingStub bStub, Session mSession, String schemaName, String tableName) {
        /** open a table for write */
        // create an insert option builder
        MergeOption.Builder mBuilder = MergeOption.newBuilder();


        for (String pk: primaryFieldNamesStr) {
            mBuilder.addInsertColumns(pk);
            mBuilder.addMatchColumns(pk);
        }
        for (String nonPk: nonPrimaryFieldNamesStr) {
            mBuilder.addInsertColumns(nonPk);
            mBuilder.addUpdateColumns(nonPk);
        }
        // TODO MergeOption.Builder.setCondition,  MergeOption.Builder.setErrorLimit, MergeOption.Builder.setErrorLimitPercentage
//        mBuilder.setErrorLimitCount();
//        mBuilder.setErrorLimitPercentage();
//        mBuilder.setCondition();

        MergeOption mOpt = mBuilder.build();

        // create an open request builder
        OpenRequest oReq = OpenRequest.newBuilder()
                .setSession(mSession)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                //.setPreSQL("")
                //.setPostSQL("")
                //.setEncoding("")
                //.setStagingSchema("")
                .setMergeOption(mOpt)
                .build();

        // use the blocking stub to call the Open service; it returns nothing
        bStub.open(oReq);
    }

    private void executeMergeWithStreamingServer(List<RowData> rows, StreamingServerRowConverter rowDataConverter, boolean del) throws InterruptedException {
        long start = System.currentTimeMillis();
        int retryTime = 0;
        while(retryTime++ < maxRetryTime) {
            ManagedChannel channel = null;
            GpssGrpc.GpssBlockingStub bStub = null;
            Session mSession = null;
            long bps = 0;
            try {
                // check if hostname and port are already set
                if (database == null && hostname == null && port == 0) {
                    extractParametersWithURL(url);
                }

                // connect to ADBSS gRPC service instance; create a channel and a blocking stub
                channel = ManagedChannelBuilder.forAddress(adbssHost, adbssPort).usePlaintext().build();
                bStub = GpssGrpc.newBlockingStub(channel);
                // use the blocking stub to call the Connect service
                mSession = buildSession(bStub, hostname, port, userName, password, database);
                LOG.info("Got streaming server session id:" + mSession.getID());
                // setup column names and use blocking stub to call the Open service
                openForMerge(bStub, mSession, targetSchema, tableName);

                //put Flink-RowData into Adbss-RowData
                List<org.apache.flink.connector.jdbc.table.sink.api.RowData> ssRows = new ArrayList<>();
                for (RowData row : rows) {
                    if (existsPrimaryKeys && del) {
                        bps += rowDataConverter.toExternal(row, ssRows);
                    } else {
                        bps += rowDataConverter.toExternal(row, ssRows);
                    }
                }

                /** create a write request builder */
                WriteRequest wReq = WriteRequest.newBuilder()
                        .setSession(mSession)
                        .addAllRows(ssRows)
                        .build();

                // use the blocking stub to call the Write service; it returns nothing
                LOG.info("Start writing row data with adbss...");
                bStub.write(wReq);
                LOG.info("Finished writing row data with adbss...");

                /** create a close request builder */
                TransferStats tStats;
                CloseRequest cReq = CloseRequest.newBuilder()
                        .setSession(mSession)
                        //.setMaxErrorRows(15)
                        //.setAbort(true)
                        .build();
                /** use the blocking stub to call the Close service */
                tStats = bStub.close(cReq);
                /** display the result to stdout */
                LOG.info("CloseRequest tStats: " + tStats.toString());

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                assert bStub != null;
                assert mSession != null;
                /** use the blocking stub to call the Disconnect service */
                LOG.info("Disconnecting adbss with sessionID " + mSession.getID() + " ...");
                bStub.disconnect(mSession);

                // shutdown the channel
                channel.shutdown().awaitTermination(7, TimeUnit.SECONDS);
            }
            long end = System.currentTimeMillis();
            reportMetric(rows, start, end, bps);
        }
    }
    private void executeSqlWithPrepareStatement(
            String sql, List<RowData> valueList, JdbcRowConverter rowDataConverter, boolean del) throws SQLException {
        long start = System.currentTimeMillis();
        int retryTime = 0;
        while (retryTime++ < maxRetryTime) {
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                for (RowData rowData : valueList) {
                    if (existsPrimaryKeys && del) {
                        rowDataConverter.toExternal(getPrimaryKey(rowData), preparedStatement);
                    } else {
                        rowDataConverter.toExternal(rowData, preparedStatement);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                break;
            } catch (SQLException exception) {
                SQLException throwables = exception;
                if (throwables instanceof BatchUpdateException) {
                    throwables = ((BatchUpdateException) throwables).getNextException();
                }
                LOG.error(String.format("Execute sql error, sql: %s, retryTimes: %d", sql, retryTime), throwables);
                if (retryTime == maxRetryTime) {
                    if ("strict".equalsIgnoreCase(exceptionMode)) {
                        throw throwables;
                    } else {
                        LOG.warn("Ignore exception {} when execute sql {}", throwables, sql);
                        sinkSkipCounter.inc();
                    }
                }
                try {
                    // sleep according to retryTimes
                    Thread.sleep(retryWaitTime);
                } catch (Exception e) {
                    // ignore
                }
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception exception) {
                        LOG.debug("close connection error", exception);
                    }
                }
            }
        }

        long end = System.currentTimeMillis();
        reportMetric(valueList, start, end, (long) valueList.size() * sql.length() * 2);
        LOG.debug("%s operation succeed on %d records ", !del ? "Delete" : "Write", valueList.size());
    }

    /**
     * Executing batch write with copy command, if failed, will retry the command with 'retryWaitTime' times.
     * @param data in bytes, which is using for CopyManager
     * @return flink bps metric
     * @throws SQLException
     * @throws IOException
     */
    private long executeCopy(byte[] data) throws SQLException, IOException {
        long bps = data.length;
        InputStream inputStream = new ByteArrayInputStream(data);
        if (inputStream == null) {
            return 0;
        }
        inputStream.mark(0);
        int retryTime = 0;
        while (retryTime++ < maxRetryTime) {
            try {
                inputStream.reset();
                inputStream.mark(0);
                if (baseConn == null || baseConn.isClosed()) {
                    LOG.info("recreate baseConn within executeCopy");
                    DruidPooledConnection rawConn = dataSource.getConnection();
                    baseConn = (BaseConnection) (rawConn.getConnection());
                }
                if (copyManager == null) {
                    LOG.info("recreate copyManager within executeCopy");
                    copyManager = new CopyManager(baseConn);
                }
                String sql = adbpgDialect.getCopyStatement(tableName, fieldNamesStrs, "STDIN", conflictMode, support_upsert);
                LOG.info("Writing data with sql:" + sql);
                copyManager.copyIn(sql, inputStream);
                break;
            } catch (SQLException | IOException e) {
                if ((e.getMessage() != null && e.getMessage().contains("duplicate key")
                        && e.getMessage().contains("violates unique constraint"))
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
        return bps * retryTime;
    }

    /**
     * Executing write with 'insert on conflict' command, if failed, will throw exceptions to upstream.
     * @param row
     */
    private void upsertRow(RowData row) {
        String sql = adbpgDialect.getUpsertStatement(tableName, fieldNamesStrs, primaryFieldNamesStr, nonPrimaryFieldNamesStr, support_upsert);
        LOG.debug("Upserting row with sql:" + sql);
        try {
            executeSqlWithPrepareStatement(sql, Collections.singletonList(row), rowConverter, false);
        } catch (SQLException upsertException) {
            LOG.error("Exception in upsert sql: " + sql, upsertException);
            if ("strict".equalsIgnoreCase(exceptionMode)) {
                throw new RuntimeException(upsertException);
            }
        }
    }

    private RowData getUpdateStatementFields(RowData row) {

        GenericRowData updateStatementFields =
                new GenericRowData(updateStatementFieldIndices.length);
        for (int i = 0; i < updateStatementFieldIndices.length; ++i) {
            Object field = safeGet(row, updateStatementFieldIndices[i], updateStatementFieldTypes[i]);
            updateStatementFields.setField(i, field);
        }
        return updateStatementFields;
    }

    /**
     * Executing write with 'update' command, if failed, will throw exceptions to upstream.
     * @param row
     */
    private void updateRow(RowData row) {
        String sql = adbpgDialect.getUpdateStatement(tableName, primaryFieldNamesStr, nonPrimaryFieldNamesStr);
        LOG.debug("Updating row with sql:" + sql);
        try {
            // need to rearrange update field since pk is now at end
            RowData updateStatementFields = getUpdateStatementFields(row);
            executeSqlWithPrepareStatement(sql, Collections.singletonList(updateStatementFields), upsertConverter, false);
        } catch (SQLException upsertException) {
            LOG.error("Exception in upsert sql: " + sql, upsertException);
            if ("strict".equalsIgnoreCase(exceptionMode)) {
                throw new RuntimeException(upsertException);
            }
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
                    LOG.info("connection closed and discarded ");
                }
                connection = null;
            }
        } catch (SQLException e) {
            LOG.error("error during closeConnection");
            throw new RuntimeException(e);
        } finally {
            statement = null;
            connection = null;
        }
    }

    private void closeCopyConnection() {
        try {
            LOG.info("Close copy connection ");
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
        } catch (SQLException e) {
            LOG.error("error during closeCopyConnection");
            throw new RuntimeException(e);
        } finally {
            rawConn = null;
            baseConn = null;
        }
    }

    @Override
    public void waitFinish() throws Exception {
        LOG.info("waiting existing record finish syncing !");
        sync();
        LOG.info("finished waiting");
    }

    public static String extractDatabaseName(String jdbcUrl) {
        // check if jdbcUrl is empty
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return null;
        }

        // try to split with given url
        String[] parts = jdbcUrl.split("/");
        // database name should be the last part
        if (parts.length > 0) {
            String lastPart = parts[parts.length - 1];
            // if there are parameters in url like '?', then these should be removed
            int paramsIndex = lastPart.indexOf('?');
            if (paramsIndex != -1) {
                return lastPart.substring(0, paramsIndex);
            } else {
                return lastPart;
            }
        }
        return null;
    }

    public void extractParametersWithURL(String jdbcUrl) {
        // check if jdbcUrl is empty
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new RuntimeException("Invalid jdbc url, can't extract hostname and port with empty url");
        }

        // extract database with the given url
        String[] parts = jdbcUrl.split("/");
        if (parts.length > 0) {                         // database name should be the last part
            String lastPart = parts[parts.length - 1];
            int paramsIndex = lastPart.indexOf('?');    // if there are parameters in url like '?', then these should be removed
            if (paramsIndex != -1) {
                database =  lastPart.substring(0, paramsIndex);
            } else {
                database = lastPart;
            }
        }

        // extract hostname and port with the given url
        parts = jdbcUrl.split("//");                // assume that URL follow a regular format：jdbc:database_type://hostname:port/database_name
        String hostAndPort = parts[1].split("/")[0];
        String[] hostPortParts = hostAndPort.split(":");
        // normally the length of hostPortParts should only be 2
        if (hostPortParts.length != 2) {
            throw new RuntimeException("Invalid jdbc url, can't extract hostname and port with url: " + jdbcUrl);
        }
        // extract hostname
        hostname = hostAndPort.split(":")[0];
        port = Integer.parseInt(hostAndPort.split(":")[1]);
    }
}



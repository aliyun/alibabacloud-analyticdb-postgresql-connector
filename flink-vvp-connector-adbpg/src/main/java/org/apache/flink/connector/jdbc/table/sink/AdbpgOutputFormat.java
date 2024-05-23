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
import java.util.Locale;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.table.metric.MetricUtils;
import org.apache.flink.connector.jdbc.table.metric.SimpleGauge;
import org.apache.flink.connector.jdbc.table.utils.AdbpgDialect;
import org.apache.flink.connector.jdbc.table.utils.AdbpgOptions;
import org.apache.flink.connector.jdbc.table.utils.DateUtil;
import org.apache.flink.connector.jdbc.table.utils.JdbcRowConverter;
import org.apache.flink.connector.jdbc.table.utils.StringFormatRowConverter;
import org.apache.flink.connector.jdbc.table.utils.hash.util.JdbcTypes;
import org.apache.flink.connector.jdbc.table.utils.hash.GreenplumCdbHash;
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
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.ACCESS_METHOD;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.BATCH_SIZE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.BATCH_WRITE_TIMEOUT_MS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CASE_SENSITIVE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONFLICT_MODE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONNECTOR_TYPE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.EXCEPTION_MODE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.MAX_RETRY_TIMES;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.RESERVEMS;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.RETRY_WAIT_TIME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.SHARD_COUNT;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.TARGET_SCHEMA;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.URL;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.USE_COPY;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.VERBOSE;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.WRITE_MODE;

/**
 * ADBPG sink Implementation.
 * create AdbpgOutputFormat for detail implementation
 */
public class AdbpgOutputFormat extends RichOutputFormat<RowData> implements CleanupWhenUnsuccessful, Syncable {

    private static final transient Logger LOG = LoggerFactory.getLogger(AdbpgOutputFormat.class);
    private boolean existsPrimaryKeys = false;
    protected final RowDataSerializer rowDataSerializer;
    private final ReadableConfig config;
    private final String DELETE_WITH_KEY_SQL_TPL = "DELETE FROM %s WHERE %s ";
    private final JdbcRowConverter rowConverter;
    private final JdbcRowConverter upsertConverter;
    private final StringFormatRowConverter copyModeRowConverter;
    private final AdbpgDialect adbpgDialect;
    String[] fieldNamesStrs;
    LogicalType[] lts;
    private String url;
    private String tableName;
    private String userName;
    private String password;
    private Set<String> primaryKeys;
    private List<String> pkFields = new ArrayList<String>();
    private List<Integer> pkIndex = new ArrayList<>();
    private List<Integer> shardKeys = new ArrayList<>();
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
    // Use TreeMap treeMapBuffer for data with primary key to distinct key and reorder data
    private Map<String, RowData> treeMapBuffer = new TreeMap<>();
    // Use HashMap mapBuffer for data with primary key to preserve order
    private Map<String, RowData> mapBuffer = new HashMap<>();
    // Use List mapBufferWithoutPk for data without primary key
    private List<RowData> mapBufferWithoutPk = new ArrayList<>();
    private String insertClause = "INSERT INTO ";
    private String timeZone = "Asia/Shanghai";
    private long inputCount = 0;
    // version after which support upsert for partitioned table
    private long adbpg_version = 6360;
    private boolean support_upsert = true;

    private boolean support_pre_hash = false;
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
    private String accessMethod;
    private int shardCount;
    private int actualShardCount;
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

    public AdbpgOutputFormat(
            int fieldNum,
            String[] fieldNamesStrs,
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
        this.accessMethod = config.get(ACCESS_METHOD);
        this.shardCount = config.get(SHARD_COUNT);
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
        this.fieldNamesStrs = fieldNamesStrs;

        if (keyFields != null) {
            this.pkTypes = new LogicalType[keyFields.length];
            for (int i = 0; i < keyFields.length; i++) {
                pkFields.add(keyFields[i]);
                int t = 0;
                for (; t < fieldNamesStrs.length; t++) {
                    if (keyFields[i].equals(fieldNamesStrs[t])) {
                        pkIndex.add(t);
                        break;
                    }
                }
                if (fieldNamesStrs.length == t) {
                    throw new RuntimeException("Key cannot found in filenames.");
                }
                int keyIdx = Arrays.asList(fieldNamesStrs).indexOf(keyFields[i]);
                this.pkTypes[i] = lts[keyIdx];
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
            for (int i = 0; i < lts.length; ++i) {
                if (Arrays.asList(this.primaryFieldNamesStr).contains(fieldNamesStrs[i])) {
                    continue;
                }
                updateStatementFieldIndices[j] = i;
                updateStatementFieldTypes[j] = lts[i];
                j++;
            }
            for (int i = 0; i < primaryFieldNamesStr.length; ++i) {
                updateStatementFieldTypes[j] = pkTypes[i];
                updateStatementFieldIndices[j] = pkIndex.get(i);
                j++;
            }
            this.upsertConverter = new JdbcRowConverter(updateStatementFieldTypes);
        }
        this.rowConverter = new JdbcRowConverter(lts);
        this.copyModeRowConverter = new StringFormatRowConverter(lts);
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

    private boolean getShardKeys() {
        boolean res = false;
        try {
            // 准备SQL查询
            String sql = String.format("SELECT dp.distkey, string_agg(a.attname, ' ') AS distkeyname, dp.numsegments\n" +
                    "            FROM  gp_distribution_policy dp\n" +
                    "            JOIN pg_class c ON dp.localoid = c.oid\n" +
                    "            JOIN pg_attribute a ON c.oid = a.attrelid\n" +
                    "            WHERE c.relname = '%s' AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s') AND\n" +
                    "            a.attnum = ANY(dp.distkey) GROUP BY dp.distkey, dp.numsegments;", this.tableName, this.targetSchema);
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);

            if (rs.next()) {
                // 获取并处理distkey
                String distKeyStr = rs.getString("distkey");
                String distKeyNameStr = rs.getString("distkeyname");
                List<String> shardKeyNames = new ArrayList<>();
                if (distKeyStr != null && !distKeyStr.trim().isEmpty()) {
                    // 用空格分隔distkey，并转换为Integer列表
                    this.shardKeys = Arrays.stream(distKeyStr.trim().split("\\s+"))
                            .map(s -> Integer.parseInt(s) - 1)
                            .collect(Collectors.toList());
                    shardKeyNames = Arrays.stream(distKeyNameStr.trim().split("\\s+"))
                            .collect(Collectors.toList());

                    res = true;
                    if (1 == verbose) {
                        LOG.info("Table shard keys are {}", this.shardKeys);
                    }
                }

                // 检查定义的schema包含分布键的列顺序是否与ADBPG的Table Schema的分布键一致，如果不一致，则直接报错
                for (int i = 0; i < this.shardKeys.size(); i++) {
                    if (!this.fieldNamesStrs[this.shardKeys.get(i)].toLowerCase().equals(shardKeyNames.get(i)))
                    {
                        LOG.error("The shard key is not consistent with the table schema, Please check the table schema with adbpg table.");
                        throw new RuntimeException("The shard key is not consistent with the table schema, Please check the table schema with adbpg table.");
                    }
                }

                // 获取并转换numsegments为int
                this.actualShardCount = rs.getInt("numsegments");
            }
        } catch (SQLException e) {
            LOG.warn("Error encountered during check table shard", e);
        }
        return res;
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        dataSource = AdbpgOptions.buildDataSourceFromOptions(config);
        try {
            dataSource.init();
            executeSql("set optimizer to off");
            if (getShardKeys() && this.shardCount > 0) {
                this.support_pre_hash = true;
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
        if (existsPrimaryKeys && support_pre_hash )
        {
            synchronized (treeMapBuffer) {
                // Construct primary key string as map key
                String dupKey = constructDupKey(rowData, pkIndex);
                treeMapBuffer.put(dupKey, rowData);
            }
        }
        else if (existsPrimaryKeys) {
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
            LOG.info("current input count: " + inputCount);
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

    private int getShardId(RowData row, List<Integer> shardKeys) {
        int nKeys;
        int j = 0;
        int shardId;
        nKeys = shardKeys.size();
        String[] values = new String[nKeys];
        int[] nulls = new int[nKeys];
        int[] types = new int[nKeys];

        for (int i : shardKeys) {
            if (row.isNullAt(i)) {
                nulls[j] = 1;
                continue;
            }

            LogicalType t = lts[i];

            nulls[j] = 0;

            if (t instanceof BooleanType) {
                boolean value = row.getBoolean(i);
                values[j] = value ? "'true'" : "'false'";
                types[j] = JdbcTypes.BOOLEAN;
            } else if (t instanceof TimestampType) {
                Timestamp value = row.getTimestamp(i, 8).toTimestamp();
                values[j] = DateUtil.timeStamp2String((Timestamp) value, timeZone, reserveMs);
                types[j] = JdbcTypes.TIMESTAMP;
            } else if (t instanceof VarCharType || t instanceof CharType) {
                values[j] = row.getString(i).toString();
                types[j] = JdbcTypes.VARCHAR;
            } else if (t instanceof FloatType) {
                values[j] = row.getFloat(i) + "";
                types[j] = JdbcTypes.FLOAT;
            } else if (t instanceof DoubleType) {
                values[j] = row.getDouble(i) + "";
                types[j] = JdbcTypes.DOUBLE;
            } else if (t instanceof IntType) {
                values[j] = row.getInt(i) + "";
                types[j] = JdbcTypes.INTEGER;
            } else if (t instanceof SmallIntType) {
                values[j] = row.getShort(i) + "";
                types[j] = JdbcTypes.INTEGER;
            } else if (t instanceof TinyIntType) {
                values[j] = row.getByte(i) + "";
                types[j] = JdbcTypes.INTEGER;
            } else if (t instanceof BigIntType) {
                values[j] = row.getLong(i) + "";
                types[j] = JdbcTypes.BIGINT;
            } else if (t instanceof DecimalType) {
                DecimalType dt = (DecimalType) t;
                values[j] = row.getDecimal(i, dt.getPrecision(), dt.getScale()).toString();
                types[j] = JdbcTypes.DECIMAL;
            } else {
                throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:wangheyang.why@alibaba-inc.com");
            }
            j++;
        }

        shardId = GreenplumCdbHash.getTargetSegmentId(values, nulls, types, nKeys, this.actualShardCount);
        if (LOG.isDebugEnabled())
        {
            String dupKey = constructDupKey(row, shardKeys);
            LOG.debug("current row shard id is " + shardId + ", shard key is " + dupKey);
        }

        return shardId;
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
            LOG.info("start to sync " + (treeMapBuffer.size() + mapBuffer.size() + mapBufferWithoutPk.size()) + " records.");
            LOG.info("exist primary key mode is " + existsPrimaryKeys + ". shard count is " + shardCount);
        }
        if (existsPrimaryKeys && shardCount > 0) {
            synchronized (treeMapBuffer) {
                Map<Integer, List<RowData>> addBufferMap = new HashMap<>();
                Map<Integer, List<RowData>> deleteBufferMap = new HashMap<>();

                if (treeMapBuffer.size() > 0) {
                    // Init pre-hashed buffer
                    for (int i = 0; i < this.actualShardCount; i++) {
                        List<RowData> addBuffer = new ArrayList<>();
                        List<RowData> deleteBuffer = new ArrayList<>();
                        addBufferMap.put(i, addBuffer);
                        deleteBufferMap.put(i, deleteBuffer);
                    }

                    treeMapBuffer.forEach((key, value) -> {
                        int shard = getShardId(value, this.shardKeys);

                        switch (value.getRowKind()) {
                            case INSERT:
                            case UPDATE_AFTER:
                                addBufferMap.get(shard).add(value);
                                break;
                            case DELETE:
                            case UPDATE_BEFORE:
                                deleteBufferMap.get(shard).add(value);
                                break;
                            default:
                                throw new RuntimeException(
                                        "Not supported row kind " + value.getRowKind());
                        }
                    });

                    for (int i = 0; i < this.actualShardCount; i++) {
                        if (deleteBufferMap.get(i).size() > 0) {
                            batchDelete(deleteBufferMap.get(i));
                        }
                        batchWrite(addBufferMap.get(i));
                    }
                }

                if (1 == verbose) {
                    LOG.info("finished syncing " + (treeMapBuffer.size() + mapBuffer.size() + mapBufferWithoutPk.size()) + " records.");
                }

                // Clear treeMapBuffer, mapBuffer and mapBufferWithoutPk
                treeMapBuffer.clear();
                mapBuffer.clear();
                mapBufferWithoutPk.clear();
                inputCount = 0;
                lastWriteTime = System.currentTimeMillis();
            }
        }
        else {
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

                    // delete before insert.
                    if (deleteBuffer.size() > 0) {
                        if (existsPrimaryKeys) {
                            batchDelete(deleteBuffer);
                        }
                        else {
                            batchDeleteWithoutPk(deleteBuffer);
                        }
                    }
                    batchWrite(addBuffer);
                }
                if (1 == verbose) {
                    LOG.info("finished syncing " + (treeMapBuffer.size() + mapBuffer.size() + mapBufferWithoutPk.size()) + " records.");
                }

                // Clear treeMapBuffer, mapBuffer and mapBufferWithoutPk
                treeMapBuffer.clear();
                mapBuffer.clear();
                mapBufferWithoutPk.clear();
                inputCount = 0;
                lastWriteTime = System.currentTimeMillis();
            }
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

                if (nullFieldsIndex.size() > 0) {
                    LogicalType[] types = new LogicalType[rowData.getArity() - nullFieldsIndex.size()];
                    GenericRowData param = new GenericRowData(rowData.getArity() - nullFieldsIndex.size());
                    for (int i = 0, j = 0; i < rowData.getArity(); ++i) {
                        if (!nullFieldsIndex.contains(i)) {
                            types[j] = lts[i];
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

    private void batchDelete(List<RowData> buffers) {
        String sql = adbpgDialect.getDeleteStatement(tableName, pkFields.toArray(new String[0]));
        try {
            executeSqlWithPrepareStatement(sql, buffers, this.pkConverter, true);
            deleteCounter.inc(buffers.size());
        } catch (SQLException e) {
            LOG.warn("Exception in delete sql: " + sql, e);
        }
    }

    /**
     * The core logic of writing data to the database. Use batch write logic first which can maximizes the writing performance
     * and if batch write logic failed, will use row by row write logic with the preset 'conflict mode'.
     * There are 3 write modes for now:
     *      0. insert mode: use 'insert into' command to write data to the database.
     *      1. copy mode: use 'copy from STDIN' command to write data to the database.
     *          If conflict mode is 'upsert', the sql will be 'copy from STDIN on conflict'.
     *      2. upsert mode: use 'insert on conflict' command to write data to the database.
     * @param rows
     */
    private void batchWrite(List<RowData> rows) {
        long bps = 0;
        if (null == rows || rows.size() == 0) {
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
                String sql = adbpgDialect.getUpsertStatement(tableName, fieldNamesStrs, primaryFieldNamesStr, nonPrimaryFieldNamesStr, support_upsert, accessMethod);
                executeSqlWithPrepareStatement(sql, rows, rowConverter, false);
            } else if (writeMode == 0) {            /** batch insert */
                String insertSql = adbpgDialect.getInsertIntoStatement(tableName, fieldNamesStrs);
                executeSqlWithPrepareStatement(insertSql, rows, rowConverter, false);
            } else {
                LOG.error("Unsupported write mode: " + writeMode);
                System.exit(255);
            }
            closeConnection();
        } catch (Exception e) {
            boolean isDuplicateKeyException =
                    (e.getMessage().contains("duplicate key") && e.getMessage().contains("violates unique constraint"))    // duplicate key on copy statement
                    || e.getMessage().contains("ON CONFLICT DO UPDATE");                                                   // duplicate key on copy on conflict statement
            if (isDuplicateKeyException) {
                LOG.warn("Batch write failed with duplicate-key exception, will retry with preset conflict-mode.");
            } else {
                LOG.warn("Batch write failed with exception will retry with preset conflict action. The exception is:", e);
            }

            // Batch upsert demotes to single upsert when conflictMode='upsert' or writeMode=2
            // note that exception generated by prepared statement stack have one extra layer
            for (RowData row : rows) {
                // note that exception generated by prepared statement stack have one extra layer
                if (e instanceof BatchUpdateException) {
                    e = ((BatchUpdateException) e).getNextException();
                }

                if (isDuplicateKeyException) {
                    if ("strict".equalsIgnoreCase(conflictMode)) {                    // conflictMode = 'strict', report error without any action
                        throw new RuntimeException("duplicate key value violates unique constraint");
                    } else if ("upsert".equalsIgnoreCase(conflictMode)) {             // conflictMode = 'upsert', use upsert sql
                        LOG.warn("Retrying to replace record with upsert.");
                        upsertRow(row);
                    } else if ("ignore".equalsIgnoreCase(conflictMode)) {
                        LOG.warn("Batch write failed, because preset conflictmode is 'ignore', connector will skip this row");
                    } else {                                                            // conflictMode = 'update' or any other string, use update sql
                        updateRow(row);
                    }
                } else {
                    // exceptionMode only have "strict" and "ignore", if this is "ignore" return directly without report an expection
                    if ("strict".equalsIgnoreCase(exceptionMode)) {
                        LOG.warn("Found unexpect exception, will ignore this row.");
                        throw new RuntimeException(e);
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
        String sql = adbpgDialect.getUpsertStatement(tableName, fieldNamesStrs, primaryFieldNamesStr, nonPrimaryFieldNamesStr, support_upsert, accessMethod);
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
}

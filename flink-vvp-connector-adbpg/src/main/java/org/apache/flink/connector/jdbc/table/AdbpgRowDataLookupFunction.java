package org.apache.flink.connector.jdbc.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * ADBPG AdbpgRowDataLookupFunction Implementation.
 */
public class AdbpgRowDataLookupFunction extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(AdbpgRowDataLookupFunction.class);
    private final String url;
    private final String tablename;
    private final String username;
    private final String password;
    private int fieldNum;
    private String[]  fieldNamesStr;
    private LogicalType[] lts;
    private int retryWaitTime;;
    private int batchWriteTimeoutMs;
    private int maxRetryTime;
    private int connectionMaxActive;
    private String exceptionMode;
    private String targetSchema;
    private boolean caseSensitive;
    private int verbose;
    private int joinMaxRows;
    private String cacheStrategy;
    private int cacheSize;
    private int cacheTTLMs;

    // Primary key list and non-primary field names
    private String[] keyNames;
    private LogicalType[] keyTypes;
    private String escapedFieldNames;
    private String queryTemplate;

    // datasource
    private String driverClassName = "org.postgresql.Driver";
    private transient Cache<RowData, List<RowData>> cache;


    public AdbpgRowDataLookupFunction(String url, String tablename, String username, String password, int fieldNum, String[] fieldNamesStr, LogicalType[] lts, int retryWaitTime, int batchWriteTimeoutMs, int maxRetryTime, int connectionMaxActive, String exceptionMode, String targetSchema, int caseSensitive, int joinMaxRows, String cacheStrategy, int cacheSize, int cacheTTLMs, String[] keyNames, LogicalType[] keyTypes, int verbose)
    {
        this.url = url;
        this.tablename = tablename;
        this.username = username;
        this.password = password;
        this.fieldNum = fieldNum;
        this.fieldNamesStr = fieldNamesStr;
        this.lts = lts;
        this.retryWaitTime = retryWaitTime;
        this.batchWriteTimeoutMs = batchWriteTimeoutMs;
        this.maxRetryTime = maxRetryTime;
        this.connectionMaxActive = connectionMaxActive;
        this.exceptionMode = exceptionMode;
        this.targetSchema = targetSchema;
        this.caseSensitive = (caseSensitive == 1);
        this.joinMaxRows = joinMaxRows;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.cacheTTLMs = cacheTTLMs;
        this.keyNames = keyNames;
        this.keyTypes = keyTypes;
        this.verbose = verbose;

        Joiner joinerOnComma = Joiner.on(",").useForNull("null");
        this.escapedFieldNames = joinerOnComma.join(fieldNamesStr);
        List<String> keyFilters = new ArrayList<>();
        for (int i = 0; i < keyNames.length; i++) {
            if (this.caseSensitive) {
                keyFilters.add("\"" + keyNames[i] + "\"" + " = ?");
            }
            else{
                keyFilters.add(keyNames[i] + " = ?");
            }
        }
        String queryKeys = StringUtils.join(keyFilters, " AND ");
        if (this.caseSensitive) {
            this.queryTemplate = "SELECT " + escapedFieldNames
                    + " FROM "
                    + "\""
                    + targetSchema
                    + "\""
                    + "."
                    + "\""
                    + tablename
                    + "\""
                    + " WHERE "
                    + queryKeys;
        }
        else {
            this.queryTemplate = "SELECT " + escapedFieldNames + " FROM " + targetSchema + "." + tablename + " WHERE " + queryKeys;
        }
        if (joinMaxRows > 0) {
            this.queryTemplate = this.queryTemplate + " limit " + joinMaxRows;
        }
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keys lookup keys
     */
    public void eval(Object... keys) {
        if (1 == verbose) {
            StringBuffer sb = new StringBuffer();
            for(Object key:keys) {
                sb.append(key);
                sb.append(",");
            }
            LOG.info("start to loop up from adbpg, keys:" + sb.toString());
        }

        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                if (1 == verbose) {
                    LOG.info("fetched from cache");
                }
                return;
            }
        }
        try {
            ArrayList<RowData> rows = new ArrayList<>();
            retryExecuteQuery(keyRow, rows);
            for (RowData row: rows) {
                collect(row);
            }
            if (cache != null) {
                cache.put(keyRow, rows);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            LOG.info("error fetch from adbpg", e);
            throw new RuntimeException("cannot fetch from adbpg source", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.cache = cacheStrategy.equals("none") ? null : CacheBuilder.newBuilder().expireAfterWrite(cacheTTLMs, TimeUnit.MILLISECONDS).maximumSize(cacheSize).build();
        LOG.info("source connector created using url=" + url + ", " +
                "tableName=" + tablename + ", " +
                "userName=" + username + ", " +
                "password=" + password + ", " +
                "filedNum=" + fieldNum + ", " +
                "fieldNamesStr=" + Arrays.asList(fieldNamesStr).toString() + ", " +
                "lts=" + Arrays.asList(lts).toString() + ", " +
                "maxRetries=" + maxRetryTime + ", " +
                "retryWaitTime=" + retryWaitTime + ", " +
                "connectionMaxActive=" + connectionMaxActive + ", " +
                "exceptionMode=" + exceptionMode + ", " +
                "verbose=" + verbose + ", " +
                "caseSensitive=" + caseSensitive + ", " +
                "batchWriteTimeoutMs=" + batchWriteTimeoutMs + ", " +
                "joinMaxRows=" + joinMaxRows + ", " +
                "cacheStrategy=" + cacheStrategy + ", " +
                "targetSchema=" + targetSchema + ", " +
                "keyNames=" + Arrays.asList(keyNames).toString() + ", " +
                "keyTypes=" + Arrays.asList(keyTypes) + ", " +
                "cacheSize=" + cacheSize + ", " +
                "cacheTTLMs=" + cacheTTLMs);
    }

    private void retryExecuteQuery(RowData keyRow, ArrayList<RowData> rows) throws Exception {
        int attemptNum = 0;
        Exception lastError = null;
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            Class.forName(driverClassName).newInstance();
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.prepareStatement(queryTemplate);
            statement.clearParameters();
            for (int i = 0; i < keyNames.length; i++) {
                setStatementParameter(statement, i, keyRow);
            }
            ResultSet resultSet = statement.executeQuery();
            int cnt = 0;
            while (cnt < joinMaxRows) {
                if (resultSet.isClosed()) {
                    rows.clear();
                    throw new RuntimeException("result closed before collect.");
                }
                if(!resultSet.next()){
                    break;
                }
                rows.add(toResultRow(resultSet));
                cnt++;
            }
            return;
        } catch (Exception e) {
                LOG.warn("Error happens when query Adbpg, try for the {} time.", attemptNum, e);
                lastError = e;
        }
        finally {
            if (statement != null && !statement.isClosed()) {
                    statement.close();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
        }
        // sleep if query execute failed.
        try {
            if (attemptNum < maxRetryTime) {
                Thread.sleep(retryWaitTime);
            }
        }
        catch (Exception e) {

        }
        assert lastError != null;
        LOG.info("error orrcured where execute " + queryTemplate, lastError);
        if ("strict".equalsIgnoreCase(exceptionMode)) {
            throw lastError;
        }
    }

    private void setStatementParameter(PreparedStatement statement, int index, RowData rowdata) throws SQLException {
        LogicalType t = lts[index];
        int statindex = index + 1;
        if (t instanceof BooleanType) {
            statement.setBoolean(statindex, rowdata.getBoolean(index));
        }
        else if (t instanceof TimestampType){
            statement.setTimestamp(statindex, rowdata.getTimestamp(index, 8).toTimestamp());
        }
        else if (t instanceof TimeType) {
            Object o = rowdata.getString(index).toString();
            Time t2 = (Time) o;
            statement.setTime(statindex, t2);
        }
        else if (t instanceof DateType) {
            Object o = rowdata.getString(index).toString();
            Date d2 = (Date) o;
            statement.setDate(statindex, d2);
        }
        else if (t instanceof VarCharType || t instanceof  CharType) {
            statement.setString(statindex, rowdata.getString(index).toString());
        }
        else if (t instanceof SmallIntType){
            statement.setShort(statindex, rowdata.getShort(index));
        }
        else if (t instanceof IntType){
            statement.setInt(statindex, rowdata.getInt(index));
        }
        else if (t instanceof TinyIntType) {
            statement.setByte(statindex, rowdata.getByte(index));
        }
        else if (t instanceof BigIntType) {
            statement.setLong(statindex, rowdata.getLong(index));
        }
        else if (t instanceof FloatType){
            statement.setFloat(statindex, rowdata.getFloat(index));
        }
        else if (t instanceof DoubleType){
            statement.setDouble(statindex, rowdata.getDouble(index));
        }
        else {
            throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:huaxi.shx@alibaba-inc.com");
        }
    }
    private RowData toResultRow(ResultSet resultset) throws Exception {
        GenericRowData genericRowData = new GenericRowData(fieldNamesStr.length);
        for (int pos = 0; pos < fieldNamesStr.length; pos++) {
            Object field = resultset.getObject(pos + 1);
            genericRowData.setField(pos, dimDeserialize(pos + 1, field));
        }
        return genericRowData;
    }
    private Object dimDeserialize(int index, Object value) {
        LogicalType t = lts[index - 1];
        if (t instanceof BooleanType) {
            return (Boolean) value;
        }
        else if (t instanceof TimestampType){
            return TimestampData.fromTimestamp((Timestamp) value);
        }
        else if (t instanceof TimeType) {
            return (Time) value;
        }
        else if (t instanceof DateType) {
            return (Date) value;
        }
        else if (t instanceof VarCharType || t instanceof  CharType) {
            return StringData.fromString(value.toString());
        }
        else if (t instanceof SmallIntType){
            return Short.parseShort(value.toString());
        }
        else if (t instanceof IntType){
            return Integer.parseInt(value.toString());
        }
        else if (t instanceof TinyIntType) {
            return  Byte.parseByte(value.toString());
        }
        else if (t instanceof BigIntType) {
            return  Long.parseLong(value.toString());
        }
        else if (t instanceof FloatType){
            return Float.parseFloat(value.toString());
        }
        else if (t instanceof DoubleType){
            return Double.parseDouble(value.toString());
        }
        else {
            throw new RuntimeException("unsupported data type:" + t.toString() + ", please contact developer:huaxi.shx@alibaba-inc.com");
        }
    }
}

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;

/**
 * ADBPG Source Implementation.
 * parse look up keys and
 * create AdbpgRowDataLookupFunction for detail implementation
 */
public class AdbpgDynamicTableSource implements LookupTableSource {
    private final String url;
    private final String tablename;
    private final String username;
    private final String password;
    private int fieldNum;
    private String[] fieldNamesStr;
    private LogicalType[] lts;
    private int retryWaitTime;
    private int batchWriteTimeoutMs;
    private int maxRetryTime;
    private int connectionMaxActive;
    private String exceptionMode;
    private String targetSchema;
    private int caseSensitive;
    private int joinMaxRows;
    private String cache;
    private int cacheSize;
    private int cacheTTLMs;
    private int verbose;

    public AdbpgDynamicTableSource(String url, String tablename, String username, String password, int fieldNum,
                                   String[] fieldNamesStr, LogicalType[] lts, int retryWaitTime, int batchWriteTimeoutMs,
                                   int maxRetryTime, int connectionMaxActive, String exceptionMode, String targetSchema,
                                   int caseSensitive, int joinMaxRows, String cache, int cacheSize, int cacheTTLMs,
                                   int verbose) {
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
        this.targetSchema = targetSchema;
        this.caseSensitive = caseSensitive;
        this.exceptionMode = exceptionMode;
        this.joinMaxRows = joinMaxRows;
        this.cache = cache;
        this.cacheSize = cacheSize;
        this.cacheTTLMs = cacheTTLMs;
        this.verbose = verbose;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        String[] keyNames = new String[lookupContext.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = fieldNamesStr[innerKeyArr[0]];
        }
        List<String> nameList = Arrays.asList(fieldNamesStr);
        LogicalType[] keyTypes =
                Arrays.stream(keyNames)
                        .map(s -> {
                            Preconditions.checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                            return lts[nameList.indexOf(s)];
                        })
                        .toArray(LogicalType[]::new);

        return TableFunctionProvider.of(
                new AdbpgRowDataLookupFunction(url,
                        tablename,
                        username,
                        password,
                        fieldNum,
                        fieldNamesStr,
                        lts,
                        retryWaitTime,
                        batchWriteTimeoutMs,
                        maxRetryTime,
                        connectionMaxActive,
                        exceptionMode,
                        targetSchema,
                        caseSensitive,
                        joinMaxRows,
                        cache,
                        cacheSize,
                        cacheTTLMs,
                        keyNames,
                        keyTypes,
                        verbose));
    }

    @Override
    public DynamicTableSource copy() {
        return new AdbpgDynamicTableSource(url,
                tablename,
                username,
                password,
                fieldNum,
                fieldNamesStr,
                lts,
                retryWaitTime,
                batchWriteTimeoutMs,
                maxRetryTime,
                connectionMaxActive,
                exceptionMode,
                targetSchema,
                caseSensitive,
                joinMaxRows,
                cache,
                cacheSize,
                cacheTTLMs,
                verbose);
    }

    @Override
    public String asSummaryString() {
        return "ADBPG Source:";
    }
}

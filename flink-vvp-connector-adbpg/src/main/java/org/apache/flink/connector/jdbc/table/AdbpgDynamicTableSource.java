package org.apache.flink.connector.jdbc.table;

import org.apache.flink.configuration.ReadableConfig;
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
    private int fieldNum;
    private String[] fieldNamesStr;
    private LogicalType[] lts;
    private ReadableConfig config;

    public AdbpgDynamicTableSource(int fieldNum, String[] fieldNamesStr, LogicalType[] lts, ReadableConfig config) {
        this.fieldNum = fieldNum;
        this.fieldNamesStr = fieldNamesStr;
        this.lts = lts;
        this.config = config;
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
                new AdbpgRowDataLookupFunction(
                        fieldNum,
                        fieldNamesStr,
                        lts,
                        keyNames,
                        keyTypes,
                        config));
    }

    @Override
    public DynamicTableSource copy() {
        return new AdbpgDynamicTableSource(fieldNum, fieldNamesStr, lts, config);
    }

    @Override
    public String asSummaryString() {
        return "ADBPG Source:";
    }
}

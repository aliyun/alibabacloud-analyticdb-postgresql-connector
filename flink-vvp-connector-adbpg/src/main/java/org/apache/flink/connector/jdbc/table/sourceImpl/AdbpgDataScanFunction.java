package org.apache.flink.connector.jdbc.table.sourceImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.table.utils.YaStringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.URL;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.PASSWORD;

public class AdbpgDataScanFunction extends RichInputFormat<RowData, InputSplit>
        implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AdbpgDataScanFunction.class);
    private final String url;
    private final String tablename;
    private final String username;
    private final String password;
    private final String regexSplitter;
    private final RowDataTypeInfo returnType;
    private final DataType[] types;
    private ReadableConfig config;
    private transient DruidDataSource dataSource = null;

    private String queryTemplate;
    private Object[][] parameterValues;
    private String driverClassName = "org.postgresql.Driver";

    private transient Connection connection;
    private transient BufferedReader reader;
    private transient String line;
    private transient InputStreamReader streamReader;
    private transient PGCopyInputStream in;

    private static RowDataTypeInfo deriveRowType(TableSchema tableSchema) {
        int columnsNum = tableSchema.getFieldCount();
        String[] fieldNames = new String[columnsNum];
        LogicalType[] fieldTypes = new LogicalType[columnsNum];
        for (int idx = 0; idx < columnsNum; idx++) {
            fieldNames[idx] = tableSchema.getFieldNames()[idx];
            fieldTypes[idx] = tableSchema.getFieldDataType(fieldNames[idx]).get().getLogicalType();
        }
        return new RowDataTypeInfo(fieldTypes, fieldNames);
    }

    public AdbpgDataScanFunction(int fieldNum,
                                 String[] fieldNamesStr, LogicalType[] lts, ReadableConfig config, TableSchema schema,
                                 Object[][] parameterValues, String query) {
        this.parameterValues = parameterValues;
        this.config = config;
        this.regexSplitter = "\\" + "\u0002";
        this.returnType = deriveRowType(schema);
        this.queryTemplate = query;
        this.types = schema.getFieldDataTypes();
        this.url = config.get(URL);
        this.tablename = config.get(TABLE_NAME);
        this.username = config.get(USERNAME);
        this.password = config.get(PASSWORD);
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing here
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (parameterValues == null) {
            return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        }
        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        try {
            if (inputSplit != null && parameterValues != null) {
                int pl = parameterValues[inputSplit.getSplitNumber()].length;
                String[] bound = new String[pl];
                for (int i = 0; i < pl; i++) {
                    bound[i] = parameterValues[inputSplit.getSplitNumber()][i].toString();
                }
                queryTemplate =
                        String.format(queryTemplate, bound);
            }

            String query =
                    String.format(
                            "COPY (%s ) TO STDOUT WITH DELIMITER e'%s'",
                            queryTemplate, "\u0002");
            LOG.info(
                    String.format(
                            "Executing '%s' ",
                            query));

            Class.forName(driverClassName).newInstance();
            connection = DriverManager.getConnection(url, username, password);
            in = new PGCopyInputStream((PGConnection) connection, query);
            streamReader = new InputStreamReader(in,"UTF-8");
            reader = new BufferedReader(streamReader);
        } catch (Exception se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        LOG.info("reachedEnd called");

        return (line = reader.readLine()) == null;
    }

    @Override
    public RowData nextRecord(RowData rowData) throws IOException {

        GenericRowData row = new GenericRowData(returnType.getArity());

        String[] values = line.split(regexSplitter, -1);
        LOG.info("get " + values.toString());
        assert (row.getArity() == values.length);

        for (int i = 0; i < row.getArity(); i++) {
            if (values[i].equals("\\N")) {
                continue;
            }
            row.setField(i, YaStringUtils.convertStringToInternalObject(values[i], types[i]));
        }

        return row;
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return returnType;
    }
}

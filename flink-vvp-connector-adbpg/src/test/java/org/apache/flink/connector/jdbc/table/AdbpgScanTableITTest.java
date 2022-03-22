package org.apache.flink.connector.jdbc.table;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.connector.jdbc.table.base.AdbpgScanITCaseBase;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.CONNECTOR_TYPE;

public class AdbpgScanTableITTest extends AdbpgScanITCaseBase {

    protected EnvironmentSettings streamSettings;

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    // a |  b  |    c    | d |     e      |     f      |      g      |             h
    //  |    i    |                 j                  | k |             l             |
    //            m                    |            n             |         o         |     p
    // ---+-----+---------+---+------------+------------+-------------+----------------------------+---------+------------------------------------+---+---------------------------+-----------------------------------------+--------------------------+-------------------+-----------
    // 1 | dim | 20.2007 | f |   652482   | 2020-07-08 | source_test | 2020-07-10
    // 16:28:07.737+08 | 8.58965 | {8589934592,8589934593,8589934594} |   |
    // {8.58967,96.4667,9345.16} | {587897.4646746,792343.646446,76.46464} |
    // {monday,saturday,sunday} | {464,98661,32489} | {t,t,f,t}
    private static final String INSERT_SQL = " insert into TABLE_NAME values  (1,'dim',20.2007,'f',652482," +
            "'2020-07-08','source_test',timestamp '2020-07-10 16:28:07.737+08',8.58965,'{8589934592,8589934593,8589934594}',null," +
            "'{8.589667,96.46667,9345.164}','{587897.4646746,792343.646446,76.46464}','{monday,saturday,sunday}','{464,98661,32489}','{t,t,f,t}',8119.23,'t')";
    private static final String CREATE_SCAN_SOURCE = " create table TABLE_NAME  \n" +
            "                           ( \n" +
            "                           a int not null,\n" +
            "                           b TEXT not null,\n" +
            "                           c DOUBLE PRECISION not null,\n" +
            "                           d boolean,\n" +
            "                           e bigint, \n" +
            "                           f date, \n" +
            "                           g varchar, \n" +
            "                           h TIMESTAMP, \n" +
            "                           i float, \n" +
            "                           j  bigint [],\n" +
            "                           k  bigint [], \n" +
            "                           l  float [], \n" +
            "                           m  DOUBLE PRECISION [], \n" +
            "                           n  TEXT [], \n" +
            "                           o  int [], \n" +
            "                           p  boolean [], \n" +
            "                           r Decimal(6,2), \n" +
            "                           s boolean );";
    private static final String TEST_TABLE_NAME =
            "dynamic_scan_" + RandomStringUtils.randomAlphabetic(16).toLowerCase();
    public AdbpgScanTableITTest() throws IOException {
    }

    @Before
    public void setup() {
        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();

        this.streamSettings = streamBuilder.useBlinkPlanner().build();

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setParallelism(2);
        tEnv = StreamTableEnvironment.create(env, streamSettings);
    }

    private void createSourceTable() throws Exception {
        createTable(CREATE_SCAN_SOURCE.replace("TABLE_NAME", TEST_TABLE_NAME));
        executeSql(INSERT_SQL.replace("TABLE_NAME", TEST_TABLE_NAME), false);
    }

    @Test
    public void testScanTable() throws Exception {
        createSourceTable();
        tEnv.executeSql(
                "create table "
                        + TEST_TABLE_NAME
                        + "(\n"
                        + "a int not null,\n"
                        + "b STRING not null,\n"
                        + "c double not null,\n"
                        + "d boolean,\n"
                        + "e bigint,\n"
                        + "f date,\n"
                        + "g varchar,\n"
                        + "h TIMESTAMP,\n"
                        + "i float,\n"
                        + "j array<bigint>,\n"
                        + "k array<bigint>,\n"
                        + "l array<float>,\n"
                        + "m array<double>,\n"
                        + "n array<STRING>,\n"
                        + "o array<int>,\n"
                        + "p array<boolean>,\n"
                        + "r Decimal(6,2),\n"
                        + "s boolean\n"
                        + ") with ("
                        + String.format("'connector'='%s',\n", CONNECTOR_TYPE)
                        + "'url'='"
                        + url
                        + "',\n"
                        + "'tablename'='"
                        + TEST_TABLE_NAME
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql(
                        "SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, r, s FROM "
                                + TEST_TABLE_NAME);
        Long[] j = {8589934592L, 8589934593L, 8589934594L};
        Long[] k = null;
        Float[] l = {8.589667f, 96.46667f, 9345.164f};
        Double[] m = {587897.4646746, 792343.646446, 76.46464};
        String[] n = {"monday", "saturday", "sunday"};
        Integer[] o = {464, 98661, 32489};
        Boolean[] p = {true, true, false, true};
        Object[] actual = Lists.newArrayList(result.collect()).toArray();
        Object[] expected =
                new Object[]{
                        Row.of(
                                1,
                                "dim",
                                20.2007,
                                false,
                                652482L,
                                LocalDate.of(2020, 7, 8),
                                "source_test",
                                LocalDateTime.of(
                                        LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
                                8.58965f,
                                j,
                                k,
                                l,
                                m,
                                n,
                                o,
                                p,
                                BigDecimal.valueOf(811923, 2),
                                true)
                };
        Assert.assertArrayEquals(expected, actual);
    }
}

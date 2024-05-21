package org.apache.flink.connector.jdbc.table.utils.hash;

import org.apache.flink.connector.jdbc.table.utils.hash.util.JdbcTypes;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GreenplumCdbHashTest {

    @Test
    public void testGetTargetSegmentIdWithNonNullValues() {
        String[] values = {"10374294", "1679-绿色-M"};
        int[] isNulls = {0, 0}; // 0 indicates not null
        int[] dataType = {JdbcTypes.INTEGER, JdbcTypes.VARCHAR}; // Example data types
        int nKeys = values.length;
        int numSegments = 20; // Example number of segments

        int actualResult = GreenplumCdbHash.getTargetSegmentId(values, isNulls, dataType, nKeys, numSegments);

        Assert.assertEquals(18, actualResult);

        String[] values2 = {"99900000"};
        int[] isNulls2 = {0}; // 0 indicates not null
        int[] dataType2 = {JdbcTypes.INTEGER};
        nKeys = values2.length;
        numSegments = 3;

        actualResult = GreenplumCdbHash.getTargetSegmentId(values2, isNulls2, dataType2, nKeys, numSegments);
        Assert.assertEquals(2, actualResult);
    }
}
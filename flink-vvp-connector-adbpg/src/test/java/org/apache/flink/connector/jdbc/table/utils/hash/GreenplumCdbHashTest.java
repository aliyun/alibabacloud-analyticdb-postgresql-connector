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

        String[] values3 = {"12994560", "B2-718左大"};
        int[] isNulls3 = {0, 0}; // 0 indicates not null
        int[] dataType3 = {JdbcTypes.INTEGER, JdbcTypes.VARCHAR}; // Example data types
        nKeys = values.length;
        numSegments = 20; // Example number of segments

        actualResult = GreenplumCdbHash.getTargetSegmentId(values3, isNulls3, dataType3, nKeys, numSegments);

        Assert.assertEquals(9, actualResult);


        String[] values4 = {"12994560", "手工槽8243左大+LR8001+JF7001*2"};
        int[] isNulls4 = {0, 0}; // 0 indicates not null
        int[] dataType4 = {JdbcTypes.INTEGER, JdbcTypes.VARCHAR}; // Example data types
        nKeys = values.length;
        numSegments = 20; // Example number of segments

        actualResult = GreenplumCdbHash.getTargetSegmentId(values4, isNulls4, dataType4, nKeys, numSegments);

        Assert.assertEquals(3, actualResult);


        String[] values5 = {"12994560", "B2-718右大"};
        int[] isNulls5 = {0, 0}; // 0 indicates not null
        int[] dataType5 = {JdbcTypes.INTEGER, JdbcTypes.VARCHAR}; // Example data types
        nKeys = values.length;
        numSegments = 20; // Example number of segments

        actualResult = GreenplumCdbHash.getTargetSegmentId(values5, isNulls5, dataType5, nKeys, numSegments);

        Assert.assertEquals(1, actualResult);

        String[] values6 = {"12994560", "手工槽8243右大"};
        int[] isNulls6 = {0, 0}; // 0 indicates not null
        int[] dataType6 = {JdbcTypes.INTEGER, JdbcTypes.VARCHAR}; // Example data types
        nKeys = values.length;
        numSegments = 20; // Example number of segments

        actualResult = GreenplumCdbHash.getTargetSegmentId(values6, isNulls6, dataType6, nKeys, numSegments);

        Assert.assertEquals(2, actualResult);
    }
}
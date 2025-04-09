package org.apache.flink.connector.jdbc.table;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.connector.jdbc.table.sink.AdbpgOutputFormat;
import org.apache.flink.connector.jdbc.table.utils.StringFormatRowConverter;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.apache.flink.connector.jdbc.table.utils.AdbpgOptions.*;


@RunWith(MockitoJUnitRunner.class)
public class AdbpgOutputFormatTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private StringFormatRowConverter mockConverter;

    private AdbpgOutputFormat instance;

    @Before
    public void setup() throws Exception {
        // 初始化 AdbpgOutputFormat 实例
        instance = new AdbpgOutputFormat(
                3, // fieldNum
                new String[]{"field1", "field2", "field3"}, // fieldNamesStrs
                new String[]{"field1"}, // keyFields（必须存在于 fieldNamesStrs 中）
                new LogicalType[]{ // 填充有效的 LogicalType 数组
                        new VarCharType(), // field1 的类型
                        new VarCharType(), // field2 的类型
                        new VarCharType()  // field3 的类型
                },
                mockConfig() // 使用模拟的 config 对象
        );

        // 设置实例变量（替换反射设置静态字段的代码）
        setInstanceField(instance, "copyFormat", "csv");
        setInstanceField(instance, "copyQuote", "\"");
        setInstanceField(instance, "replace_break", false);
        setInstanceField(instance, "delimiter", ",");
        setInstanceField(instance, "copyModeRowConverter", mockConverter);
    }

    private ReadableConfig mockConfig() {
        ReadableConfig mockConfig = mock(ReadableConfig.class);
        // 模拟必要的配置项
        when(mockConfig.get(ADBSSHOST)).thenReturn("localhost");
        when(mockConfig.get(ADBSSPORT)).thenReturn(5432);
        when(mockConfig.get(URL)).thenReturn("jdbc:adbss://localhost:5432/test_db");
        when(mockConfig.get(TABLE_NAME)).thenReturn("test_table");
        when(mockConfig.get(USERNAME)).thenReturn("user");
        when(mockConfig.get(PASSWORD)).thenReturn("pass");
        when(mockConfig.get(BATCH_WRITE_TIMEOUT_MS)).thenReturn(1000);
        when(mockConfig.get(RESERVEMS)).thenReturn(0);
        when(mockConfig.get(CONFLICT_MODE)).thenReturn("upsert");
        when(mockConfig.get(USE_COPY)).thenReturn(0);
        when(mockConfig.get(MAX_RETRY_TIMES)).thenReturn(3);
        when(mockConfig.get(REPLACE_NULL_CHAR)).thenReturn(false);
        when(mockConfig.get(BATCH_SIZE)).thenReturn(100);
        when(mockConfig.get(TARGET_SCHEMA)).thenReturn("public");
        when(mockConfig.get(EXCEPTION_MODE)).thenReturn("ignore");
        when(mockConfig.get(CASE_SENSITIVE)).thenReturn(0);
        when(mockConfig.get(WRITE_MODE)).thenReturn(1);
        when(mockConfig.get(DELIMITER)).thenReturn("\t");
        when(mockConfig.get(REPLACE_BREAK)).thenReturn(false);
        when(mockConfig.get(COPY_FORMAT)).thenReturn("csv");
        when(mockConfig.get(COPY_QUOTE)).thenReturn("\"");
        when(mockConfig.get(VERBOSE)).thenReturn(0);
        when(mockConfig.get(RETRY_WAIT_TIME)).thenReturn(100);
        return mockConfig;
    }

    @Test
    public void testPreprocessCopyDataCsvFormat() throws Exception {
        List<RowData> rows = new ArrayList<>();
        RowData row = GenericRowData.of("a", "b", "c");
        when(mockConverter.convertToString(row)).thenReturn(new String[]{"a", "b", "c"});
        rows.add(row);

        byte[] resultBytes = instance.preprocessCopyData(rows);
        String resultStr = new String(resultBytes, StandardCharsets.UTF_8); // 调用实例方法
        String expected = "\"a\",\"b\",\"c\"\r\n";
        assertEquals(expected, resultStr);
    }

    @Test
    public void testNullValueHandlingInCsv() throws Exception {
        List<RowData> rows = new ArrayList<>();
        RowData row = GenericRowData.of("a", null, "c");
        when(mockConverter.convertToString(row)).thenReturn(new String[]{"a", null, "c"});
        rows.add(row);

        byte[] resultBytes = instance.preprocessCopyData(rows);
        String resultStr = new String(resultBytes, StandardCharsets.UTF_8);
        String expected = "\"a\",\"null\",\"c\"\r\n";
        assertEquals(expected, resultStr);
    }

    @Test
    public void testTextFormatWithReplaceBreak() throws Exception {
        setInstanceField(instance, "copyFormat", "text");
        setInstanceField(instance, "replace_break", true);
        setInstanceField(instance, "delimiter", "\t");

        List<RowData> rows = new ArrayList<>();
        RowData row = GenericRowData.of("a\nb", "c");
        when(mockConverter.convertToString(row)).thenReturn(new String[]{"a\nb", "c"});
        rows.add(row);

        byte[] resultBytes = instance.preprocessCopyData(rows);
        String resultStr = new String(resultBytes, StandardCharsets.UTF_8);
        String expected = "ab" + "\t" + "c" + "\r\n";
        assertEquals(expected, resultStr);
    }

    @Test
    public void testInvalidCopyFormat() throws Exception {
        setInstanceField(instance, "copyFormat", "json");
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unsupported copyformat: json");

        // 创建 Mock RowData 并配置转换器行为
        RowData mockRow = mock(RowData.class);
        // 确保返回非空字符串数组（至少一个元素）
        when(mockConverter.convertToString(mockRow)).thenReturn(new String[]{"dummy"});

        instance.preprocessCopyData(Collections.singletonList(mockRow));
    }

    // 辅助方法：设置实例字段
    private static void setInstanceField(Object instance, String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }
}

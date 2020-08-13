import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class InputMap implements MapFunction<String, Row> {
    private static final long serialVersionUID = 1L;
    AdbpgTableSchema schema;
    private static String delimiter = ",";

    public InputMap(AdbpgTableSchema schema) {
        this.schema = schema;
    }

    public InputMap(AdbpgTableSchema schema, String delimiter) {
        this.schema = schema;
        this.delimiter = delimiter;
    }

    public Row map(String line) throws Exception {
        String[] arr = line.split(delimiter);
        int columnLen = this.schema.getLength();
        if (arr.length == columnLen) {
            Row row = new Row(columnLen);
            for(int i = 0; i < columnLen; i++){
                row.setField(i, arr[i]);
            }
            return row;
        }
        return null;
    }
}
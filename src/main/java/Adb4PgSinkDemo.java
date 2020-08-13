import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import java.util.*;


/**
 * Usage:
 * bin/flink run -c Adb4PgSinkDemo flinksinkadbpg-1.0-jar-with-dependencies.jar
 */

public class Adb4PgSinkDemo {
    public static void main(String[] args) throws Exception {
        // setup environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // setup source stream
        DataStream<String> sourceStream = env.socketTextStream("127.0.0.1", 9000);

        // setup sink stream
        String dbURL = "jdbc:postgresql://adbpg-url:port/databasename";
        String tableName = "tablename";
        String userName = "username";
        String passWord = "password";
        ArrayList<String> primaryKeys = new ArrayList<String>(Arrays.asList(new String[]{"id"}));
        ArrayList<String> fieldNames = new ArrayList<String>(Arrays.asList(new String[]{"id", "name"}));
        ArrayList<Class<?>> dataTypes = new ArrayList<Class<?>>(Arrays.asList(new Class<?>[]{Integer.class, String.class}));
        AdbpgTableSchema schema = new AdbpgTableSchema(primaryKeys, fieldNames, dataTypes);
        Adb4PgTableSink sinkStream = new Adb4PgTableSink(dbURL, tableName, userName, passWord, schema, schema.getPrimaryKeys());
        // configure your adbpgsink
        //sinkStream.setCaseSensitive(true);
        //sinkStream.setMaxRetryTime(4);
        //sinkStream.setWriteMode(0);
        //sinkStream.setConflictMode("upsert");

        // launch job
        String delimiter = ",";
        DataStream<Row> messageStream = sourceStream.map(new InputMap(schema, delimiter));
        messageStream.addSink(sinkStream);
        env.execute("poc Example");
    }
}



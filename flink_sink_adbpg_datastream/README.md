# Flink sink for ADBPG (AnalyticDB for PostgreSQL) with Datastream API

This project supports writing data to ADBPG based on the Flink sink stream.

# How to use:

Here we introduce the usage combined with a demo:

```java
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

        // launch job
        String delimiter = ",";
        DataStream<Row> messageStream = sourceStream.map(new InputMap(schema, delimiter));
        messageStream.addSink(sinkStream);
        env.execute("poc Example");
}
```

# Configuration of the source data:
In the demo, the socket stream is used as an example. When using, you can change it to the actual application's stream data source. It's required to ensure that the format of the remote data entering the ADBPG sink is a single line of text with each column separated by a fixed delimiter.

For instance, if you want to write data with id=1, name=hello, the format should be transformed to:
```
1,hello
```
Here, a comma is used as a delimiter. The actual delimiter can be specified by changing the 'delimiter' in the code.

# Configuration of the sink data:
On the sink side, you need to pass primaryKeys (primary key fields), fieldNames (all field names), and types (all field types, see supported types at the end) as three parameters to deliver data metadata to the ADBPG sink connector. These three parameters must be in an ordered list, and the subsequent order of data fields written must be consistent with the order of the input list.

Further, you need to pass your ADBPG connection address (formatted as jdbc:postgresql://host:port/dbName), tableName, userName, and password to initialize an Adb4PgTableSink. The sink can call the respective setter to modify the write configuration. A detailed configuration list is provided at the end.

# Compilation:
```shell
mvn clean package     
```
After the demo program is successfully compiled, the corresponding jar package (flinksinkadbpg-1.0-jar-with-dependencies.jar) will be generated in the target directory.


# Execution:

## Start a source-side socket stream data flow:
```shell
nc -l 9000
```
## Start the Flink job:
```shell
bin/flink -c Adb4PgSinkDemo flinksinkadbpg-1.0-jar-with-dependencies.jar
```

Supported Parameters:

| Parameter Name | Meaning                                                                                                                                                | Mandatory | Default                                                                              |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------------|
| url	           | Connection address                                                                                                                                     | 	Yes     | 	None                                                                                |
|tableName| Target table name                                                                                                                                      |Yes| None                                                                                 |
|userName| Username                                                                                                                                               |Yes| None                                                                                 |
|password| Password                                                                                                                                               |Yes| None                                                                                 |
|primaryKeys| Primary keys                                                                                                                                           |	Yes| 	None                                                                                |
|maxRetryTime| 	Maximum retry attempts                                                                                                                                |	No	| 3                                                                                    |
|batchSize| Default write batch                                                                                                                                    |	No| 	5000                                                                                |
|targetSchema| 	Target schema                                                                                                                                         |	No| 	public                                                                              |
|caseSensitive| 	Case sensitivity                                                                                                                                      |	No| 	false                                                                               |
|writeMode| 	Write method                                                                                                                                          |	No| 	0: batch insert;<br> 1: batch copy;<br> 2: batch upsert. Default mode is batch copy |
|retryWaitTime| 	Wait time during error retry                                                                                                                          |	No|	100 (in ms)|
|exceptionMode| 	Exception strategy                                                                                                                                    |	No|	ignore: ignore erroneous data and continue inserting the next entry; strict: raise an error and terminate the task. Default is ignore|
|conflictMode| Primary key conflict strategy                                                                                                                          |	No|	On primary key conflict, ignore: continue inserting the next data; strict: raise an error and terminate the task; upsert: use upsert method for inserting. Default is ignore|
|batchWriteTimeout| 	Timeout for automatic write. If this interval is exceeded without reaching batchSize, it will trigger a write to the target automatically. Time in ms |	No|	5000

# Supported Data Types:
|Data types supported by adbpg sink|	Corresponding ADBPG type|
|------------|---------------|
|Short.class|	SMALLINT|
|Boolean.class|	BOOLEAN|
|Integer.class|	INT|
|Long.class|	BIGINT|
|Float.class|	REAL|
|Double.class|	DOUBLE PRECISION|
|String.class|	TEXT|
|Time.class|	TIME|
|Timestamp.class|	TIMESTAMP|

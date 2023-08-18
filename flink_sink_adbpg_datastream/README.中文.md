flink sink for ADBPG(AnalyticDB for PostgreSQL)    

本项目用于支持基于flink sink stream，向ADBPG写入数据。    
## 使用方式：    
这里结合一个demo介绍使用方式：    
```public static void main(String[] args) throws Exception {
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
### source端数据配置：   
demo中以socket stream为例，使用时可以更改为实际应用的流数据源，需要保证接入adbpg sink的远端数据的格式为：各列以固定分隔符分隔的单行文本。
例如我们想写入一条id=1,name=hello的数据，格式应转换为:
```
1,hello
```    
这里以逗号为分隔符，实际分隔符可以通过改变代码里的delimiter指定。
### sink端数据配置：
sink端需要传入primaryKeys（主键字段）, fieldNames（全部字段名）, types（全部字段类型，支持的字段类型种类见文末）三个参数来向ADBPG sink connector传入数据元信息。三个入参必须为有序列表类型，后续写入数据的字段排列顺序需要与传入列表的排列顺序一致。    
还需要传入自己的ADBPG连接地址（格式为jdbc:postgresql://host:port/dbName），tableName，userName，password来初始化一个Adb4PgTableSink。sink可以调用相应setter更改写入配置，详细配置列表见文末。    
## 编译:    
  mvn clean package     
  demo程序编译成功后会在target目录下生成对应的jar包(flinksinkadbpg-1.0-jar-with-dependencies.jar).   
## 执行:
  ### 启动一个源端socket stream数据流:
  nc -l 9000
  ### 启动flink job:
  bin/flink -c Adb4PgSinkDemo flinksinkadbpg-1.0-jar-with-dependencies.jar
  
## 支持参数列表：
| 参数名 | 含义 | 是否必填 | 默认值 |  
| ----  | ---- | ----    | ---- |
|url|连接地址|是|无|
|tableName|目标表表名|是|无|
|userName|用户名|是|无|
|password|密码|是|无|
|primaryKeys|连接地址|是|无|
|maxRetryTime|最大重试次数|否|3|
|batchSize|默认写入batch|否|5000|
|targetSchema|目标schema|否|public|
|caseSensitive|是否大小写敏感|否|false|
|writeMode|写入方式|否|0: batch insert; 1: batch copy; 2: batch upsert 默认写入方式为batch copy|
|retryWaitTime|错误重试时等待时间|否|100，单位ms|
|exceptionMode|异常策略|否|ignore:忽略产生异常的数据，继续插入下一条数据;strict:报错并终止任务，默认值:ignore|
|conflictMode|主键冲突策略|否|当产生主键冲突时，ignore:忽略产生异常的数据，继续插入下一条数据; strict:报错并终止任务; upsert: 采用upsert方式插入数据。默认值:ignore|
|batchWriteTimeout|超时自动写入时间，如果超过这个间隔还没有攒够batchSize，会自动触发一次到目标端的写入, 单位ms|否|5000|

## 数据类型支持：
|adbpg sink支持的数据类型|对应的ADBPG类型|
| ----  | ---- | 
|Short.class|SAMLLINT|
|Boolean.class|BOOLEAN|
|Integer.class|INT|
|Long.class|BIGINT|
|Float.class|REAL|
|Double.class|DOUBLE PRECISION|
|String.class|TEXT|
|Time.class|TIME|
|Timestamp.class|Timestamp|

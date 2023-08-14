# 通过实时计算Flink版写入数据到云原生数据仓库AnalyticDB PostgreSQL版

此项目用于alibaba公有云产品[adbpg](https://www.aliyun.com/product/apsaradb/gpdb)，本文介绍如何通过阿里云实时计算Flink版写入数据到AnalyticDB PostgreSQL版。

## 使用限制
该功能暂不支持AnalyticDB PostgreSQL版Serverless模式。
仅Flink实时计算引擎VVR 6.0.0及以上版本支持云原生数据仓库AnalyticDB PostgreSQL版连接器，且该连接器暂不支持云原生数据仓库AnalyticDB PostgreSQL版7.0版。

> 说明 如果您使用了自定义连接器，具体操作请参见管理[自定义连接器](https://help.aliyun.com/zh/flink/user-guide/manage-custom-connectors?spm=a2c4g.408979.0.0.c9da402czZ7Rlv)。

## 前提条件
* AnalyticDB PostgreSQL版实例和Flink全托管工作空间需要位于同一VPC下。
* 已创建Flink全托管工作空间。具体操作，请参见开通Flink全托管。
* 已创建AnalyticDB PostgreSQL版实例。具体操作，请参见创建实例。

## 配置AnalyticDB PostgreSQL版实例
1. 登录[云原生数据仓库AnalyticDB PostgreSQL版控制台](https://gpdbnext.console.aliyun.com/gpdb/cn-hangzhou/list?spm=a2c4g.408979.0.0.c9da402czZ7Rlv)。
2. 将Flink工作空间所属的网段加入AnalyticDB PostgreSQL版的白名单。如何添加白名单，请参见设置[白名单](https://help.aliyun.com/document_detail/50207.html?spm=a2c4g.408979.0.0.c9da402czZ7Rlv#task-v4f-dmr-52b)。
3. 单击登录数据库，连接数据库的更多方式，请参见[客户端连接](https://help.aliyun.com/document_detail/35428.html?spm=a2c4g.408979.0.0.c9da402czZ7Rlv#concept-ncj-gmr-52b)。
4. 在AnalyticDB PostgreSQL版实例上创建一张表。
建表SQL示例如下：
```sql
CREATE TABLE test_adbpg_table(
b1 int,
b2 int,
b3 text,
PRIMARY KEY(b1)
);
```
## 配置实时计算Flink
1. 登录[实时计算控制台](https://realtime-compute.console.aliyun.com/regions/cn-shanghai?spm=a2c4g.408979.0.0.c9da402czZ7Rlv)。
2. 在Flink全托管页签，单击目标工作空间操作列下的控制台。
3. 在左侧导航栏，单击数据连接。
4. 在数据连接页面，单击创建连接器。
5. 上传自定义连接器JAR文件。
> 说明
获取AnalyticDB PostgreSQL版自定义Flink Connector的JAR包，请参见AnalyticDB PostgreSQL Connector。
JAR包的版本需要与实时计算平台的Flink引擎版本一致。

6. 上传完成后，单击继续。
系统会对您上传的自定义连接器内容进行解析。如果解析成功，您可以继续下一步。如果解析失败，请确认您上传的自定义连接器代码是否符合Flink社区标准。
7. 单击完成。
创建完成的自定义连接器会出现在连接器列表中。

## 写入数据到AnalyticDB PostgreSQL版
1. 创建Flink作业。
   - 登录实时计算控制台，在Flink全托管页签，单击目标工作空间操作列下的控制台。
   - 在左侧导航栏，单击SQL开发，单击新建，选择空白的流作业草稿，单击下一步。
   - 在新建文件草稿对话框，填写作业配置信息后，单击创建。

| 作业参数 | 说明                                                                                                                                                             | 示例                   |
|------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|
| 文件名称 | 作业的名称(作业名称在当前项目中必须保持唯一)                                                                                                                                        | adbpg-test           |
| 存储位置 | 指定该作业的代码文件所属的文件夹。您还可以在现有文件夹右侧，单击新建文件夹图标，新建子文件夹。                                                                                                                | 作业草稿                 |
| 引擎版本 | 当前作业使用的Flink的引擎版本。引擎版本号含义、版本对应关系和生命周期重要时间点详情请参见[引擎版本介绍](https://help.aliyun.com/zh/flink/product-overview/engine-version?spm=a2c4g.408979.0.0.c9da402czZ7Rlv)。 | vvr-6.0.6-flink-1.15 |
2. 编写作业代码。
   创建随机源表`datagen_source`和对应AnalyticDB PostgreSQL版的表`test_adbpg_table`，将以下作业代码拷贝到作业文本编辑区。
```sql
   CREATE TABLE datagen_source (
   f_sequence INT,
   f_random INT,
   f_random_str STRING
   ) WITH (
   'connector' = 'datagen',
   'rows-per-second'='5',
   'fields.f_sequence.kind'='sequence',
   'fields.f_sequence.start'='1',
   'fields.f_sequence.end'='1000',
   'fields.f_random.min'='1',
   'fields.f_random.max'='1000',
   'fields.f_random_str.length'='10'
   );

CREATE TABLE test_adbpg_table (
`B1` bigint   ,
`B2` bigint  ,
`B3` VARCHAR ,
`B4` VARCHAR,
PRIMARY KEY(B1) not ENFORCED
) with (
'connector' = 'adbpg-nightly-1.13',
'password' = 'xxx',
'tablename' = 'test_adbpg_table',
'username' = 'xxxx',
'url' = 'jdbc:postgresql://url:5432/schema',
'maxretrytimes' = '2',
'batchsize' = '50000',
'connectionmaxactive' = '5',
'conflictmode' = 'ignore',
'usecopy' = '0',
'targetschema' = 'public',
'exceptionmode' = 'ignore',
'casesensitive' = '0',
'writemode' = '1',
'retrywaittime' = '200'
);
```
其中`datagen_source`表的参数无需修改，`test_adbpg_table`表的参数需要根据您实际情况进行修改，参数说明如下：

| 参数                  | 是否必填 | 说明                                                                                                                                                                                                                                                                                                                                                                                                         |
|---------------------|------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector           | 是    | connector名称，固定为adbpg-nightly-版本号，例如adbpg-nightly-1.13。                                                                                                                                                                                                                                                                                                                                                     |
| url                 | 是    | AnalyticDB PostgreSQL版的JDBC连接地址。格式为jdbc:postgresql://<内网地址>:<端口>/<连接的数据库名称>，示例如下jdbc:postgresql://gp-xxxxxx.gpdb.cn-chengdu.rds.aliyuncs.com:5432/postgres。                                                                                                                                                                                                                                                |
| tablename           | 是    | AnalyticDB PostgreSQL版的表名。                                                                                                                                                                                                                                                                                                                                                                                 |
| username            | 是    | AnalyticDB PostgreSQL版的数据库账号。                                                                                                                                                                                                                                                                                                                                                                              |
| password            | 是    | AnalyticDB PostgreSQL版的数据库账号密码。                                                                                                                                                                                                                                                                                                                                                                            |
| maxretrytimes       | 否    | SQL执行失败后重试次数，默认为3次。                                                                                                                                                                                                                                                                                                                                                                                        |
| batchsize           | 否    | 一次批量写入的最大条数，默认为50000条。                                                                                                                                                                                                                                                                                                                                                                                     |
| exceptionmode       | 否    | 数据写入过程中出现异常时的处理策略。支持以下两种处理策略：<br>* ignore（默认值）：忽略出现异常时写入的数据。<br>* strict：数据写入异常时，故障转移（Failover）并报错。                                                                                                                                                                                                                                                                                                        |
| conflictmode        | 否    | 当出现主键冲突或者唯一索引冲突时的处理策略。支持以下四种处理策略：<br>* ignore ：忽略主键冲突，保留之前的数据。<br>* strict：主键冲突时，故障转移（Failover）并报错。<br>* update：主键冲突时，更新新到的数据。<br>* upsert（默认值）：主键冲突时，采用UPSERT方式写入数据。<br>AnalyticDB PostgreSQL版通过INSERT ON CONFLICT和COPY ON CONFLICT实现UPSERT写入数据，如果目标表为分区表，则需要内核小版本为V6.3.6.1及以上，如何升级内核小版本，请参见[版本升级](https://help.aliyun.com/document_detail/139271.html?spm=a2c4g.408979.0.0.c9da402czZ7Rlv#task-2245828)。 |
| targetschema        | 否    | AnalyticDB PostgreSQL版的Schema，默认为public。                                                                                                                                                                                                                                                                                                                                                                   |
| writemode           | 否    | 写入方式。取值说明：<br>0 ：采用BATCH INSERT方式写入数据。<br>1：默认值，采用COPY API写入数据。<br>2：采用BATCH UPSERT方式写入数据。<br>如果`writemode=1`并且`conflictmode=upsert`时，程序内部会自动开启copy on conflict语法执行最初的攒批写入操作。                                                                                                                                                                                                                              |
| verbose             | 否    | 是否输出connector运行日志。取值说明：<br>* 0（默认）：不输出运行日志。<br>* 1：输出运行日志。                                                                                                                                                                                                                                                                                                                                                 |
| retrywaittime       | 否    | 出现异常重试时间隔的时间。单位为ms，默认值为100。                                                                                                                                                                                                                                                                                                                                                                                |
| batchwritetimeoutms | 否    | 攒批写入数据时最长攒批时间，超过此事件会触发这一批的写入。单位为毫秒（ms），默认值为50000。                                                                                                                                                                                                                                                                                                                                                          |
| connectionmaxactive | 否    | 连接池参数，单个Task manager中连接池同一时刻最大连接数。默认值为5。                                                                                                                                                                                                                                                                                                                                                                   |
| casesensitive       | 否    | 列名和表名是否区分大小写，取值说明：<br>* 0（默认）：不区分大小写。<br>* 1：区分大小写。                                                                                                                                                                                                                                                                                                                                                        |

> 说明 支持参数和类型映射，详情请参见连接器云原生数据仓库AnalyticDB PostgreSQL版。

3. 启动作业
   - 在作业开发页面顶部，单击部署，在弹出的对话框中，单击确定。
   - 在作业运维页面，单击目标作业操作列的启动。
   - 单击启动。
> 说明 Session集群适用于非生产环境的开发测试环境，您可以使用Session集群模式调试作业，提高作业JM（Job Manager）资源利用率和作业启动速度。但不推荐您将作业提交至Session集群中，因为会存在业务稳定性问题，详情请参见[作业调试](https://help.aliyun.com/zh/flink/user-guide/debug-a-deployment?spm=a2c4g.408979.0.0.c9da402czZ7Rlv)。


# 观察同步结果
连接AnalyticDB PostgreSQL版数据库。具体操作，请参见客户端连接。
执行以下语句查询test_adbpg_table表。
SELECT * FROM test_adbpg_table;
数据正常写入到AnalyticDB PostgreSQL版中，返回示例如下。
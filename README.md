# alibabacloud-analyticdb-postgresql-connector
alibabacloud-analyticdb-postgresql-connector

简介：
====
该项目提供Flink使用的adbpg-connector，支持Source端和Sink端两种方式,测试可用。

Source端参数说明
-------

参数 | 含义 | 备注
---- | ----- | ------
url | ADBPG连接地址 | 必填选项
tablename | ADBPG源表名 | 必填选项,库名
username | ADBPG账号 | 必填
password | ADBPG密码 | 必填
joinMaxRows | 左表一条记录连接右表的最大记录数 | 非必填，表示在一对多连接时，左表一条记录连接右表的最大记录数（默认值为1024）。在一对多连接的记录数过多时，可能会极大的影响流任务的性能，因此您需要增大Cache的内存（cacheSize限制的是左表key的个数）。
maxRetryTimes | 单次SQL失败后重试次数 | 非必填，实际执行时，可能会因为各种因素造成执行失败，比如网络或者IO不稳定，超时等原因，ADBPG维表支持SQL执行失败后自动重试，用maxRetryTimes参数可以设定重试次数。默认值为3。
connectionMaxActive | 连接池最大连接数 | 非必填，ADBPG维表中内置连接池，设置合理的连接池最大连接数可以兼顾效率和安全性，默认值为5。
retryWaitTime | 重试休眠时间 | 非必填，每次SQL失败重试之间的sleep间隔，单位ms，默认值100
targetSchema | 查询的ADBPG schema | 非必填，默认值public
caseSensitive | 是否大小写敏感 | 非必填，默认值0，即不敏感；填1可以设置为敏感;
cache | 缓存策略 | 目前分析型数据库PostgreSQL版支持以下三种缓存策略：none（默认值）：无缓存; lru：缓存维表里的部分数据。源表来一条数据，系统会先查找Cache，如果没有找到，则去物理维表中查询。需要配置相关参数：缓存大小（cacheSize）和缓存更新时间间隔（cacheTTLMs)
cacheSize | 设置LRU缓存的最大行数 | 非必填，默认为10000行
cacheTTLMs | 缓存更新时间间隔。系统会根据您设置的缓存更新时间间隔，重新加载一次维表中的最新数据，保证源表能JOIN到维表的最新数据 | 非必填，单位为毫秒。默认不设置此参数，表示不重新加载维表中的新数据


Sink端参数说明
-------

参数 | 说明 |  是否必选  | 备注
---- | ----- | ------ | ------
type | 类型 | 是 | 固定值,为adbgp
url | jdbc连接地址 | 是 | adbpg的JDBC连接地址,格式为：url='jdbc:postgresql://gp-xxxxxx.gpdb.cn-chengdu.rds.aliyuncs.com:5432/postgres'.
tableName | 表名 | 是 | 无
username | 账号 | 是 | 无
password | 密码 | 是 | 无
maxRetryTimes | 写入重试次数 | 否 | 默认3次
useCopy | 是否采用copy API写入数据 | 否 | 默认为1，表示采用copy API方式写入;当取值为0时，代表根据writeMode字段采用其他方式写入数据
exceptionMode | 当存在写入过程中出现异常时的处理策略 | 否 | 支持以下两种取值："ignore": 忽略出现导致写入异常的数据;"strict": 日志记录导致写入异常的数据，然后停止任务;默认取值为"ignore"
batchSize | 一次批量写入的条数 | 否 | 默认值为500
batchWriteTimeoutMs | 批写入超时时间 | 否 | 默认值5000ms
conflictMode | 当出现主键冲突或者唯一索引冲突时的处理策略 | 否 | 支持以下三种取值："ignore": 忽略出现导致主键冲突的数据;"strict": 日志记录导致主键冲突的数据，然后停止任务;"update":当出现主键冲突时更新为新值;"upsert": 以insert on conflict方式处理主键冲突;默认取值为"ignore"
targetSchema | schema名称 | 否 | 默认值为"public"
writeMode | 在useCopy字段基础上，更细分的写入方式 | 否 | 默认值为1代表采用copy API写入数据;在useCopy字段为0的场景下，可以设定writeMode字段采用其他写入方式;writeMode=0 ：采用insert方式写入数据;writeMode=2：采用upsert方式写入数据;upsert含义见文档;注意采用upsert方式写入时需要设定主键字段，设定主键的方式参考示例语句。

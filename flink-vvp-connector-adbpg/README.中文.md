# Flink Adbpg Connector For TableAPI

此项目用于alibaba公有云产品[adbpg](https://www.aliyun.com/product/apsaradb/gpdb)，此项目为使用table api方式开发的adbpg connector，用于在flink通过sql方式读取和写入adbpg数据并进行流式ETL。greenplum由于语法同样基于Postgresql，理论上除adbpg特有语法相关配置外可以通用。

# 特别说明
adbpg connector在aliyun存在两个版本，[开源版本](https://github.com/aliyun/alibabacloud-analyticdb-postgresql-connector)和[商业版本](https://help.aliyun.com/zh/flink/use-cases/read-analyticdb-for-postgresql-data-by-using-realtime-compute-for-apache-flink?spm=a2c4g.11186623.0.0.63346986iYy6zO)。
<br>

**商业版**：[公有云托管版flink](https://www.aliyun.com/product/bigdata/sc?spm=5176.23667485.J_4VYgf18xNlTAyFFbOuOQe.183.2ec774fbcsTml8&scm=20140722.X_data-30fc6ea88dfef9d209f0._.V_1)内置了adbpg connector，相较于开源版有更完善的开发平台和监控系统，建议购买了商业版版本的用户直接使用内置adbpg connector，会有7*24小时值班服务。
<br>

**开源版**：对于使用自建[apache flink](https://flink.apache.org)并且使用了adbpg的用户，可以使用本项目作为flink与adbpg的对接途径。
<br>


> 由于本项目为开源项目，不需要商业版的发版过程，部分特性更新可能领先于商业版，如果需要这些功能商业版用户需要使用也通过使用[自定义connector方式](https://help.aliyun.com/zh/flink/user-guide/manage-custom-connectors?spm=a2c4g.408979.0.0.c9da402czZ7Rlv)应用此connector。
欢迎提交commit和issue，共同完善项目功能和使用体验。

# 最佳实践汇总
* [通过Flink写入数据到云原生数据仓库AnalyticDB PostgreSQL版](README.insert.md)
* [通过Flink读取云原生数据仓库AnalyticDB PostgreSQL版](https://help.aliyun.com/document_detail/408978.html?spm=a2c4g.2402144.0.0.23a914749hTLen)
* [通过Flink将Kafka数据同步至AnalyticDB PostgreSQL版](https://help.aliyun.com/document_detail/606470.html?spm=a2c4g.35364.0.i1)
* [通过Flink集成向量数据至AnalyticDB PostgreSQL版](https://help.aliyun.com/document_detail/2411195.html?spm=a2c4g.606470.0.i4)



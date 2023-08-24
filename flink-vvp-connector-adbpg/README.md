# Flink Adbpg Connector For TableAPI

This project is for the Alibaba public cloud product adbpg. It is developed as an adbpg connector using flink table API, allowing for reading and writing of adbpg data in Flink via SQL and performing stream ETL. Since greenplum's syntax is also based on PostgresSQL, theoretically, except for configurations related to adbpg-specific syntax, it can be universally applied.

## Special Notes

There are two versions of the adbpg connector on Aliyun: the [Open Source Version](https://github.com/aliyun/alibabacloud-analyticdb-postgresql-connector) and the [Commercial Version](https://help.aliyun.com/zh/flink/use-cases/read-analyticdb-for-postgresql-data-by-using-realtime-compute-for-apache-flink?spm=a2c4g.11186623.0.0.63346986iYy6zO).

**Commercial Edition**: [The Public Cloud Managed Flink](https://www.aliyun.com/product/bigdata/sc?spm=5176.23667485.J_4VYgf18xNlTAyFFbOuOQe.183.2ec774fbcsTml8&scm=20140722.X_data-30fc6ea88dfef9d209f0._.V_1) has a built-in adbpg connector. Compared to the open-source version, it has a more comprehensive development platform and monitoring system. Users who have purchased the commercial edition are recommended to directly use the built-in adbpg connector, which comes with 24 * 7 on-duty services.

**Open Source Edition**: For users who utilize a self-built apache flink and have also used adbpg, this project can work as a [custom connector](https://help.aliyun.com/zh/flink/user-guide/manage-custom-connectors?spm=a2c4g.408979.0.0.c9da402czZ7Rlv) on Commercial Flink Edition.

Additionally, since this is an open-source project that doesn't require the release process of the commercial edition, some feature updates might precede the commercial version. If users of the commercial edition require these features, they can also use this project's connector via the custom connector method. Contributions in the form of commits and issues are welcome to enhance the project's functionality and user experience.

## Collection of Best Practices

* [Writing Data to Cloud-native Data Warehouse AnalyticDB PostgreSQL Edition through Flink](https://www.alibabacloud.com/help/en/analyticdb-for-postgresql/latest/use-realtime-compute-to-write-data)
* [Reading Data from Cloud-native Data Warehouse AnalyticDB PostgreSQL Edition via Flink](https://www.alibabacloud.com/help/en/analyticdb-for-postgresql/latest/use-realtime-compute-to-read-data-from-analyticdb-for-postgresql-databases)
* [Synchronizing Kafka Data to AnalyticDB PostgreSQL Edition via Flink](https://www.alibabacloud.com/help/en/analyticdb-for-postgresql/latest/synchronize-kafka-data-to-analyticdb-postgresql-through-flink)
* [Integrating Vector Data to AnalyticDB PostgreSQL Edition through Flink](https://help.aliyun.com/document_detail/2411195.html?spm=a2c4g.606470.0.i4) Chinese only, English version is coming




[中文版](README.中文.md)
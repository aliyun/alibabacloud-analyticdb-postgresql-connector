# Flink AnalyticDB PostgreSQL Connector
This project is using for the Alibaba public cloud product adbpg. It is developed as an adbpg connector using flink connector API(Table API or Datastream API), allowing for reading and writing of adbpg data in Flink via SQL or java code and performing stream ETL. Since greenplum's syntax is also based on PostgresSQL, theoretically, except for configurations related to adbpg-specific syntax, it can be applied on greenplum.

* [Table API](flink-vvp-connector-adbpg/README.md): Ingest data with flink-sql.
* [Datastream API](flink_sink_adbpg_datastream/README.md): Ingest data with java code.
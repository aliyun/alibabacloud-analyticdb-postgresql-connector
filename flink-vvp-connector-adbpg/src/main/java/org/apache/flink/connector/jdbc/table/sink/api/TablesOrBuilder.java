// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: gpss.proto

package org.apache.flink.connector.jdbc.table.sink.api;

public interface TablesOrBuilder extends
    // @@protoc_insertion_point(interface_extends:api.Tables)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .api.TableInfo Tables = 1;</code>
   */
  java.util.List<TableInfo>
      getTablesList();
  /**
   * <code>repeated .api.TableInfo Tables = 1;</code>
   */
  TableInfo getTables(int index);
  /**
   * <code>repeated .api.TableInfo Tables = 1;</code>
   */
  int getTablesCount();
  /**
   * <code>repeated .api.TableInfo Tables = 1;</code>
   */
  java.util.List<? extends TableInfoOrBuilder>
      getTablesOrBuilderList();
  /**
   * <code>repeated .api.TableInfo Tables = 1;</code>
   */
  TableInfoOrBuilder getTablesOrBuilder(
      int index);
}
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: adbpgss.proto

package org.apache.flink.connector.jdbc.table.sink.api;

public interface UpdateOptionOrBuilder extends
    // @@protoc_insertion_point(interface_extends:api.UpdateOption)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * Names of the target table columns to compare when determining to update or not
   * </pre>
   *
   * <code>repeated string MatchColumns = 1;</code>
   * @return A list containing the matchColumns.
   */
  java.util.List<java.lang.String>
      getMatchColumnsList();
  /**
   * <pre>
   * Names of the target table columns to compare when determining to update or not
   * </pre>
   *
   * <code>repeated string MatchColumns = 1;</code>
   * @return The count of matchColumns.
   */
  int getMatchColumnsCount();
  /**
   * <pre>
   * Names of the target table columns to compare when determining to update or not
   * </pre>
   *
   * <code>repeated string MatchColumns = 1;</code>
   * @param index The index of the element to return.
   * @return The matchColumns at the given index.
   */
  java.lang.String getMatchColumns(int index);
  /**
   * <pre>
   * Names of the target table columns to compare when determining to update or not
   * </pre>
   *
   * <code>repeated string MatchColumns = 1;</code>
   * @param index The index of the value to return.
   * @return The bytes of the matchColumns at the given index.
   */
  com.google.protobuf.ByteString
      getMatchColumnsBytes(int index);

  /**
   * <pre>
   * Names of the target table columns to update if MatchColumns match
   * </pre>
   *
   * <code>repeated string UpdateColumns = 2;</code>
   * @return A list containing the updateColumns.
   */
  java.util.List<java.lang.String>
      getUpdateColumnsList();
  /**
   * <pre>
   * Names of the target table columns to update if MatchColumns match
   * </pre>
   *
   * <code>repeated string UpdateColumns = 2;</code>
   * @return The count of updateColumns.
   */
  int getUpdateColumnsCount();
  /**
   * <pre>
   * Names of the target table columns to update if MatchColumns match
   * </pre>
   *
   * <code>repeated string UpdateColumns = 2;</code>
   * @param index The index of the element to return.
   * @return The updateColumns at the given index.
   */
  java.lang.String getUpdateColumns(int index);
  /**
   * <pre>
   * Names of the target table columns to update if MatchColumns match
   * </pre>
   *
   * <code>repeated string UpdateColumns = 2;</code>
   * @param index The index of the value to return.
   * @return The bytes of the updateColumns at the given index.
   */
  com.google.protobuf.ByteString
      getUpdateColumnsBytes(int index);

  /**
   * <pre>
   * Optional additional match condition; SQL syntax and used after the 'WHERE' clause
   * </pre>
   *
   * <code>string Condition = 3;</code>
   * @return The condition.
   */
  java.lang.String getCondition();
  /**
   * <pre>
   * Optional additional match condition; SQL syntax and used after the 'WHERE' clause
   * </pre>
   *
   * <code>string Condition = 3;</code>
   * @return The bytes for condition.
   */
  com.google.protobuf.ByteString
      getConditionBytes();

  /**
   * <pre>
   * Error limit count; used by external table
   * </pre>
   *
   * <code>int64 ErrorLimitCount = 4;</code>
   * @return The errorLimitCount.
   */
  long getErrorLimitCount();

  /**
   * <pre>
   * Error limit percentage; used by external table
   * </pre>
   *
   * <code>int32 ErrorLimitPercentage = 5;</code>
   * @return The errorLimitPercentage.
   */
  int getErrorLimitPercentage();
}

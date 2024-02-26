// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: gpss.proto

package org.apache.flink.connector.jdbc.table.sink.api;

public interface DBValueOrBuilder extends
    // @@protoc_insertion_point(interface_extends:api.DBValue)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>int32 Int32Value = 1;</code>
   * @return The int32Value.
   */
  int getInt32Value();

  /**
   * <code>int64 Int64Value = 2;</code>
   * @return The int64Value.
   */
  long getInt64Value();

  /**
   * <code>float Float32Value = 5;</code>
   * @return The float32Value.
   */
  float getFloat32Value();

  /**
   * <code>double Float64Value = 6;</code>
   * @return The float64Value.
   */
  double getFloat64Value();

  /**
   * <pre>
   * Includes types whose values are presented as string but are not a real string type in Greenplum; for example: macaddr, time with time zone, box, etc.
   * </pre>
   *
   * <code>string StringValue = 7;</code>
   * @return The stringValue.
   */
  String getStringValue();
  /**
   * <pre>
   * Includes types whose values are presented as string but are not a real string type in Greenplum; for example: macaddr, time with time zone, box, etc.
   * </pre>
   *
   * <code>string StringValue = 7;</code>
   * @return The bytes for stringValue.
   */
  com.google.protobuf.ByteString
      getStringValueBytes();

  /**
   * <code>bytes BytesValue = 8;</code>
   * @return The bytesValue.
   */
  com.google.protobuf.ByteString getBytesValue();

  /**
   * <pre>
   * Time without timezone
   * </pre>
   *
   * <code>.google.protobuf.Timestamp TimeStampValue = 10;</code>
   * @return Whether the timeStampValue field is set.
   */
  boolean hasTimeStampValue();
  /**
   * <pre>
   * Time without timezone
   * </pre>
   *
   * <code>.google.protobuf.Timestamp TimeStampValue = 10;</code>
   * @return The timeStampValue.
   */
  com.google.protobuf.Timestamp getTimeStampValue();
  /**
   * <pre>
   * Time without timezone
   * </pre>
   *
   * <code>.google.protobuf.Timestamp TimeStampValue = 10;</code>
   */
  com.google.protobuf.TimestampOrBuilder getTimeStampValueOrBuilder();

  /**
   * <code>.google.protobuf.NullValue NullValue = 11;</code>
   * @return The enum numeric value on the wire for nullValue.
   */
  int getNullValueValue();
  /**
   * <code>.google.protobuf.NullValue NullValue = 11;</code>
   * @return The nullValue.
   */
  com.google.protobuf.NullValue getNullValue();

  public DBValue.DBTypeCase getDBTypeCase();
}
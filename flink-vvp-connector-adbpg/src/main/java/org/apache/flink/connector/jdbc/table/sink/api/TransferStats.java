// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: adbpgss.proto

package org.apache.flink.connector.jdbc.table.sink.api;

/**
 * <pre>
 * Close service Response message
 * </pre>
 *
 * Protobuf type {@code api.TransferStats}
 */
public final class TransferStats extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:api.TransferStats)
    TransferStatsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use TransferStats.newBuilder() to construct.
  private TransferStats(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private TransferStats() {
    errorRows_ = com.google.protobuf.LazyStringArrayList.EMPTY;
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new TransferStats();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return org.apache.flink.connector.jdbc.table.sink.api.Adbpgss.internal_static_api_TransferStats_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return org.apache.flink.connector.jdbc.table.sink.api.Adbpgss.internal_static_api_TransferStats_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            org.apache.flink.connector.jdbc.table.sink.api.TransferStats.class, org.apache.flink.connector.jdbc.table.sink.api.TransferStats.Builder.class);
  }

  public static final int SUCCESSCOUNT_FIELD_NUMBER = 1;
  private long successCount_ = 0L;
  /**
   * <pre>
   * Number of rows successfully loaded
   * </pre>
   *
   * <code>int64 SuccessCount = 1;</code>
   * @return The successCount.
   */
  @java.lang.Override
  public long getSuccessCount() {
    return successCount_;
  }

  public static final int ERRORCOUNT_FIELD_NUMBER = 2;
  private long errorCount_ = 0L;
  /**
   * <pre>
   * Number of error lines if Errorlimit is not reached
   * </pre>
   *
   * <code>int64 ErrorCount = 2;</code>
   * @return The errorCount.
   */
  @java.lang.Override
  public long getErrorCount() {
    return errorCount_;
  }

  public static final int ERRORROWS_FIELD_NUMBER = 3;
  @SuppressWarnings("serial")
  private com.google.protobuf.LazyStringList errorRows_;
  /**
   * <pre>
   * Number of rows with incorrectly-formatted data; not supported
   * </pre>
   *
   * <code>repeated string ErrorRows = 3;</code>
   * @return A list containing the errorRows.
   */
  public com.google.protobuf.ProtocolStringList
      getErrorRowsList() {
    return errorRows_;
  }
  /**
   * <pre>
   * Number of rows with incorrectly-formatted data; not supported
   * </pre>
   *
   * <code>repeated string ErrorRows = 3;</code>
   * @return The count of errorRows.
   */
  public int getErrorRowsCount() {
    return errorRows_.size();
  }
  /**
   * <pre>
   * Number of rows with incorrectly-formatted data; not supported
   * </pre>
   *
   * <code>repeated string ErrorRows = 3;</code>
   * @param index The index of the element to return.
   * @return The errorRows at the given index.
   */
  public java.lang.String getErrorRows(int index) {
    return errorRows_.get(index);
  }
  /**
   * <pre>
   * Number of rows with incorrectly-formatted data; not supported
   * </pre>
   *
   * <code>repeated string ErrorRows = 3;</code>
   * @param index The index of the value to return.
   * @return The bytes of the errorRows at the given index.
   */
  public com.google.protobuf.ByteString
      getErrorRowsBytes(int index) {
    return errorRows_.getByteString(index);
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (successCount_ != 0L) {
      output.writeInt64(1, successCount_);
    }
    if (errorCount_ != 0L) {
      output.writeInt64(2, errorCount_);
    }
    for (int i = 0; i < errorRows_.size(); i++) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, errorRows_.getRaw(i));
    }
    getUnknownFields().writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (successCount_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, successCount_);
    }
    if (errorCount_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(2, errorCount_);
    }
    {
      int dataSize = 0;
      for (int i = 0; i < errorRows_.size(); i++) {
        dataSize += computeStringSizeNoTag(errorRows_.getRaw(i));
      }
      size += dataSize;
      size += 1 * getErrorRowsList().size();
    }
    size += getUnknownFields().getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof org.apache.flink.connector.jdbc.table.sink.api.TransferStats)) {
      return super.equals(obj);
    }
    org.apache.flink.connector.jdbc.table.sink.api.TransferStats other = (org.apache.flink.connector.jdbc.table.sink.api.TransferStats) obj;

    if (getSuccessCount()
        != other.getSuccessCount()) return false;
    if (getErrorCount()
        != other.getErrorCount()) return false;
    if (!getErrorRowsList()
        .equals(other.getErrorRowsList())) return false;
    if (!getUnknownFields().equals(other.getUnknownFields())) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + SUCCESSCOUNT_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getSuccessCount());
    hash = (37 * hash) + ERRORCOUNT_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getErrorCount());
    if (getErrorRowsCount() > 0) {
      hash = (37 * hash) + ERRORROWS_FIELD_NUMBER;
      hash = (53 * hash) + getErrorRowsList().hashCode();
    }
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(org.apache.flink.connector.jdbc.table.sink.api.TransferStats prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * <pre>
   * Close service Response message
   * </pre>
   *
   * Protobuf type {@code api.TransferStats}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:api.TransferStats)
      org.apache.flink.connector.jdbc.table.sink.api.TransferStatsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.flink.connector.jdbc.table.sink.api.Adbpgss.internal_static_api_TransferStats_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.flink.connector.jdbc.table.sink.api.Adbpgss.internal_static_api_TransferStats_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.flink.connector.jdbc.table.sink.api.TransferStats.class, org.apache.flink.connector.jdbc.table.sink.api.TransferStats.Builder.class);
    }

    // Construct using org.apache.flink.connector.jdbc.table.sink.api.TransferStats.newBuilder()
    private Builder() {

    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);

    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      bitField0_ = 0;
      successCount_ = 0L;
      errorCount_ = 0L;
      errorRows_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return org.apache.flink.connector.jdbc.table.sink.api.Adbpgss.internal_static_api_TransferStats_descriptor;
    }

    @java.lang.Override
    public org.apache.flink.connector.jdbc.table.sink.api.TransferStats getDefaultInstanceForType() {
      return org.apache.flink.connector.jdbc.table.sink.api.TransferStats.getDefaultInstance();
    }

    @java.lang.Override
    public org.apache.flink.connector.jdbc.table.sink.api.TransferStats build() {
      org.apache.flink.connector.jdbc.table.sink.api.TransferStats result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public org.apache.flink.connector.jdbc.table.sink.api.TransferStats buildPartial() {
      org.apache.flink.connector.jdbc.table.sink.api.TransferStats result = new org.apache.flink.connector.jdbc.table.sink.api.TransferStats(this);
      buildPartialRepeatedFields(result);
      if (bitField0_ != 0) { buildPartial0(result); }
      onBuilt();
      return result;
    }

    private void buildPartialRepeatedFields(org.apache.flink.connector.jdbc.table.sink.api.TransferStats result) {
      if (((bitField0_ & 0x00000004) != 0)) {
        errorRows_ = errorRows_.getUnmodifiableView();
        bitField0_ = (bitField0_ & ~0x00000004);
      }
      result.errorRows_ = errorRows_;
    }

    private void buildPartial0(org.apache.flink.connector.jdbc.table.sink.api.TransferStats result) {
      int from_bitField0_ = bitField0_;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.successCount_ = successCount_;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        result.errorCount_ = errorCount_;
      }
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof org.apache.flink.connector.jdbc.table.sink.api.TransferStats) {
        return mergeFrom((org.apache.flink.connector.jdbc.table.sink.api.TransferStats)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(org.apache.flink.connector.jdbc.table.sink.api.TransferStats other) {
      if (other == org.apache.flink.connector.jdbc.table.sink.api.TransferStats.getDefaultInstance()) return this;
      if (other.getSuccessCount() != 0L) {
        setSuccessCount(other.getSuccessCount());
      }
      if (other.getErrorCount() != 0L) {
        setErrorCount(other.getErrorCount());
      }
      if (!other.errorRows_.isEmpty()) {
        if (errorRows_.isEmpty()) {
          errorRows_ = other.errorRows_;
          bitField0_ = (bitField0_ & ~0x00000004);
        } else {
          ensureErrorRowsIsMutable();
          errorRows_.addAll(other.errorRows_);
        }
        onChanged();
      }
      this.mergeUnknownFields(other.getUnknownFields());
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 8: {
              successCount_ = input.readInt64();
              bitField0_ |= 0x00000001;
              break;
            } // case 8
            case 16: {
              errorCount_ = input.readInt64();
              bitField0_ |= 0x00000002;
              break;
            } // case 16
            case 26: {
              java.lang.String s = input.readStringRequireUtf8();
              ensureErrorRowsIsMutable();
              errorRows_.add(s);
              break;
            } // case 26
            default: {
              if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                done = true; // was an endgroup tag
              }
              break;
            } // default:
          } // switch (tag)
        } // while (!done)
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      } finally {
        onChanged();
      } // finally
      return this;
    }
    private int bitField0_;

    private long successCount_ ;
    /**
     * <pre>
     * Number of rows successfully loaded
     * </pre>
     *
     * <code>int64 SuccessCount = 1;</code>
     * @return The successCount.
     */
    @java.lang.Override
    public long getSuccessCount() {
      return successCount_;
    }
    /**
     * <pre>
     * Number of rows successfully loaded
     * </pre>
     *
     * <code>int64 SuccessCount = 1;</code>
     * @param value The successCount to set.
     * @return This builder for chaining.
     */
    public Builder setSuccessCount(long value) {
      
      successCount_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Number of rows successfully loaded
     * </pre>
     *
     * <code>int64 SuccessCount = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearSuccessCount() {
      bitField0_ = (bitField0_ & ~0x00000001);
      successCount_ = 0L;
      onChanged();
      return this;
    }

    private long errorCount_ ;
    /**
     * <pre>
     * Number of error lines if Errorlimit is not reached
     * </pre>
     *
     * <code>int64 ErrorCount = 2;</code>
     * @return The errorCount.
     */
    @java.lang.Override
    public long getErrorCount() {
      return errorCount_;
    }
    /**
     * <pre>
     * Number of error lines if Errorlimit is not reached
     * </pre>
     *
     * <code>int64 ErrorCount = 2;</code>
     * @param value The errorCount to set.
     * @return This builder for chaining.
     */
    public Builder setErrorCount(long value) {
      
      errorCount_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Number of error lines if Errorlimit is not reached
     * </pre>
     *
     * <code>int64 ErrorCount = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearErrorCount() {
      bitField0_ = (bitField0_ & ~0x00000002);
      errorCount_ = 0L;
      onChanged();
      return this;
    }

    private com.google.protobuf.LazyStringList errorRows_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    private void ensureErrorRowsIsMutable() {
      if (!((bitField0_ & 0x00000004) != 0)) {
        errorRows_ = new com.google.protobuf.LazyStringArrayList(errorRows_);
        bitField0_ |= 0x00000004;
       }
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @return A list containing the errorRows.
     */
    public com.google.protobuf.ProtocolStringList
        getErrorRowsList() {
      return errorRows_.getUnmodifiableView();
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @return The count of errorRows.
     */
    public int getErrorRowsCount() {
      return errorRows_.size();
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @param index The index of the element to return.
     * @return The errorRows at the given index.
     */
    public java.lang.String getErrorRows(int index) {
      return errorRows_.get(index);
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @param index The index of the value to return.
     * @return The bytes of the errorRows at the given index.
     */
    public com.google.protobuf.ByteString
        getErrorRowsBytes(int index) {
      return errorRows_.getByteString(index);
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @param index The index to set the value at.
     * @param value The errorRows to set.
     * @return This builder for chaining.
     */
    public Builder setErrorRows(
        int index, java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      ensureErrorRowsIsMutable();
      errorRows_.set(index, value);
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @param value The errorRows to add.
     * @return This builder for chaining.
     */
    public Builder addErrorRows(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      ensureErrorRowsIsMutable();
      errorRows_.add(value);
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @param values The errorRows to add.
     * @return This builder for chaining.
     */
    public Builder addAllErrorRows(
        java.lang.Iterable<java.lang.String> values) {
      ensureErrorRowsIsMutable();
      com.google.protobuf.AbstractMessageLite.Builder.addAll(
          values, errorRows_);
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @return This builder for chaining.
     */
    public Builder clearErrorRows() {
      errorRows_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000004);
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Number of rows with incorrectly-formatted data; not supported
     * </pre>
     *
     * <code>repeated string ErrorRows = 3;</code>
     * @param value The bytes of the errorRows to add.
     * @return This builder for chaining.
     */
    public Builder addErrorRowsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      ensureErrorRowsIsMutable();
      errorRows_.add(value);
      onChanged();
      return this;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:api.TransferStats)
  }

  // @@protoc_insertion_point(class_scope:api.TransferStats)
  private static final org.apache.flink.connector.jdbc.table.sink.api.TransferStats DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new org.apache.flink.connector.jdbc.table.sink.api.TransferStats();
  }

  public static org.apache.flink.connector.jdbc.table.sink.api.TransferStats getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<TransferStats>
      PARSER = new com.google.protobuf.AbstractParser<TransferStats>() {
    @java.lang.Override
    public TransferStats parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      Builder builder = newBuilder();
      try {
        builder.mergeFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(builder.buildPartial());
      } catch (com.google.protobuf.UninitializedMessageException e) {
        throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(e)
            .setUnfinishedMessage(builder.buildPartial());
      }
      return builder.buildPartial();
    }
  };

  public static com.google.protobuf.Parser<TransferStats> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<TransferStats> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public org.apache.flink.connector.jdbc.table.sink.api.TransferStats getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


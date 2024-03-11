package org.apache.flink.connector.jdbc.table.sink.api;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: adbpgss.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class GpssGrpc {

  private GpssGrpc() {}

  public static final java.lang.String SERVICE_NAME = "api.Gpss";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Session> getConnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Connect",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest.class,
      responseType = org.apache.flink.connector.jdbc.table.sink.api.Session.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Session> getConnectMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest, org.apache.flink.connector.jdbc.table.sink.api.Session> getConnectMethod;
    if ((getConnectMethod = GpssGrpc.getConnectMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getConnectMethod = GpssGrpc.getConnectMethod) == null) {
          GpssGrpc.getConnectMethod = getConnectMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest, org.apache.flink.connector.jdbc.table.sink.api.Session>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Connect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.Session.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Connect"))
              .build();
        }
      }
    }
    return getConnectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.Session,
      com.google.protobuf.Empty> getDisconnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Disconnect",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.Session.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.Session,
      com.google.protobuf.Empty> getDisconnectMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.Session, com.google.protobuf.Empty> getDisconnectMethod;
    if ((getDisconnectMethod = GpssGrpc.getDisconnectMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getDisconnectMethod = GpssGrpc.getDisconnectMethod) == null) {
          GpssGrpc.getDisconnectMethod = getDisconnectMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.Session, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Disconnect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.Session.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Disconnect"))
              .build();
        }
      }
    }
    return getDisconnectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.OpenRequest,
      com.google.protobuf.Empty> getOpenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Open",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.OpenRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.OpenRequest,
      com.google.protobuf.Empty> getOpenMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.OpenRequest, com.google.protobuf.Empty> getOpenMethod;
    if ((getOpenMethod = GpssGrpc.getOpenMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getOpenMethod = GpssGrpc.getOpenMethod) == null) {
          GpssGrpc.getOpenMethod = getOpenMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.OpenRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Open"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.OpenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Open"))
              .build();
        }
      }
    }
    return getOpenMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.WriteRequest,
      com.google.protobuf.Empty> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Write",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.WriteRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.WriteRequest,
      com.google.protobuf.Empty> getWriteMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.WriteRequest, com.google.protobuf.Empty> getWriteMethod;
    if ((getWriteMethod = GpssGrpc.getWriteMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getWriteMethod = GpssGrpc.getWriteMethod) == null) {
          GpssGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.WriteRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.WriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.CloseRequest,
      org.apache.flink.connector.jdbc.table.sink.api.TransferStats> getCloseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Close",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.CloseRequest.class,
      responseType = org.apache.flink.connector.jdbc.table.sink.api.TransferStats.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.CloseRequest,
      org.apache.flink.connector.jdbc.table.sink.api.TransferStats> getCloseMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.CloseRequest, org.apache.flink.connector.jdbc.table.sink.api.TransferStats> getCloseMethod;
    if ((getCloseMethod = GpssGrpc.getCloseMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getCloseMethod = GpssGrpc.getCloseMethod) == null) {
          GpssGrpc.getCloseMethod = getCloseMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.CloseRequest, org.apache.flink.connector.jdbc.table.sink.api.TransferStats>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Close"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.CloseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.TransferStats.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Close"))
              .build();
        }
      }
    }
    return getCloseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Schemas> getListSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSchema",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest.class,
      responseType = org.apache.flink.connector.jdbc.table.sink.api.Schemas.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Schemas> getListSchemaMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest, org.apache.flink.connector.jdbc.table.sink.api.Schemas> getListSchemaMethod;
    if ((getListSchemaMethod = GpssGrpc.getListSchemaMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getListSchemaMethod = GpssGrpc.getListSchemaMethod) == null) {
          GpssGrpc.getListSchemaMethod = getListSchemaMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest, org.apache.flink.connector.jdbc.table.sink.api.Schemas>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.Schemas.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("ListSchema"))
              .build();
        }
      }
    }
    return getListSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Tables> getListTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListTable",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest.class,
      responseType = org.apache.flink.connector.jdbc.table.sink.api.Tables.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Tables> getListTableMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest, org.apache.flink.connector.jdbc.table.sink.api.Tables> getListTableMethod;
    if ((getListTableMethod = GpssGrpc.getListTableMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getListTableMethod = GpssGrpc.getListTableMethod) == null) {
          GpssGrpc.getListTableMethod = getListTableMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest, org.apache.flink.connector.jdbc.table.sink.api.Tables>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.Tables.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("ListTable"))
              .build();
        }
      }
    }
    return getListTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Columns> getDescribeTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribeTable",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest.class,
      responseType = org.apache.flink.connector.jdbc.table.sink.api.Columns.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest,
      org.apache.flink.connector.jdbc.table.sink.api.Columns> getDescribeTableMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest, org.apache.flink.connector.jdbc.table.sink.api.Columns> getDescribeTableMethod;
    if ((getDescribeTableMethod = GpssGrpc.getDescribeTableMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getDescribeTableMethod = GpssGrpc.getDescribeTableMethod) == null) {
          GpssGrpc.getDescribeTableMethod = getDescribeTableMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest, org.apache.flink.connector.jdbc.table.sink.api.Columns>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribeTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.Columns.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("DescribeTable"))
              .build();
        }
      }
    }
    return getDescribeTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ReadRequest,
      org.apache.flink.connector.jdbc.table.sink.api.ReadResponse> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Read",
      requestType = org.apache.flink.connector.jdbc.table.sink.api.ReadRequest.class,
      responseType = org.apache.flink.connector.jdbc.table.sink.api.ReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ReadRequest,
      org.apache.flink.connector.jdbc.table.sink.api.ReadResponse> getReadMethod() {
    io.grpc.MethodDescriptor<org.apache.flink.connector.jdbc.table.sink.api.ReadRequest, org.apache.flink.connector.jdbc.table.sink.api.ReadResponse> getReadMethod;
    if ((getReadMethod = GpssGrpc.getReadMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getReadMethod = GpssGrpc.getReadMethod) == null) {
          GpssGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<org.apache.flink.connector.jdbc.table.sink.api.ReadRequest, org.apache.flink.connector.jdbc.table.sink.api.ReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.flink.connector.jdbc.table.sink.api.ReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GpssStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GpssStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GpssStub>() {
        @java.lang.Override
        public GpssStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GpssStub(channel, callOptions);
        }
      };
    return GpssStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GpssBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GpssBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GpssBlockingStub>() {
        @java.lang.Override
        public GpssBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GpssBlockingStub(channel, callOptions);
        }
      };
    return GpssBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GpssFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GpssFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GpssFutureStub>() {
        @java.lang.Override
        public GpssFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GpssFutureStub(channel, callOptions);
        }
      };
    return GpssFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    default void connect(org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Session> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getConnectMethod(), responseObserver);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    default void disconnect(org.apache.flink.connector.jdbc.table.sink.api.Session request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDisconnectMethod(), responseObserver);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    default void open(org.apache.flink.connector.jdbc.table.sink.api.OpenRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getOpenMethod(), responseObserver);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    default void write(org.apache.flink.connector.jdbc.table.sink.api.WriteRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    default void close(org.apache.flink.connector.jdbc.table.sink.api.CloseRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.TransferStats> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloseMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    default void listSchema(org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Schemas> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSchemaMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    default void listTable(org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Tables> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListTableMethod(), responseObserver);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    default void describeTable(org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Columns> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDescribeTableMethod(), responseObserver);
    }

    /**
     * <pre>
     * Not supported
     * </pre>
     */
    default void read(org.apache.flink.connector.jdbc.table.sink.api.ReadRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.ReadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Gpss.
   */
  public static abstract class GpssImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return GpssGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Gpss.
   */
  public static final class GpssStub
      extends io.grpc.stub.AbstractAsyncStub<GpssStub> {
    private GpssStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GpssStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GpssStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public void connect(org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Session> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public void disconnect(org.apache.flink.connector.jdbc.table.sink.api.Session request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDisconnectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public void open(org.apache.flink.connector.jdbc.table.sink.api.OpenRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public void write(org.apache.flink.connector.jdbc.table.sink.api.WriteRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public void close(org.apache.flink.connector.jdbc.table.sink.api.CloseRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.TransferStats> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCloseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public void listSchema(org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Schemas> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public void listTable(org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Tables> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public void describeTable(org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Columns> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDescribeTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Not supported
     * </pre>
     */
    public void read(org.apache.flink.connector.jdbc.table.sink.api.ReadRequest request,
        io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.ReadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Gpss.
   */
  public static final class GpssBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<GpssBlockingStub> {
    private GpssBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GpssBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GpssBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public org.apache.flink.connector.jdbc.table.sink.api.Session connect(org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getConnectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public com.google.protobuf.Empty disconnect(org.apache.flink.connector.jdbc.table.sink.api.Session request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDisconnectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public com.google.protobuf.Empty open(org.apache.flink.connector.jdbc.table.sink.api.OpenRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getOpenMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public com.google.protobuf.Empty write(org.apache.flink.connector.jdbc.table.sink.api.WriteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public org.apache.flink.connector.jdbc.table.sink.api.TransferStats close(org.apache.flink.connector.jdbc.table.sink.api.CloseRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCloseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public org.apache.flink.connector.jdbc.table.sink.api.Schemas listSchema(org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSchemaMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public org.apache.flink.connector.jdbc.table.sink.api.Tables listTable(org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public org.apache.flink.connector.jdbc.table.sink.api.Columns describeTable(org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDescribeTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Not supported
     * </pre>
     */
    public org.apache.flink.connector.jdbc.table.sink.api.ReadResponse read(org.apache.flink.connector.jdbc.table.sink.api.ReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Gpss.
   */
  public static final class GpssFutureStub
      extends io.grpc.stub.AbstractFutureStub<GpssFutureStub> {
    private GpssFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GpssFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GpssFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.flink.connector.jdbc.table.sink.api.Session> connect(
        org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> disconnect(
        org.apache.flink.connector.jdbc.table.sink.api.Session request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDisconnectMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> open(
        org.apache.flink.connector.jdbc.table.sink.api.OpenRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> write(
        org.apache.flink.connector.jdbc.table.sink.api.WriteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.flink.connector.jdbc.table.sink.api.TransferStats> close(
        org.apache.flink.connector.jdbc.table.sink.api.CloseRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCloseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.flink.connector.jdbc.table.sink.api.Schemas> listSchema(
        org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSchemaMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.flink.connector.jdbc.table.sink.api.Tables> listTable(
        org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.flink.connector.jdbc.table.sink.api.Columns> describeTable(
        org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDescribeTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Not supported
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.flink.connector.jdbc.table.sink.api.ReadResponse> read(
        org.apache.flink.connector.jdbc.table.sink.api.ReadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CONNECT = 0;
  private static final int METHODID_DISCONNECT = 1;
  private static final int METHODID_OPEN = 2;
  private static final int METHODID_WRITE = 3;
  private static final int METHODID_CLOSE = 4;
  private static final int METHODID_LIST_SCHEMA = 5;
  private static final int METHODID_LIST_TABLE = 6;
  private static final int METHODID_DESCRIBE_TABLE = 7;
  private static final int METHODID_READ = 8;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CONNECT:
          serviceImpl.connect((org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Session>) responseObserver);
          break;
        case METHODID_DISCONNECT:
          serviceImpl.disconnect((org.apache.flink.connector.jdbc.table.sink.api.Session) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_OPEN:
          serviceImpl.open((org.apache.flink.connector.jdbc.table.sink.api.OpenRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write((org.apache.flink.connector.jdbc.table.sink.api.WriteRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CLOSE:
          serviceImpl.close((org.apache.flink.connector.jdbc.table.sink.api.CloseRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.TransferStats>) responseObserver);
          break;
        case METHODID_LIST_SCHEMA:
          serviceImpl.listSchema((org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Schemas>) responseObserver);
          break;
        case METHODID_LIST_TABLE:
          serviceImpl.listTable((org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Tables>) responseObserver);
          break;
        case METHODID_DESCRIBE_TABLE:
          serviceImpl.describeTable((org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.Columns>) responseObserver);
          break;
        case METHODID_READ:
          serviceImpl.read((org.apache.flink.connector.jdbc.table.sink.api.ReadRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.flink.connector.jdbc.table.sink.api.ReadResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getConnectMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.ConnectRequest,
              org.apache.flink.connector.jdbc.table.sink.api.Session>(
                service, METHODID_CONNECT)))
        .addMethod(
          getDisconnectMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.Session,
              com.google.protobuf.Empty>(
                service, METHODID_DISCONNECT)))
        .addMethod(
          getOpenMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.OpenRequest,
              com.google.protobuf.Empty>(
                service, METHODID_OPEN)))
        .addMethod(
          getWriteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.WriteRequest,
              com.google.protobuf.Empty>(
                service, METHODID_WRITE)))
        .addMethod(
          getCloseMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.CloseRequest,
              org.apache.flink.connector.jdbc.table.sink.api.TransferStats>(
                service, METHODID_CLOSE)))
        .addMethod(
          getListSchemaMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.ListSchemaRequest,
              org.apache.flink.connector.jdbc.table.sink.api.Schemas>(
                service, METHODID_LIST_SCHEMA)))
        .addMethod(
          getListTableMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.ListTableRequest,
              org.apache.flink.connector.jdbc.table.sink.api.Tables>(
                service, METHODID_LIST_TABLE)))
        .addMethod(
          getDescribeTableMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.DescribeTableRequest,
              org.apache.flink.connector.jdbc.table.sink.api.Columns>(
                service, METHODID_DESCRIBE_TABLE)))
        .addMethod(
          getReadMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.flink.connector.jdbc.table.sink.api.ReadRequest,
              org.apache.flink.connector.jdbc.table.sink.api.ReadResponse>(
                service, METHODID_READ)))
        .build();
  }

  private static abstract class GpssBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GpssBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.flink.connector.jdbc.table.sink.api.Adbpgss.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Gpss");
    }
  }

  private static final class GpssFileDescriptorSupplier
      extends GpssBaseDescriptorSupplier {
    GpssFileDescriptorSupplier() {}
  }

  private static final class GpssMethodDescriptorSupplier
      extends GpssBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    GpssMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GpssGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GpssFileDescriptorSupplier())
              .addMethod(getConnectMethod())
              .addMethod(getDisconnectMethod())
              .addMethod(getOpenMethod())
              .addMethod(getWriteMethod())
              .addMethod(getCloseMethod())
              .addMethod(getListSchemaMethod())
              .addMethod(getListTableMethod())
              .addMethod(getDescribeTableMethod())
              .addMethod(getReadMethod())
              .build();
        }
      }
    }
    return result;
  }
}

package org.apache.flink.connector.jdbc.table.sink.api;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.28.1)",
    comments = "Source: gpss.proto")
public final class GpssGrpc {

  private GpssGrpc() {}

  public static final String SERVICE_NAME = "api.Gpss";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<ConnectRequest,
      Session> getConnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Connect",
      requestType = ConnectRequest.class,
      responseType = Session.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ConnectRequest,
      Session> getConnectMethod() {
    io.grpc.MethodDescriptor<ConnectRequest, Session> getConnectMethod;
    if ((getConnectMethod = GpssGrpc.getConnectMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getConnectMethod = GpssGrpc.getConnectMethod) == null) {
          GpssGrpc.getConnectMethod = getConnectMethod =
              io.grpc.MethodDescriptor.<ConnectRequest, Session>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Connect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ConnectRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Session.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Connect"))
              .build();
        }
      }
    }
    return getConnectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<Session,
      com.google.protobuf.Empty> getDisconnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Disconnect",
      requestType = Session.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<Session,
      com.google.protobuf.Empty> getDisconnectMethod() {
    io.grpc.MethodDescriptor<Session, com.google.protobuf.Empty> getDisconnectMethod;
    if ((getDisconnectMethod = GpssGrpc.getDisconnectMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getDisconnectMethod = GpssGrpc.getDisconnectMethod) == null) {
          GpssGrpc.getDisconnectMethod = getDisconnectMethod =
              io.grpc.MethodDescriptor.<Session, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Disconnect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Session.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Disconnect"))
              .build();
        }
      }
    }
    return getDisconnectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<OpenRequest,
      com.google.protobuf.Empty> getOpenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Open",
      requestType = OpenRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<OpenRequest,
      com.google.protobuf.Empty> getOpenMethod() {
    io.grpc.MethodDescriptor<OpenRequest, com.google.protobuf.Empty> getOpenMethod;
    if ((getOpenMethod = GpssGrpc.getOpenMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getOpenMethod = GpssGrpc.getOpenMethod) == null) {
          GpssGrpc.getOpenMethod = getOpenMethod =
              io.grpc.MethodDescriptor.<OpenRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Open"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  OpenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Open"))
              .build();
        }
      }
    }
    return getOpenMethod;
  }

  private static volatile io.grpc.MethodDescriptor<WriteRequest,
      com.google.protobuf.Empty> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Write",
      requestType = WriteRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<WriteRequest,
      com.google.protobuf.Empty> getWriteMethod() {
    io.grpc.MethodDescriptor<WriteRequest, com.google.protobuf.Empty> getWriteMethod;
    if ((getWriteMethod = GpssGrpc.getWriteMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getWriteMethod = GpssGrpc.getWriteMethod) == null) {
          GpssGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<WriteRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  WriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<CloseRequest,
      TransferStats> getCloseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Close",
      requestType = CloseRequest.class,
      responseType = TransferStats.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<CloseRequest,
      TransferStats> getCloseMethod() {
    io.grpc.MethodDescriptor<CloseRequest, TransferStats> getCloseMethod;
    if ((getCloseMethod = GpssGrpc.getCloseMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getCloseMethod = GpssGrpc.getCloseMethod) == null) {
          GpssGrpc.getCloseMethod = getCloseMethod =
              io.grpc.MethodDescriptor.<CloseRequest, TransferStats>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Close"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CloseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  TransferStats.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("Close"))
              .build();
        }
      }
    }
    return getCloseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<ListSchemaRequest,
      Schemas> getListSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSchema",
      requestType = ListSchemaRequest.class,
      responseType = Schemas.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ListSchemaRequest,
      Schemas> getListSchemaMethod() {
    io.grpc.MethodDescriptor<ListSchemaRequest, Schemas> getListSchemaMethod;
    if ((getListSchemaMethod = GpssGrpc.getListSchemaMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getListSchemaMethod = GpssGrpc.getListSchemaMethod) == null) {
          GpssGrpc.getListSchemaMethod = getListSchemaMethod =
              io.grpc.MethodDescriptor.<ListSchemaRequest, Schemas>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ListSchemaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Schemas.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("ListSchema"))
              .build();
        }
      }
    }
    return getListSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<ListTableRequest,
      Tables> getListTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListTable",
      requestType = ListTableRequest.class,
      responseType = Tables.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ListTableRequest,
      Tables> getListTableMethod() {
    io.grpc.MethodDescriptor<ListTableRequest, Tables> getListTableMethod;
    if ((getListTableMethod = GpssGrpc.getListTableMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getListTableMethod = GpssGrpc.getListTableMethod) == null) {
          GpssGrpc.getListTableMethod = getListTableMethod =
              io.grpc.MethodDescriptor.<ListTableRequest, Tables>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ListTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Tables.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("ListTable"))
              .build();
        }
      }
    }
    return getListTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<DescribeTableRequest,
      Columns> getDescribeTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribeTable",
      requestType = DescribeTableRequest.class,
      responseType = Columns.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<DescribeTableRequest,
      Columns> getDescribeTableMethod() {
    io.grpc.MethodDescriptor<DescribeTableRequest, Columns> getDescribeTableMethod;
    if ((getDescribeTableMethod = GpssGrpc.getDescribeTableMethod) == null) {
      synchronized (GpssGrpc.class) {
        if ((getDescribeTableMethod = GpssGrpc.getDescribeTableMethod) == null) {
          GpssGrpc.getDescribeTableMethod = getDescribeTableMethod =
              io.grpc.MethodDescriptor.<DescribeTableRequest, Columns>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribeTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  DescribeTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  Columns.getDefaultInstance()))
              .setSchemaDescriptor(new GpssMethodDescriptorSupplier("DescribeTable"))
              .build();
        }
      }
    }
    return getDescribeTableMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GpssStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GpssStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GpssStub>() {
        @Override
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
        @Override
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
        @Override
        public GpssFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GpssFutureStub(channel, callOptions);
        }
      };
    return GpssFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class GpssImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public void connect(ConnectRequest request,
                        io.grpc.stub.StreamObserver<Session> responseObserver) {
      asyncUnimplementedUnaryCall(getConnectMethod(), responseObserver);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public void disconnect(Session request,
                           io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(getDisconnectMethod(), responseObserver);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public void open(OpenRequest request,
                     io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(getOpenMethod(), responseObserver);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public void write(WriteRequest request,
                      io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public void close(CloseRequest request,
                      io.grpc.stub.StreamObserver<TransferStats> responseObserver) {
      asyncUnimplementedUnaryCall(getCloseMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public void listSchema(ListSchemaRequest request,
                           io.grpc.stub.StreamObserver<Schemas> responseObserver) {
      asyncUnimplementedUnaryCall(getListSchemaMethod(), responseObserver);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public void listTable(ListTableRequest request,
                          io.grpc.stub.StreamObserver<Tables> responseObserver) {
      asyncUnimplementedUnaryCall(getListTableMethod(), responseObserver);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public void describeTable(DescribeTableRequest request,
                              io.grpc.stub.StreamObserver<Columns> responseObserver) {
      asyncUnimplementedUnaryCall(getDescribeTableMethod(), responseObserver);
    }

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getConnectMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ConnectRequest,
                Session>(
                  this, METHODID_CONNECT)))
          .addMethod(
            getDisconnectMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                Session,
                com.google.protobuf.Empty>(
                  this, METHODID_DISCONNECT)))
          .addMethod(
            getOpenMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                OpenRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_OPEN)))
          .addMethod(
            getWriteMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                WriteRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_WRITE)))
          .addMethod(
            getCloseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                CloseRequest,
                TransferStats>(
                  this, METHODID_CLOSE)))
          .addMethod(
            getListSchemaMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ListSchemaRequest,
                Schemas>(
                  this, METHODID_LIST_SCHEMA)))
          .addMethod(
            getListTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ListTableRequest,
                Tables>(
                  this, METHODID_LIST_TABLE)))
          .addMethod(
            getDescribeTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                DescribeTableRequest,
                Columns>(
                  this, METHODID_DESCRIBE_TABLE)))
          .build();
    }
  }

  /**
   */
  public static final class GpssStub extends io.grpc.stub.AbstractAsyncStub<GpssStub> {
    private GpssStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected GpssStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GpssStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public void connect(ConnectRequest request,
                        io.grpc.stub.StreamObserver<Session> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public void disconnect(Session request,
                           io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDisconnectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public void open(OpenRequest request,
                     io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public void write(WriteRequest request,
                      io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public void close(CloseRequest request,
                      io.grpc.stub.StreamObserver<TransferStats> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCloseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public void listSchema(ListSchemaRequest request,
                           io.grpc.stub.StreamObserver<Schemas> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getListSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public void listTable(ListTableRequest request,
                          io.grpc.stub.StreamObserver<Tables> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getListTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public void describeTable(DescribeTableRequest request,
                              io.grpc.stub.StreamObserver<Columns> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDescribeTableMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GpssBlockingStub extends io.grpc.stub.AbstractBlockingStub<GpssBlockingStub> {
    private GpssBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected GpssBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GpssBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public Session connect(ConnectRequest request) {
      return blockingUnaryCall(
          getChannel(), getConnectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public com.google.protobuf.Empty disconnect(Session request) {
      return blockingUnaryCall(
          getChannel(), getDisconnectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public com.google.protobuf.Empty open(OpenRequest request) {
      return blockingUnaryCall(
          getChannel(), getOpenMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public com.google.protobuf.Empty write(WriteRequest request) {
      return blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public TransferStats close(CloseRequest request) {
      return blockingUnaryCall(
          getChannel(), getCloseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public Schemas listSchema(ListSchemaRequest request) {
      return blockingUnaryCall(
          getChannel(), getListSchemaMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public Tables listTable(ListTableRequest request) {
      return blockingUnaryCall(
          getChannel(), getListTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public Columns describeTable(DescribeTableRequest request) {
      return blockingUnaryCall(
          getChannel(), getDescribeTableMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GpssFutureStub extends io.grpc.stub.AbstractFutureStub<GpssFutureStub> {
    private GpssFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected GpssFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GpssFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Session> connect(
        ConnectRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> disconnect(
        Session request) {
      return futureUnaryCall(
          getChannel().newCall(getDisconnectMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> open(
        OpenRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> write(
        WriteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<TransferStats> close(
        CloseRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCloseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Schemas> listSchema(
        ListSchemaRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListSchemaMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Tables> listTable(
        ListTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Columns> describeTable(
        DescribeTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDescribeTableMethod(), getCallOptions()), request);
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

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GpssImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GpssImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CONNECT:
          serviceImpl.connect((ConnectRequest) request,
              (io.grpc.stub.StreamObserver<Session>) responseObserver);
          break;
        case METHODID_DISCONNECT:
          serviceImpl.disconnect((Session) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_OPEN:
          serviceImpl.open((OpenRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write((WriteRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CLOSE:
          serviceImpl.close((CloseRequest) request,
              (io.grpc.stub.StreamObserver<TransferStats>) responseObserver);
          break;
        case METHODID_LIST_SCHEMA:
          serviceImpl.listSchema((ListSchemaRequest) request,
              (io.grpc.stub.StreamObserver<Schemas>) responseObserver);
          break;
        case METHODID_LIST_TABLE:
          serviceImpl.listTable((ListTableRequest) request,
              (io.grpc.stub.StreamObserver<Tables>) responseObserver);
          break;
        case METHODID_DESCRIBE_TABLE:
          serviceImpl.describeTable((DescribeTableRequest) request,
              (io.grpc.stub.StreamObserver<Columns>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class GpssBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GpssBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return GpssOuterClass.getDescriptor();
    }

    @Override
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
    private final String methodName;

    GpssMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
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
              .build();
        }
      }
    }
    return result;
  }
}

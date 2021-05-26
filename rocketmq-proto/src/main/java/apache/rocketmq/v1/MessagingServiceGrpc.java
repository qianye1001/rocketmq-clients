package apache.rocketmq.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.35.0)",
    comments = "Source: apache/rocketmq/v1/service.proto")
public final class MessagingServiceGrpc {

  private MessagingServiceGrpc() {}

  public static final String SERVICE_NAME = "apache.rocketmq.v1.MessagingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryRouteRequest,
      apache.rocketmq.v1.QueryRouteResponse> getQueryRouteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryRoute",
      requestType = apache.rocketmq.v1.QueryRouteRequest.class,
      responseType = apache.rocketmq.v1.QueryRouteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryRouteRequest,
      apache.rocketmq.v1.QueryRouteResponse> getQueryRouteMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryRouteRequest, apache.rocketmq.v1.QueryRouteResponse> getQueryRouteMethod;
    if ((getQueryRouteMethod = MessagingServiceGrpc.getQueryRouteMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getQueryRouteMethod = MessagingServiceGrpc.getQueryRouteMethod) == null) {
          MessagingServiceGrpc.getQueryRouteMethod = getQueryRouteMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.QueryRouteRequest, apache.rocketmq.v1.QueryRouteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryRoute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryRouteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryRouteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("QueryRoute"))
              .build();
        }
      }
    }
    return getQueryRouteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.HeartbeatRequest,
      apache.rocketmq.v1.HeartbeatResponse> getHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Heartbeat",
      requestType = apache.rocketmq.v1.HeartbeatRequest.class,
      responseType = apache.rocketmq.v1.HeartbeatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.HeartbeatRequest,
      apache.rocketmq.v1.HeartbeatResponse> getHeartbeatMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.HeartbeatRequest, apache.rocketmq.v1.HeartbeatResponse> getHeartbeatMethod;
    if ((getHeartbeatMethod = MessagingServiceGrpc.getHeartbeatMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getHeartbeatMethod = MessagingServiceGrpc.getHeartbeatMethod) == null) {
          MessagingServiceGrpc.getHeartbeatMethod = getHeartbeatMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.HeartbeatRequest, apache.rocketmq.v1.HeartbeatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Heartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HeartbeatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HeartbeatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("Heartbeat"))
              .build();
        }
      }
    }
    return getHeartbeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.HealthCheckRequest,
      apache.rocketmq.v1.HealthCheckResponse> getHealthCheckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HealthCheck",
      requestType = apache.rocketmq.v1.HealthCheckRequest.class,
      responseType = apache.rocketmq.v1.HealthCheckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.HealthCheckRequest,
      apache.rocketmq.v1.HealthCheckResponse> getHealthCheckMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.HealthCheckRequest, apache.rocketmq.v1.HealthCheckResponse> getHealthCheckMethod;
    if ((getHealthCheckMethod = MessagingServiceGrpc.getHealthCheckMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getHealthCheckMethod = MessagingServiceGrpc.getHealthCheckMethod) == null) {
          MessagingServiceGrpc.getHealthCheckMethod = getHealthCheckMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.HealthCheckRequest, apache.rocketmq.v1.HealthCheckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HealthCheck"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HealthCheckRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HealthCheckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("HealthCheck"))
              .build();
        }
      }
    }
    return getHealthCheckMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.SendMessageRequest,
      apache.rocketmq.v1.SendMessageResponse> getSendMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendMessage",
      requestType = apache.rocketmq.v1.SendMessageRequest.class,
      responseType = apache.rocketmq.v1.SendMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.SendMessageRequest,
      apache.rocketmq.v1.SendMessageResponse> getSendMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.SendMessageRequest, apache.rocketmq.v1.SendMessageResponse> getSendMessageMethod;
    if ((getSendMessageMethod = MessagingServiceGrpc.getSendMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getSendMessageMethod = MessagingServiceGrpc.getSendMessageMethod) == null) {
          MessagingServiceGrpc.getSendMessageMethod = getSendMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.SendMessageRequest, apache.rocketmq.v1.SendMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.SendMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.SendMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("SendMessage"))
              .build();
        }
      }
    }
    return getSendMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryAssignmentRequest,
      apache.rocketmq.v1.QueryAssignmentResponse> getQueryAssignmentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryAssignment",
      requestType = apache.rocketmq.v1.QueryAssignmentRequest.class,
      responseType = apache.rocketmq.v1.QueryAssignmentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryAssignmentRequest,
      apache.rocketmq.v1.QueryAssignmentResponse> getQueryAssignmentMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryAssignmentRequest, apache.rocketmq.v1.QueryAssignmentResponse> getQueryAssignmentMethod;
    if ((getQueryAssignmentMethod = MessagingServiceGrpc.getQueryAssignmentMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getQueryAssignmentMethod = MessagingServiceGrpc.getQueryAssignmentMethod) == null) {
          MessagingServiceGrpc.getQueryAssignmentMethod = getQueryAssignmentMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.QueryAssignmentRequest, apache.rocketmq.v1.QueryAssignmentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryAssignment"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryAssignmentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryAssignmentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("QueryAssignment"))
              .build();
        }
      }
    }
    return getQueryAssignmentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.ReceiveMessageRequest,
      apache.rocketmq.v1.ReceiveMessageResponse> getReceiveMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReceiveMessage",
      requestType = apache.rocketmq.v1.ReceiveMessageRequest.class,
      responseType = apache.rocketmq.v1.ReceiveMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.ReceiveMessageRequest,
      apache.rocketmq.v1.ReceiveMessageResponse> getReceiveMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.ReceiveMessageRequest, apache.rocketmq.v1.ReceiveMessageResponse> getReceiveMessageMethod;
    if ((getReceiveMessageMethod = MessagingServiceGrpc.getReceiveMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getReceiveMessageMethod = MessagingServiceGrpc.getReceiveMessageMethod) == null) {
          MessagingServiceGrpc.getReceiveMessageMethod = getReceiveMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.ReceiveMessageRequest, apache.rocketmq.v1.ReceiveMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReceiveMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ReceiveMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ReceiveMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("ReceiveMessage"))
              .build();
        }
      }
    }
    return getReceiveMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.AckMessageRequest,
      apache.rocketmq.v1.AckMessageResponse> getAckMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AckMessage",
      requestType = apache.rocketmq.v1.AckMessageRequest.class,
      responseType = apache.rocketmq.v1.AckMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.AckMessageRequest,
      apache.rocketmq.v1.AckMessageResponse> getAckMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.AckMessageRequest, apache.rocketmq.v1.AckMessageResponse> getAckMessageMethod;
    if ((getAckMessageMethod = MessagingServiceGrpc.getAckMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getAckMessageMethod = MessagingServiceGrpc.getAckMessageMethod) == null) {
          MessagingServiceGrpc.getAckMessageMethod = getAckMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.AckMessageRequest, apache.rocketmq.v1.AckMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AckMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.AckMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.AckMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("AckMessage"))
              .build();
        }
      }
    }
    return getAckMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.NackMessageRequest,
      apache.rocketmq.v1.NackMessageResponse> getNackMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NackMessage",
      requestType = apache.rocketmq.v1.NackMessageRequest.class,
      responseType = apache.rocketmq.v1.NackMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.NackMessageRequest,
      apache.rocketmq.v1.NackMessageResponse> getNackMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.NackMessageRequest, apache.rocketmq.v1.NackMessageResponse> getNackMessageMethod;
    if ((getNackMessageMethod = MessagingServiceGrpc.getNackMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getNackMessageMethod = MessagingServiceGrpc.getNackMessageMethod) == null) {
          MessagingServiceGrpc.getNackMessageMethod = getNackMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.NackMessageRequest, apache.rocketmq.v1.NackMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NackMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.NackMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.NackMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("NackMessage"))
              .build();
        }
      }
    }
    return getNackMessageMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MessagingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceStub>() {
        @java.lang.Override
        public MessagingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceStub(channel, callOptions);
        }
      };
    return MessagingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MessagingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingStub>() {
        @java.lang.Override
        public MessagingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceBlockingStub(channel, callOptions);
        }
      };
    return MessagingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MessagingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceFutureStub>() {
        @java.lang.Override
        public MessagingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceFutureStub(channel, callOptions);
        }
      };
    return MessagingServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class MessagingServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void queryRoute(apache.rocketmq.v1.QueryRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryRouteMethod(), responseObserver);
    }

    /**
     */
    public void heartbeat(apache.rocketmq.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }

    /**
     */
    public void healthCheck(apache.rocketmq.v1.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHealthCheckMethod(), responseObserver);
    }

    /**
     */
    public void sendMessage(apache.rocketmq.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessageMethod(), responseObserver);
    }

    /**
     */
    public void queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryAssignmentMethod(), responseObserver);
    }

    /**
     */
    public void receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReceiveMessageMethod(), responseObserver);
    }

    /**
     */
    public void ackMessage(apache.rocketmq.v1.AckMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckMessageMethod(), responseObserver);
    }

    /**
     */
    public void nackMessage(apache.rocketmq.v1.NackMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNackMessageMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getQueryRouteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.QueryRouteRequest,
                apache.rocketmq.v1.QueryRouteResponse>(
                  this, METHODID_QUERY_ROUTE)))
          .addMethod(
            getHeartbeatMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.HeartbeatRequest,
                apache.rocketmq.v1.HeartbeatResponse>(
                  this, METHODID_HEARTBEAT)))
          .addMethod(
            getHealthCheckMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.HealthCheckRequest,
                apache.rocketmq.v1.HealthCheckResponse>(
                  this, METHODID_HEALTH_CHECK)))
          .addMethod(
            getSendMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.SendMessageRequest,
                apache.rocketmq.v1.SendMessageResponse>(
                  this, METHODID_SEND_MESSAGE)))
          .addMethod(
            getQueryAssignmentMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.QueryAssignmentRequest,
                apache.rocketmq.v1.QueryAssignmentResponse>(
                  this, METHODID_QUERY_ASSIGNMENT)))
          .addMethod(
            getReceiveMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.ReceiveMessageRequest,
                apache.rocketmq.v1.ReceiveMessageResponse>(
                  this, METHODID_RECEIVE_MESSAGE)))
          .addMethod(
            getAckMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.AckMessageRequest,
                apache.rocketmq.v1.AckMessageResponse>(
                  this, METHODID_ACK_MESSAGE)))
          .addMethod(
            getNackMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.NackMessageRequest,
                apache.rocketmq.v1.NackMessageResponse>(
                  this, METHODID_NACK_MESSAGE)))
          .build();
    }
  }

  /**
   */
  public static final class MessagingServiceStub extends io.grpc.stub.AbstractAsyncStub<MessagingServiceStub> {
    private MessagingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceStub(channel, callOptions);
    }

    /**
     */
    public void queryRoute(apache.rocketmq.v1.QueryRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryRouteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeat(apache.rocketmq.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void healthCheck(apache.rocketmq.v1.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendMessage(apache.rocketmq.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReceiveMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void ackMessage(apache.rocketmq.v1.AckMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void nackMessage(apache.rocketmq.v1.NackMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MessagingServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<MessagingServiceBlockingStub> {
    private MessagingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public apache.rocketmq.v1.QueryRouteResponse queryRoute(apache.rocketmq.v1.QueryRouteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryRouteMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.HeartbeatResponse heartbeat(apache.rocketmq.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.HealthCheckResponse healthCheck(apache.rocketmq.v1.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHealthCheckMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.SendMessageResponse sendMessage(apache.rocketmq.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.QueryAssignmentResponse queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryAssignmentMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.ReceiveMessageResponse receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReceiveMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.AckMessageResponse ackMessage(apache.rocketmq.v1.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.NackMessageResponse nackMessage(apache.rocketmq.v1.NackMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNackMessageMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MessagingServiceFutureStub extends io.grpc.stub.AbstractFutureStub<MessagingServiceFutureStub> {
    private MessagingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryRouteResponse> queryRoute(
        apache.rocketmq.v1.QueryRouteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryRouteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.HeartbeatResponse> heartbeat(
        apache.rocketmq.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.HealthCheckResponse> healthCheck(
        apache.rocketmq.v1.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.SendMessageResponse> sendMessage(
        apache.rocketmq.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryAssignmentResponse> queryAssignment(
        apache.rocketmq.v1.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.ReceiveMessageResponse> receiveMessage(
        apache.rocketmq.v1.ReceiveMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReceiveMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.AckMessageResponse> ackMessage(
        apache.rocketmq.v1.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.NackMessageResponse> nackMessage(
        apache.rocketmq.v1.NackMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_ROUTE = 0;
  private static final int METHODID_HEARTBEAT = 1;
  private static final int METHODID_HEALTH_CHECK = 2;
  private static final int METHODID_SEND_MESSAGE = 3;
  private static final int METHODID_QUERY_ASSIGNMENT = 4;
  private static final int METHODID_RECEIVE_MESSAGE = 5;
  private static final int METHODID_ACK_MESSAGE = 6;
  private static final int METHODID_NACK_MESSAGE = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MessagingServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MessagingServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_QUERY_ROUTE:
          serviceImpl.queryRoute((apache.rocketmq.v1.QueryRouteRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((apache.rocketmq.v1.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse>) responseObserver);
          break;
        case METHODID_HEALTH_CHECK:
          serviceImpl.healthCheck((apache.rocketmq.v1.HealthCheckRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse>) responseObserver);
          break;
        case METHODID_SEND_MESSAGE:
          serviceImpl.sendMessage((apache.rocketmq.v1.SendMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse>) responseObserver);
          break;
        case METHODID_QUERY_ASSIGNMENT:
          serviceImpl.queryAssignment((apache.rocketmq.v1.QueryAssignmentRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse>) responseObserver);
          break;
        case METHODID_RECEIVE_MESSAGE:
          serviceImpl.receiveMessage((apache.rocketmq.v1.ReceiveMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse>) responseObserver);
          break;
        case METHODID_ACK_MESSAGE:
          serviceImpl.ackMessage((apache.rocketmq.v1.AckMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse>) responseObserver);
          break;
        case METHODID_NACK_MESSAGE:
          serviceImpl.nackMessage((apache.rocketmq.v1.NackMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse>) responseObserver);
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

  private static abstract class MessagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MessagingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return apache.rocketmq.v1.MQService.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MessagingService");
    }
  }

  private static final class MessagingServiceFileDescriptorSupplier
      extends MessagingServiceBaseDescriptorSupplier {
    MessagingServiceFileDescriptorSupplier() {}
  }

  private static final class MessagingServiceMethodDescriptorSupplier
      extends MessagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MessagingServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (MessagingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MessagingServiceFileDescriptorSupplier())
              .addMethod(getQueryRouteMethod())
              .addMethod(getHeartbeatMethod())
              .addMethod(getHealthCheckMethod())
              .addMethod(getSendMessageMethod())
              .addMethod(getQueryAssignmentMethod())
              .addMethod(getReceiveMessageMethod())
              .addMethod(getAckMessageMethod())
              .addMethod(getNackMessageMethod())
              .build();
        }
      }
    }
    return result;
  }
}

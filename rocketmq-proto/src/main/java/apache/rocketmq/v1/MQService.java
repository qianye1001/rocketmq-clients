// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/service.proto

package apache.rocketmq.v1;

public final class MQService {
  private MQService() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_RequestCommon_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_RequestCommon_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_ResponseCommon_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_ResponseCommon_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_QueryRouteRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_QueryRouteRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_QueryRouteResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_QueryRouteResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_SendMessageRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_SendMessageRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_SendMessageResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_SendMessageResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_QueryAssignmentRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_QueryAssignmentRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_QueryAssignmentResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_QueryAssignmentResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_ReceiveMessageRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_ReceiveMessageRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_ReceiveMessageResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_ReceiveMessageResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_AckMessageRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_AckMessageRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_AckMessageResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_AckMessageResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_NackMessageRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_NackMessageRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_NackMessageResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_NackMessageResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_HeartbeatRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_HeartbeatRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_HeartbeatResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_HeartbeatResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_HealthCheckRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_HealthCheckRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_HealthCheckResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_HealthCheckResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n apache/rocketmq/v1/service.proto\022\022apac" +
      "he.rocketmq.v1\032\031google/protobuf/any.prot" +
      "o\032\036google/protobuf/duration.proto\032\037googl" +
      "e/protobuf/timestamp.proto\032\036google/rpc/e" +
      "rror_details.proto\032\025google/rpc/code.prot" +
      "o\032\027google/rpc/status.proto\032#apache/rocke" +
      "tmq/v1/definition.proto\"\205\001\n\rRequestCommo" +
      "n\022.\n\010language\030\001 \001(\0162\034.apache.rocketmq.v1" +
      ".Language\022\026\n\016client_version\030\002 \001(\t\022\030\n\020pro" +
      "tocol_version\030\003 \001(\t\022\022\n\nrequest_id\030\004 \001(\t\"" +
      "\230\002\n\016ResponseCommon\022\022\n\nrequest_id\030\001 \001(\t\022\"" +
      "\n\006status\030\002 \001(\0132\022.google.rpc.Status\022-\n\014re" +
      "quest_info\030\003 \001(\0132\027.google.rpc.RequestInf" +
      "o\022\036\n\004help\030\004 \001(\0132\020.google.rpc.Help\022)\n\nret" +
      "ry_info\030\005 \001(\0132\025.google.rpc.RetryInfo\022)\n\n" +
      "debug_info\030\006 \001(\0132\025.google.rpc.DebugInfo\022" +
      ")\n\nerror_info\030\007 \001(\0132\025.google.rpc.ErrorIn" +
      "fo\"s\n\021QueryRouteRequest\0221\n\006common\030\001 \001(\0132" +
      "!.apache.rocketmq.v1.RequestCommon\022+\n\005to" +
      "pic\030\002 \001(\0132\034.apache.rocketmq.v1.Resource\"" +
      "{\n\022QueryRouteResponse\0222\n\006common\030\001 \001(\0132\"." +
      "apache.rocketmq.v1.ResponseCommon\0221\n\npar" +
      "titions\030\002 \003(\0132\035.apache.rocketmq.v1.Parti" +
      "tion\"u\n\022SendMessageRequest\0221\n\006common\030\001 \001" +
      "(\0132!.apache.rocketmq.v1.RequestCommon\022,\n" +
      "\007message\030\002 \001(\0132\033.apache.rocketmq.v1.Mess" +
      "age\"]\n\023SendMessageResponse\0222\n\006common\030\001 \001" +
      "(\0132\".apache.rocketmq.v1.ResponseCommon\022\022" +
      "\n\nmessage_id\030\002 \001(\t\"\361\001\n\026QueryAssignmentRe" +
      "quest\0221\n\006common\030\001 \001(\0132!.apache.rocketmq." +
      "v1.RequestCommon\022+\n\005topic\030\002 \001(\0132\034.apache" +
      ".rocketmq.v1.Resource\022+\n\005group\030\003 \001(\0132\034.a" +
      "pache.rocketmq.v1.Resource\022\021\n\tclient_id\030" +
      "\004 \001(\t\0227\n\rconsume_model\030\005 \001(\0162 .apache.ro" +
      "cketmq.v1.ConsumeModel\"\213\001\n\027QueryAssignme" +
      "ntResponse\0222\n\006common\030\001 \001(\0132\".apache.rock" +
      "etmq.v1.ResponseCommon\022<\n\020load_assignmen" +
      "ts\030\002 \003(\0132\".apache.rocketmq.v1.LoadAssign" +
      "ment\"\360\003\n\025ReceiveMessageRequest\0221\n\006common" +
      "\030\001 \001(\0132!.apache.rocketmq.v1.RequestCommo" +
      "n\022+\n\005group\030\002 \001(\0132\034.apache.rocketmq.v1.Re" +
      "source\022\021\n\tclient_id\030\003 \001(\t\0220\n\tpartition\030\004" +
      " \001(\0132\035.apache.rocketmq.v1.Partition\022?\n\021f" +
      "ilter_expression\030\005 \001(\0132$.apache.rocketmq" +
      ".v1.FilterExpression\0229\n\016consume_policy\030\006" +
      " \001(\0162!.apache.rocketmq.v1.ConsumePolicy\022" +
      "<\n\030initialization_timestamp\030\007 \001(\0132\032.goog" +
      "le.protobuf.Timestamp\022\022\n\nbatch_size\030\010 \001(" +
      "\005\0225\n\022invisible_duration\030\t \001(\0132\031.google.p" +
      "rotobuf.Duration\022-\n\nawait_time\030\n \001(\0132\031.g" +
      "oogle.protobuf.Duration\"\352\001\n\026ReceiveMessa" +
      "geResponse\0222\n\006common\030\001 \001(\0132\".apache.rock" +
      "etmq.v1.ResponseCommon\022-\n\010messages\030\002 \003(\013" +
      "2\033.apache.rocketmq.v1.Message\0226\n\022deliver" +
      "y_timestamp\030\003 \001(\0132\032.google.protobuf.Time" +
      "stamp\0225\n\022invisible_duration\030\004 \001(\0132\031.goog" +
      "le.protobuf.Duration\"\337\001\n\021AckMessageReque" +
      "st\0221\n\006common\030\001 \001(\0132!.apache.rocketmq.v1." +
      "RequestCommon\022+\n\005group\030\002 \001(\0132\034.apache.ro" +
      "cketmq.v1.Resource\022+\n\005topic\030\003 \001(\0132\034.apac" +
      "he.rocketmq.v1.Resource\022\021\n\tclient_id\030\004 \001" +
      "(\t\022\026\n\016receipt_handle\030\005 \001(\t\022\022\n\nmessage_id" +
      "\030\006 \001(\t\"H\n\022AckMessageResponse\0222\n\006common\030\001" +
      " \001(\0132\".apache.rocketmq.v1.ResponseCommon" +
      "\"\226\002\n\022NackMessageRequest\0221\n\006common\030\001 \001(\0132" +
      "!.apache.rocketmq.v1.RequestCommon\022+\n\005gr" +
      "oup\030\002 \001(\0132\034.apache.rocketmq.v1.Resource\022" +
      "+\n\005topic\030\003 \001(\0132\034.apache.rocketmq.v1.Reso" +
      "urce\022\021\n\tclient_id\030\004 \001(\t\022\026\n\016receipt_handl" +
      "e\030\005 \001(\t\022\022\n\nmessage_id\030\006 \001(\t\022\027\n\017reconsume" +
      "_times\030\007 \001(\005\022\033\n\023max_reconsume_times\030\010 \001(" +
      "\005\"I\n\023NackMessageResponse\0222\n\006common\030\001 \001(\013" +
      "2\".apache.rocketmq.v1.ResponseCommon\"}\n\020" +
      "HeartbeatRequest\0221\n\006common\030\001 \001(\0132!.apach" +
      "e.rocketmq.v1.RequestCommon\0226\n\nheartbeat" +
      "s\030\002 \003(\0132\".apache.rocketmq.v1.HeartbeatEn" +
      "try\"G\n\021HeartbeatResponse\0222\n\006common\030\001 \001(\013" +
      "2\".apache.rocketmq.v1.ResponseCommon\"\\\n\022" +
      "HealthCheckRequest\0221\n\006common\030\001 \001(\0132!.apa" +
      "che.rocketmq.v1.RequestCommon\022\023\n\013client_" +
      "host\030\002 \001(\t\"I\n\023HealthCheckResponse\0222\n\006com" +
      "mon\030\001 \001(\0132\".apache.rocketmq.v1.ResponseC" +
      "ommon2\253\006\n\020MessagingService\022]\n\nQueryRoute" +
      "\022%.apache.rocketmq.v1.QueryRouteRequest\032" +
      "&.apache.rocketmq.v1.QueryRouteResponse\"" +
      "\000\022Z\n\tHeartbeat\022$.apache.rocketmq.v1.Hear" +
      "tbeatRequest\032%.apache.rocketmq.v1.Heartb" +
      "eatResponse\"\000\022`\n\013HealthCheck\022&.apache.ro" +
      "cketmq.v1.HealthCheckRequest\032\'.apache.ro" +
      "cketmq.v1.HealthCheckResponse\"\000\022`\n\013SendM" +
      "essage\022&.apache.rocketmq.v1.SendMessageR" +
      "equest\032\'.apache.rocketmq.v1.SendMessageR" +
      "esponse\"\000\022l\n\017QueryAssignment\022*.apache.ro" +
      "cketmq.v1.QueryAssignmentRequest\032+.apach" +
      "e.rocketmq.v1.QueryAssignmentResponse\"\000\022" +
      "i\n\016ReceiveMessage\022).apache.rocketmq.v1.R" +
      "eceiveMessageRequest\032*.apache.rocketmq.v" +
      "1.ReceiveMessageResponse\"\000\022]\n\nAckMessage" +
      "\022%.apache.rocketmq.v1.AckMessageRequest\032" +
      "&.apache.rocketmq.v1.AckMessageResponse\"" +
      "\000\022`\n\013NackMessage\022&.apache.rocketmq.v1.Na" +
      "ckMessageRequest\032\'.apache.rocketmq.v1.Na" +
      "ckMessageResponse\"\000B\'\n\022apache.rocketmq.v" +
      "1B\tMQServiceP\001\240\001\001\330\001\001b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.AnyProto.getDescriptor(),
          com.google.protobuf.DurationProto.getDescriptor(),
          com.google.protobuf.TimestampProto.getDescriptor(),
          com.google.rpc.ErrorDetailsProto.getDescriptor(),
          com.google.rpc.CodeProto.getDescriptor(),
          com.google.rpc.StatusProto.getDescriptor(),
          apache.rocketmq.v1.MQDomain.getDescriptor(),
        });
    internal_static_apache_rocketmq_v1_RequestCommon_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_apache_rocketmq_v1_RequestCommon_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_RequestCommon_descriptor,
        new java.lang.String[] { "Language", "ClientVersion", "ProtocolVersion", "RequestId", });
    internal_static_apache_rocketmq_v1_ResponseCommon_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_apache_rocketmq_v1_ResponseCommon_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_ResponseCommon_descriptor,
        new java.lang.String[] { "RequestId", "Status", "RequestInfo", "Help", "RetryInfo", "DebugInfo", "ErrorInfo", });
    internal_static_apache_rocketmq_v1_QueryRouteRequest_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_apache_rocketmq_v1_QueryRouteRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_QueryRouteRequest_descriptor,
        new java.lang.String[] { "Common", "Topic", });
    internal_static_apache_rocketmq_v1_QueryRouteResponse_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_apache_rocketmq_v1_QueryRouteResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_QueryRouteResponse_descriptor,
        new java.lang.String[] { "Common", "Partitions", });
    internal_static_apache_rocketmq_v1_SendMessageRequest_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_apache_rocketmq_v1_SendMessageRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_SendMessageRequest_descriptor,
        new java.lang.String[] { "Common", "Message", });
    internal_static_apache_rocketmq_v1_SendMessageResponse_descriptor =
      getDescriptor().getMessageTypes().get(5);
    internal_static_apache_rocketmq_v1_SendMessageResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_SendMessageResponse_descriptor,
        new java.lang.String[] { "Common", "MessageId", });
    internal_static_apache_rocketmq_v1_QueryAssignmentRequest_descriptor =
      getDescriptor().getMessageTypes().get(6);
    internal_static_apache_rocketmq_v1_QueryAssignmentRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_QueryAssignmentRequest_descriptor,
        new java.lang.String[] { "Common", "Topic", "Group", "ClientId", "ConsumeModel", });
    internal_static_apache_rocketmq_v1_QueryAssignmentResponse_descriptor =
      getDescriptor().getMessageTypes().get(7);
    internal_static_apache_rocketmq_v1_QueryAssignmentResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_QueryAssignmentResponse_descriptor,
        new java.lang.String[] { "Common", "LoadAssignments", });
    internal_static_apache_rocketmq_v1_ReceiveMessageRequest_descriptor =
      getDescriptor().getMessageTypes().get(8);
    internal_static_apache_rocketmq_v1_ReceiveMessageRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_ReceiveMessageRequest_descriptor,
        new java.lang.String[] { "Common", "Group", "ClientId", "Partition", "FilterExpression", "ConsumePolicy", "InitializationTimestamp", "BatchSize", "InvisibleDuration", "AwaitTime", });
    internal_static_apache_rocketmq_v1_ReceiveMessageResponse_descriptor =
      getDescriptor().getMessageTypes().get(9);
    internal_static_apache_rocketmq_v1_ReceiveMessageResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_ReceiveMessageResponse_descriptor,
        new java.lang.String[] { "Common", "Messages", "DeliveryTimestamp", "InvisibleDuration", });
    internal_static_apache_rocketmq_v1_AckMessageRequest_descriptor =
      getDescriptor().getMessageTypes().get(10);
    internal_static_apache_rocketmq_v1_AckMessageRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_AckMessageRequest_descriptor,
        new java.lang.String[] { "Common", "Group", "Topic", "ClientId", "ReceiptHandle", "MessageId", });
    internal_static_apache_rocketmq_v1_AckMessageResponse_descriptor =
      getDescriptor().getMessageTypes().get(11);
    internal_static_apache_rocketmq_v1_AckMessageResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_AckMessageResponse_descriptor,
        new java.lang.String[] { "Common", });
    internal_static_apache_rocketmq_v1_NackMessageRequest_descriptor =
      getDescriptor().getMessageTypes().get(12);
    internal_static_apache_rocketmq_v1_NackMessageRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_NackMessageRequest_descriptor,
        new java.lang.String[] { "Common", "Group", "Topic", "ClientId", "ReceiptHandle", "MessageId", "ReconsumeTimes", "MaxReconsumeTimes", });
    internal_static_apache_rocketmq_v1_NackMessageResponse_descriptor =
      getDescriptor().getMessageTypes().get(13);
    internal_static_apache_rocketmq_v1_NackMessageResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_NackMessageResponse_descriptor,
        new java.lang.String[] { "Common", });
    internal_static_apache_rocketmq_v1_HeartbeatRequest_descriptor =
      getDescriptor().getMessageTypes().get(14);
    internal_static_apache_rocketmq_v1_HeartbeatRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_HeartbeatRequest_descriptor,
        new java.lang.String[] { "Common", "Heartbeats", });
    internal_static_apache_rocketmq_v1_HeartbeatResponse_descriptor =
      getDescriptor().getMessageTypes().get(15);
    internal_static_apache_rocketmq_v1_HeartbeatResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_HeartbeatResponse_descriptor,
        new java.lang.String[] { "Common", });
    internal_static_apache_rocketmq_v1_HealthCheckRequest_descriptor =
      getDescriptor().getMessageTypes().get(16);
    internal_static_apache_rocketmq_v1_HealthCheckRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_HealthCheckRequest_descriptor,
        new java.lang.String[] { "Common", "ClientHost", });
    internal_static_apache_rocketmq_v1_HealthCheckResponse_descriptor =
      getDescriptor().getMessageTypes().get(17);
    internal_static_apache_rocketmq_v1_HealthCheckResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_HealthCheckResponse_descriptor,
        new java.lang.String[] { "Common", });
    com.google.protobuf.AnyProto.getDescriptor();
    com.google.protobuf.DurationProto.getDescriptor();
    com.google.protobuf.TimestampProto.getDescriptor();
    com.google.rpc.ErrorDetailsProto.getDescriptor();
    com.google.rpc.CodeProto.getDescriptor();
    com.google.rpc.StatusProto.getDescriptor();
    apache.rocketmq.v1.MQDomain.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}

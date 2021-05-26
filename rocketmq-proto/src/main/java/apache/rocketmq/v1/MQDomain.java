// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/definition.proto

package apache.rocketmq.v1;

public final class MQDomain {
  private MQDomain() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_FilterExpression_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_FilterExpression_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_DeadLetterPolicy_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_DeadLetterPolicy_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_Resource_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_Resource_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_ProducerGroup_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_ProducerGroup_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_ConsumerGroup_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_ConsumerGroup_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_SubscriptionEntry_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_SubscriptionEntry_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_HeartbeatEntry_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_HeartbeatEntry_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_Endpoint_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_Endpoint_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_Broker_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_Broker_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_Partition_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_Partition_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_Digest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_Digest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_SystemAttribute_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_SystemAttribute_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_Message_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_Message_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_Message_UserAttributeEntry_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_Message_UserAttributeEntry_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_apache_rocketmq_v1_LoadAssignment_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_apache_rocketmq_v1_LoadAssignment_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n#apache/rocketmq/v1/definition.proto\022\022a" +
      "pache.rocketmq.v1\032\037google/protobuf/times" +
      "tamp.proto\032\036google/protobuf/duration.pro" +
      "to\"T\n\020FilterExpression\022,\n\004type\030\001 \001(\0162\036.a" +
      "pache.rocketmq.v1.FilterType\022\022\n\nexpressi" +
      "on\030\002 \001(\t\"1\n\020DeadLetterPolicy\022\035\n\025max_deli" +
      "very_attempts\030\001 \001(\005\"%\n\010Resource\022\013\n\003arn\030\001" +
      " \001(\t\022\014\n\004name\030\002 \001(\t\"<\n\rProducerGroup\022+\n\005g" +
      "roup\030\001 \001(\0132\034.apache.rocketmq.v1.Resource" +
      "\"\356\002\n\rConsumerGroup\022+\n\005group\030\001 \001(\0132\034.apac" +
      "he.rocketmq.v1.Resource\022<\n\rsubscriptions" +
      "\030\002 \003(\0132%.apache.rocketmq.v1.Subscription" +
      "Entry\0227\n\rconsume_model\030\003 \001(\0162 .apache.ro" +
      "cketmq.v1.ConsumeModel\0229\n\016consume_policy" +
      "\030\004 \001(\0162!.apache.rocketmq.v1.ConsumePolic" +
      "y\022@\n\022dead_letter_policy\030\005 \001(\0132$.apache.r" +
      "ocketmq.v1.DeadLetterPolicy\022<\n\014consume_t" +
      "ype\030\006 \001(\0162&.apache.rocketmq.v1.ConsumeMe" +
      "ssageType\"z\n\021SubscriptionEntry\022+\n\005topic\030" +
      "\001 \001(\0132\034.apache.rocketmq.v1.Resource\0228\n\ne" +
      "xpression\030\002 \001(\0132$.apache.rocketmq.v1.Fil" +
      "terExpression\"\254\001\n\016HeartbeatEntry\022\021\n\tclie" +
      "nt_id\030\001 \001(\t\022;\n\016producer_group\030\002 \001(\0132!.ap" +
      "ache.rocketmq.v1.ProducerGroupH\000\022;\n\016cons" +
      "umer_group\030\003 \001(\0132!.apache.rocketmq.v1.Co" +
      "nsumerGroupH\000B\r\n\013client_data\"U\n\010Endpoint" +
      "\022*\n\006schema\030\001 \001(\0162\032.apache.rocketmq.v1.Sc" +
      "hema\022\017\n\007address\030\002 \001(\t\022\014\n\004port\030\003 \001(\005\"S\n\006B" +
      "roker\022\014\n\004name\030\001 \001(\t\022\n\n\002id\030\002 \001(\005\022/\n\tendpo" +
      "ints\030\003 \003(\0132\034.apache.rocketmq.v1.Endpoint" +
      "\"\244\001\n\tPartition\022+\n\005topic\030\001 \001(\0132\034.apache.r" +
      "ocketmq.v1.Resource\022\n\n\002id\030\002 \001(\005\0222\n\npermi" +
      "ssion\030\003 \001(\0162\036.apache.rocketmq.v1.Permiss" +
      "ion\022*\n\006broker\030\004 \001(\0132\032.apache.rocketmq.v1" +
      ".Broker\"H\n\006Digest\022,\n\004type\030\001 \001(\0162\036.apache" +
      ".rocketmq.v1.DigestType\022\020\n\010checksum\030\002 \001(" +
      "\t\"\364\005\n\017SystemAttribute\022\013\n\003tag\030\001 \001(\t\022\014\n\004ke" +
      "ys\030\002 \003(\t\022\022\n\nmessage_id\030\003 \001(\t\022/\n\013body_dig" +
      "est\030\004 \001(\0132\032.apache.rocketmq.v1.Digest\0223\n" +
      "\rbody_encoding\030\005 \001(\0162\034.apache.rocketmq.v" +
      "1.Encoding\0225\n\014message_type\030\006 \001(\0162\037.apach" +
      "e.rocketmq.v1.MessageType\022?\n\021transaction" +
      "_phase\030\007 \001(\0162$.apache.rocketmq.v1.Transa" +
      "ctionPhase\0222\n\016born_timestamp\030\010 \001(\0132\032.goo" +
      "gle.protobuf.Timestamp\022\021\n\tborn_host\030\t \001(" +
      "\t\0223\n\017store_timestamp\030\n \001(\0132\032.google.prot" +
      "obuf.Timestamp\022\022\n\nstore_host\030\013 \001(\t\0228\n\022de" +
      "livery_timestamp\030\014 \001(\0132\032.google.protobuf" +
      ".TimestampH\000\022\025\n\013delay_level\030\r \001(\005H\000\022\026\n\016r" +
      "eceipt_handle\030\016 \001(\t\022\024\n\014partition_id\030\017 \001(" +
      "\005\022\030\n\020partition_offset\030\020 \001(\003\0223\n\020invisible" +
      "_period\030\021 \001(\0132\031.google.protobuf.Duration" +
      "\022\026\n\016delivery_count\030\022 \001(\005\0225\n\017publisher_gr" +
      "oup\030\023 \001(\0132\034.apache.rocketmq.v1.Resource\022" +
      "\025\n\rtrace_context\030\024 \001(\tB\020\n\016timed_delivery" +
      "\"\201\002\n\007Message\022+\n\005topic\030\001 \001(\0132\034.apache.roc" +
      "ketmq.v1.Resource\022F\n\016user_attribute\030\002 \003(" +
      "\0132..apache.rocketmq.v1.Message.UserAttri" +
      "buteEntry\022=\n\020system_attribute\030\003 \001(\0132#.ap" +
      "ache.rocketmq.v1.SystemAttribute\022\014\n\004body" +
      "\030\004 \001(\014\0324\n\022UserAttributeEntry\022\013\n\003key\030\001 \001(" +
      "\t\022\r\n\005value\030\002 \001(\t:\0028\001\"x\n\016LoadAssignment\0220" +
      "\n\tPartition\030\001 \001(\0132\035.apache.rocketmq.v1.P" +
      "artition\0224\n\004mode\030\002 \001(\0162&.apache.rocketmq" +
      ".v1.ConsumeMessageType*;\n\nPermission\022\010\n\004" +
      "NONE\020\000\022\010\n\004READ\020\001\022\t\n\005WRITE\020\002\022\016\n\nREAD_WRIT" +
      "E\020\003*\036\n\nFilterType\022\007\n\003TAG\020\000\022\007\n\003SQL\020\001*0\n\014C" +
      "onsumeModel\022\016\n\nCLUSTERING\020\000\022\020\n\014BROADCAST" +
      "ING\020\001*L\n\rConsumePolicy\022\n\n\006RESUME\020\000\022\014\n\010PL" +
      "AYBACK\020\001\022\013\n\007DISCARD\020\002\022\024\n\020TARGET_TIMESTAM" +
      "P\020\003*-\n\006Schema\022\010\n\004IPv4\020\000\022\010\n\004IPv6\020\001\022\017\n\013DOM" +
      "AIN_NAME\020\002*\270\001\n\010Language\022\010\n\004JAVA\020\000\022\007\n\003CPP" +
      "\020\001\022\013\n\007C_SHARP\020\002\022\n\n\006PYTHON\020\003\022\n\n\006DELPHI\020\004\022" +
      "\n\n\006ERLANG\020\005\022\010\n\004RUBY\020\006\022\006\n\002GO\020\007\022\007\n\003PHP\020\010\022\010" +
      "\n\004RUST\020\t\022\005\n\001C\020\n\022\010\n\004PERL\020\013\022\017\n\013OBJECTIVE_C" +
      "\020\014\022\010\n\004DART\020\r\022\n\n\006KOTLIN\020\016\022\013\n\007NODE_JS\020\017*?\n" +
      "\013MessageType\022\n\n\006NORMAL\020\000\022\010\n\004FIFO\020\001\022\t\n\005DE" +
      "LAY\020\002\022\017\n\013TRANSACTION\020\003**\n\nDigestType\022\t\n\005" +
      "CRC32\020\000\022\007\n\003MD5\020\001\022\010\n\004SHA1\020\002*.\n\010Encoding\022\014" +
      "\n\010IDENTITY\020\000\022\010\n\004GZIP\020\001\022\n\n\006SNAPPY\020\002*M\n\020Tr" +
      "ansactionPhase\022\022\n\016NOT_APPLICABLE\020\000\022\013\n\007PR" +
      "EPARE\020\001\022\n\n\006COMMIT\020\002\022\014\n\010ROLLBACK\020\003*\'\n\022Con" +
      "sumeMessageType\022\010\n\004PULL\020\000\022\007\n\003POP\020\001B&\n\022ap" +
      "ache.rocketmq.v1B\010MQDomainP\001\240\001\001\330\001\001b\006prot" +
      "o3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.TimestampProto.getDescriptor(),
          com.google.protobuf.DurationProto.getDescriptor(),
        });
    internal_static_apache_rocketmq_v1_FilterExpression_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_apache_rocketmq_v1_FilterExpression_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_FilterExpression_descriptor,
        new java.lang.String[] { "Type", "Expression", });
    internal_static_apache_rocketmq_v1_DeadLetterPolicy_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_apache_rocketmq_v1_DeadLetterPolicy_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_DeadLetterPolicy_descriptor,
        new java.lang.String[] { "MaxDeliveryAttempts", });
    internal_static_apache_rocketmq_v1_Resource_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_apache_rocketmq_v1_Resource_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_Resource_descriptor,
        new java.lang.String[] { "Arn", "Name", });
    internal_static_apache_rocketmq_v1_ProducerGroup_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_apache_rocketmq_v1_ProducerGroup_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_ProducerGroup_descriptor,
        new java.lang.String[] { "Group", });
    internal_static_apache_rocketmq_v1_ConsumerGroup_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_apache_rocketmq_v1_ConsumerGroup_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_ConsumerGroup_descriptor,
        new java.lang.String[] { "Group", "Subscriptions", "ConsumeModel", "ConsumePolicy", "DeadLetterPolicy", "ConsumeType", });
    internal_static_apache_rocketmq_v1_SubscriptionEntry_descriptor =
      getDescriptor().getMessageTypes().get(5);
    internal_static_apache_rocketmq_v1_SubscriptionEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_SubscriptionEntry_descriptor,
        new java.lang.String[] { "Topic", "Expression", });
    internal_static_apache_rocketmq_v1_HeartbeatEntry_descriptor =
      getDescriptor().getMessageTypes().get(6);
    internal_static_apache_rocketmq_v1_HeartbeatEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_HeartbeatEntry_descriptor,
        new java.lang.String[] { "ClientId", "ProducerGroup", "ConsumerGroup", "ClientData", });
    internal_static_apache_rocketmq_v1_Endpoint_descriptor =
      getDescriptor().getMessageTypes().get(7);
    internal_static_apache_rocketmq_v1_Endpoint_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_Endpoint_descriptor,
        new java.lang.String[] { "Schema", "Address", "Port", });
    internal_static_apache_rocketmq_v1_Broker_descriptor =
      getDescriptor().getMessageTypes().get(8);
    internal_static_apache_rocketmq_v1_Broker_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_Broker_descriptor,
        new java.lang.String[] { "Name", "Id", "Endpoints", });
    internal_static_apache_rocketmq_v1_Partition_descriptor =
      getDescriptor().getMessageTypes().get(9);
    internal_static_apache_rocketmq_v1_Partition_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_Partition_descriptor,
        new java.lang.String[] { "Topic", "Id", "Permission", "Broker", });
    internal_static_apache_rocketmq_v1_Digest_descriptor =
      getDescriptor().getMessageTypes().get(10);
    internal_static_apache_rocketmq_v1_Digest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_Digest_descriptor,
        new java.lang.String[] { "Type", "Checksum", });
    internal_static_apache_rocketmq_v1_SystemAttribute_descriptor =
      getDescriptor().getMessageTypes().get(11);
    internal_static_apache_rocketmq_v1_SystemAttribute_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_SystemAttribute_descriptor,
        new java.lang.String[] { "Tag", "Keys", "MessageId", "BodyDigest", "BodyEncoding", "MessageType", "TransactionPhase", "BornTimestamp", "BornHost", "StoreTimestamp", "StoreHost", "DeliveryTimestamp", "DelayLevel", "ReceiptHandle", "PartitionId", "PartitionOffset", "InvisiblePeriod", "DeliveryCount", "PublisherGroup", "TraceContext", "TimedDelivery", });
    internal_static_apache_rocketmq_v1_Message_descriptor =
      getDescriptor().getMessageTypes().get(12);
    internal_static_apache_rocketmq_v1_Message_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_Message_descriptor,
        new java.lang.String[] { "Topic", "UserAttribute", "SystemAttribute", "Body", });
    internal_static_apache_rocketmq_v1_Message_UserAttributeEntry_descriptor =
      internal_static_apache_rocketmq_v1_Message_descriptor.getNestedTypes().get(0);
    internal_static_apache_rocketmq_v1_Message_UserAttributeEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_Message_UserAttributeEntry_descriptor,
        new java.lang.String[] { "Key", "Value", });
    internal_static_apache_rocketmq_v1_LoadAssignment_descriptor =
      getDescriptor().getMessageTypes().get(13);
    internal_static_apache_rocketmq_v1_LoadAssignment_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_apache_rocketmq_v1_LoadAssignment_descriptor,
        new java.lang.String[] { "Partition", "Mode", });
    com.google.protobuf.TimestampProto.getDescriptor();
    com.google.protobuf.DurationProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}

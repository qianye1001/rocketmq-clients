// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/service.proto

package apache.rocketmq.v1;

public interface NackMessageRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:apache.rocketmq.v1.NackMessageRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
   * @return Whether the common field is set.
   */
  boolean hasCommon();
  /**
   * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
   * @return The common.
   */
  apache.rocketmq.v1.RequestCommon getCommon();
  /**
   * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
   */
  apache.rocketmq.v1.RequestCommonOrBuilder getCommonOrBuilder();

  /**
   * <code>.apache.rocketmq.v1.Resource group = 2;</code>
   * @return Whether the group field is set.
   */
  boolean hasGroup();
  /**
   * <code>.apache.rocketmq.v1.Resource group = 2;</code>
   * @return The group.
   */
  apache.rocketmq.v1.Resource getGroup();
  /**
   * <code>.apache.rocketmq.v1.Resource group = 2;</code>
   */
  apache.rocketmq.v1.ResourceOrBuilder getGroupOrBuilder();

  /**
   * <code>.apache.rocketmq.v1.Resource topic = 3;</code>
   * @return Whether the topic field is set.
   */
  boolean hasTopic();
  /**
   * <code>.apache.rocketmq.v1.Resource topic = 3;</code>
   * @return The topic.
   */
  apache.rocketmq.v1.Resource getTopic();
  /**
   * <code>.apache.rocketmq.v1.Resource topic = 3;</code>
   */
  apache.rocketmq.v1.ResourceOrBuilder getTopicOrBuilder();

  /**
   * <code>string client_id = 4;</code>
   * @return The clientId.
   */
  java.lang.String getClientId();
  /**
   * <code>string client_id = 4;</code>
   * @return The bytes for clientId.
   */
  com.google.protobuf.ByteString
      getClientIdBytes();

  /**
   * <code>string receipt_handle = 5;</code>
   * @return The receiptHandle.
   */
  java.lang.String getReceiptHandle();
  /**
   * <code>string receipt_handle = 5;</code>
   * @return The bytes for receiptHandle.
   */
  com.google.protobuf.ByteString
      getReceiptHandleBytes();

  /**
   * <code>string message_id = 6;</code>
   * @return The messageId.
   */
  java.lang.String getMessageId();
  /**
   * <code>string message_id = 6;</code>
   * @return The bytes for messageId.
   */
  com.google.protobuf.ByteString
      getMessageIdBytes();

  /**
   * <code>int32 reconsume_times = 7;</code>
   * @return The reconsumeTimes.
   */
  int getReconsumeTimes();

  /**
   * <code>int32 max_reconsume_times = 8;</code>
   * @return The maxReconsumeTimes.
   */
  int getMaxReconsumeTimes();
}

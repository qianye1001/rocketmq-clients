// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/definition.proto

package apache.rocketmq.v1;

public interface PartitionOrBuilder extends
    // @@protoc_insertion_point(interface_extends:apache.rocketmq.v1.Partition)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>.apache.rocketmq.v1.Resource topic = 1;</code>
   * @return Whether the topic field is set.
   */
  boolean hasTopic();
  /**
   * <code>.apache.rocketmq.v1.Resource topic = 1;</code>
   * @return The topic.
   */
  apache.rocketmq.v1.Resource getTopic();
  /**
   * <code>.apache.rocketmq.v1.Resource topic = 1;</code>
   */
  apache.rocketmq.v1.ResourceOrBuilder getTopicOrBuilder();

  /**
   * <code>int32 id = 2;</code>
   * @return The id.
   */
  int getId();

  /**
   * <code>.apache.rocketmq.v1.Permission permission = 3;</code>
   * @return The enum numeric value on the wire for permission.
   */
  int getPermissionValue();
  /**
   * <code>.apache.rocketmq.v1.Permission permission = 3;</code>
   * @return The permission.
   */
  apache.rocketmq.v1.Permission getPermission();

  /**
   * <code>.apache.rocketmq.v1.Broker broker = 4;</code>
   * @return Whether the broker field is set.
   */
  boolean hasBroker();
  /**
   * <code>.apache.rocketmq.v1.Broker broker = 4;</code>
   * @return The broker.
   */
  apache.rocketmq.v1.Broker getBroker();
  /**
   * <code>.apache.rocketmq.v1.Broker broker = 4;</code>
   */
  apache.rocketmq.v1.BrokerOrBuilder getBrokerOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/definition.proto

package apache.rocketmq.v1;

public interface LoadAssignmentOrBuilder extends
    // @@protoc_insertion_point(interface_extends:apache.rocketmq.v1.LoadAssignment)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>.apache.rocketmq.v1.Partition Partition = 1;</code>
   * @return Whether the partition field is set.
   */
  boolean hasPartition();
  /**
   * <code>.apache.rocketmq.v1.Partition Partition = 1;</code>
   * @return The partition.
   */
  apache.rocketmq.v1.Partition getPartition();
  /**
   * <code>.apache.rocketmq.v1.Partition Partition = 1;</code>
   */
  apache.rocketmq.v1.PartitionOrBuilder getPartitionOrBuilder();

  /**
   * <code>.apache.rocketmq.v1.ConsumeMessageType mode = 2;</code>
   * @return The enum numeric value on the wire for mode.
   */
  int getModeValue();
  /**
   * <code>.apache.rocketmq.v1.ConsumeMessageType mode = 2;</code>
   * @return The mode.
   */
  apache.rocketmq.v1.ConsumeMessageType getMode();
}

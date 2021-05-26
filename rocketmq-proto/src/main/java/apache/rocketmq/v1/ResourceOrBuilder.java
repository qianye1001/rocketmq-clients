// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/definition.proto

package apache.rocketmq.v1;

public interface ResourceOrBuilder extends
    // @@protoc_insertion_point(interface_extends:apache.rocketmq.v1.Resource)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * Abstract resource namespace
   * </pre>
   *
   * <code>string arn = 1;</code>
   * @return The arn.
   */
  java.lang.String getArn();
  /**
   * <pre>
   * Abstract resource namespace
   * </pre>
   *
   * <code>string arn = 1;</code>
   * @return The bytes for arn.
   */
  com.google.protobuf.ByteString
      getArnBytes();

  /**
   * <pre>
   * Resource name identifier, which remains unique within the abstract resource namespace.
   * </pre>
   *
   * <code>string name = 2;</code>
   * @return The name.
   */
  java.lang.String getName();
  /**
   * <pre>
   * Resource name identifier, which remains unique within the abstract resource namespace.
   * </pre>
   *
   * <code>string name = 2;</code>
   * @return The bytes for name.
   */
  com.google.protobuf.ByteString
      getNameBytes();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/definition.proto

package apache.rocketmq.v1;

public interface EndpointOrBuilder extends
    // @@protoc_insertion_point(interface_extends:apache.rocketmq.v1.Endpoint)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>.apache.rocketmq.v1.Schema schema = 1;</code>
   * @return The enum numeric value on the wire for schema.
   */
  int getSchemaValue();
  /**
   * <code>.apache.rocketmq.v1.Schema schema = 1;</code>
   * @return The schema.
   */
  apache.rocketmq.v1.Schema getSchema();

  /**
   * <code>string address = 2;</code>
   * @return The address.
   */
  java.lang.String getAddress();
  /**
   * <code>string address = 2;</code>
   * @return The bytes for address.
   */
  com.google.protobuf.ByteString
      getAddressBytes();

  /**
   * <code>int32 port = 3;</code>
   * @return The port.
   */
  int getPort();
}

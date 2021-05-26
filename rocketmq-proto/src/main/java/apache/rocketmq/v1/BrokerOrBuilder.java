// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/definition.proto

package apache.rocketmq.v1;

public interface BrokerOrBuilder extends
    // @@protoc_insertion_point(interface_extends:apache.rocketmq.v1.Broker)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * Name of the broker
   * </pre>
   *
   * <code>string name = 1;</code>
   * @return The name.
   */
  java.lang.String getName();
  /**
   * <pre>
   * Name of the broker
   * </pre>
   *
   * <code>string name = 1;</code>
   * @return The bytes for name.
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <pre>
   * Broker index. Canonically, index = 0 implies that the broker is playing leader role while brokers with index &gt; 0
   * play follower role.
   * </pre>
   *
   * <code>int32 id = 2;</code>
   * @return The id.
   */
  int getId();

  /**
   * <pre>
   * Address of the broker, complying with the following scheme
   * 1. dns:[//authority/]host[:port]
   * 2. ipv4:address[:port][,address[:port],...] – IPv4 addresses
   * 3. ipv6:address[:port][,address[:port],...] – IPv6 addresses
   * </pre>
   *
   * <code>repeated .apache.rocketmq.v1.Endpoint endpoints = 3;</code>
   */
  java.util.List<apache.rocketmq.v1.Endpoint> 
      getEndpointsList();
  /**
   * <pre>
   * Address of the broker, complying with the following scheme
   * 1. dns:[//authority/]host[:port]
   * 2. ipv4:address[:port][,address[:port],...] – IPv4 addresses
   * 3. ipv6:address[:port][,address[:port],...] – IPv6 addresses
   * </pre>
   *
   * <code>repeated .apache.rocketmq.v1.Endpoint endpoints = 3;</code>
   */
  apache.rocketmq.v1.Endpoint getEndpoints(int index);
  /**
   * <pre>
   * Address of the broker, complying with the following scheme
   * 1. dns:[//authority/]host[:port]
   * 2. ipv4:address[:port][,address[:port],...] – IPv4 addresses
   * 3. ipv6:address[:port][,address[:port],...] – IPv6 addresses
   * </pre>
   *
   * <code>repeated .apache.rocketmq.v1.Endpoint endpoints = 3;</code>
   */
  int getEndpointsCount();
  /**
   * <pre>
   * Address of the broker, complying with the following scheme
   * 1. dns:[//authority/]host[:port]
   * 2. ipv4:address[:port][,address[:port],...] – IPv4 addresses
   * 3. ipv6:address[:port][,address[:port],...] – IPv6 addresses
   * </pre>
   *
   * <code>repeated .apache.rocketmq.v1.Endpoint endpoints = 3;</code>
   */
  java.util.List<? extends apache.rocketmq.v1.EndpointOrBuilder> 
      getEndpointsOrBuilderList();
  /**
   * <pre>
   * Address of the broker, complying with the following scheme
   * 1. dns:[//authority/]host[:port]
   * 2. ipv4:address[:port][,address[:port],...] – IPv4 addresses
   * 3. ipv6:address[:port][,address[:port],...] – IPv6 addresses
   * </pre>
   *
   * <code>repeated .apache.rocketmq.v1.Endpoint endpoints = 3;</code>
   */
  apache.rocketmq.v1.EndpointOrBuilder getEndpointsOrBuilder(
      int index);
}

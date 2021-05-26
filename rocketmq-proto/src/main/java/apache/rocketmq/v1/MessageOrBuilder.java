// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/definition.proto

package apache.rocketmq.v1;

public interface MessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:apache.rocketmq.v1.Message)
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
   * <pre>
   * User defined key-value pairs.
   * </pre>
   *
   * <code>map&lt;string, string&gt; user_attribute = 2;</code>
   */
  int getUserAttributeCount();
  /**
   * <pre>
   * User defined key-value pairs.
   * </pre>
   *
   * <code>map&lt;string, string&gt; user_attribute = 2;</code>
   */
  boolean containsUserAttribute(
      java.lang.String key);
  /**
   * Use {@link #getUserAttributeMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.String>
  getUserAttribute();
  /**
   * <pre>
   * User defined key-value pairs.
   * </pre>
   *
   * <code>map&lt;string, string&gt; user_attribute = 2;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getUserAttributeMap();
  /**
   * <pre>
   * User defined key-value pairs.
   * </pre>
   *
   * <code>map&lt;string, string&gt; user_attribute = 2;</code>
   */

  java.lang.String getUserAttributeOrDefault(
      java.lang.String key,
      java.lang.String defaultValue);
  /**
   * <pre>
   * User defined key-value pairs.
   * </pre>
   *
   * <code>map&lt;string, string&gt; user_attribute = 2;</code>
   */

  java.lang.String getUserAttributeOrThrow(
      java.lang.String key);

  /**
   * <code>.apache.rocketmq.v1.SystemAttribute system_attribute = 3;</code>
   * @return Whether the systemAttribute field is set.
   */
  boolean hasSystemAttribute();
  /**
   * <code>.apache.rocketmq.v1.SystemAttribute system_attribute = 3;</code>
   * @return The systemAttribute.
   */
  apache.rocketmq.v1.SystemAttribute getSystemAttribute();
  /**
   * <code>.apache.rocketmq.v1.SystemAttribute system_attribute = 3;</code>
   */
  apache.rocketmq.v1.SystemAttributeOrBuilder getSystemAttributeOrBuilder();

  /**
   * <code>bytes body = 4;</code>
   * @return The body.
   */
  com.google.protobuf.ByteString getBody();
}

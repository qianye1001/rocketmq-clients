// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/service.proto

package apache.rocketmq.v1;

/**
 * Protobuf type {@code apache.rocketmq.v1.QueryRouteRequest}
 */
public final class QueryRouteRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:apache.rocketmq.v1.QueryRouteRequest)
    QueryRouteRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use QueryRouteRequest.newBuilder() to construct.
  private QueryRouteRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private QueryRouteRequest() {
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new QueryRouteRequest();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private QueryRouteRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 10: {
            apache.rocketmq.v1.RequestCommon.Builder subBuilder = null;
            if (common_ != null) {
              subBuilder = common_.toBuilder();
            }
            common_ = input.readMessage(apache.rocketmq.v1.RequestCommon.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(common_);
              common_ = subBuilder.buildPartial();
            }

            break;
          }
          case 18: {
            apache.rocketmq.v1.Resource.Builder subBuilder = null;
            if (topic_ != null) {
              subBuilder = topic_.toBuilder();
            }
            topic_ = input.readMessage(apache.rocketmq.v1.Resource.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(topic_);
              topic_ = subBuilder.buildPartial();
            }

            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_QueryRouteRequest_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_QueryRouteRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            apache.rocketmq.v1.QueryRouteRequest.class, apache.rocketmq.v1.QueryRouteRequest.Builder.class);
  }

  public static final int COMMON_FIELD_NUMBER = 1;
  private apache.rocketmq.v1.RequestCommon common_;
  /**
   * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
   * @return Whether the common field is set.
   */
  @java.lang.Override
  public boolean hasCommon() {
    return common_ != null;
  }
  /**
   * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
   * @return The common.
   */
  @java.lang.Override
  public apache.rocketmq.v1.RequestCommon getCommon() {
    return common_ == null ? apache.rocketmq.v1.RequestCommon.getDefaultInstance() : common_;
  }
  /**
   * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
   */
  @java.lang.Override
  public apache.rocketmq.v1.RequestCommonOrBuilder getCommonOrBuilder() {
    return getCommon();
  }

  public static final int TOPIC_FIELD_NUMBER = 2;
  private apache.rocketmq.v1.Resource topic_;
  /**
   * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
   * @return Whether the topic field is set.
   */
  @java.lang.Override
  public boolean hasTopic() {
    return topic_ != null;
  }
  /**
   * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
   * @return The topic.
   */
  @java.lang.Override
  public apache.rocketmq.v1.Resource getTopic() {
    return topic_ == null ? apache.rocketmq.v1.Resource.getDefaultInstance() : topic_;
  }
  /**
   * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
   */
  @java.lang.Override
  public apache.rocketmq.v1.ResourceOrBuilder getTopicOrBuilder() {
    return getTopic();
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (common_ != null) {
      output.writeMessage(1, getCommon());
    }
    if (topic_ != null) {
      output.writeMessage(2, getTopic());
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (common_ != null) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getCommon());
    }
    if (topic_ != null) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, getTopic());
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof apache.rocketmq.v1.QueryRouteRequest)) {
      return super.equals(obj);
    }
    apache.rocketmq.v1.QueryRouteRequest other = (apache.rocketmq.v1.QueryRouteRequest) obj;

    if (hasCommon() != other.hasCommon()) return false;
    if (hasCommon()) {
      if (!getCommon()
          .equals(other.getCommon())) return false;
    }
    if (hasTopic() != other.hasTopic()) return false;
    if (hasTopic()) {
      if (!getTopic()
          .equals(other.getTopic())) return false;
    }
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasCommon()) {
      hash = (37 * hash) + COMMON_FIELD_NUMBER;
      hash = (53 * hash) + getCommon().hashCode();
    }
    if (hasTopic()) {
      hash = (37 * hash) + TOPIC_FIELD_NUMBER;
      hash = (53 * hash) + getTopic().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static apache.rocketmq.v1.QueryRouteRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(apache.rocketmq.v1.QueryRouteRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code apache.rocketmq.v1.QueryRouteRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:apache.rocketmq.v1.QueryRouteRequest)
      apache.rocketmq.v1.QueryRouteRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_QueryRouteRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_QueryRouteRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              apache.rocketmq.v1.QueryRouteRequest.class, apache.rocketmq.v1.QueryRouteRequest.Builder.class);
    }

    // Construct using apache.rocketmq.v1.QueryRouteRequest.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      if (commonBuilder_ == null) {
        common_ = null;
      } else {
        common_ = null;
        commonBuilder_ = null;
      }
      if (topicBuilder_ == null) {
        topic_ = null;
      } else {
        topic_ = null;
        topicBuilder_ = null;
      }
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_QueryRouteRequest_descriptor;
    }

    @java.lang.Override
    public apache.rocketmq.v1.QueryRouteRequest getDefaultInstanceForType() {
      return apache.rocketmq.v1.QueryRouteRequest.getDefaultInstance();
    }

    @java.lang.Override
    public apache.rocketmq.v1.QueryRouteRequest build() {
      apache.rocketmq.v1.QueryRouteRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public apache.rocketmq.v1.QueryRouteRequest buildPartial() {
      apache.rocketmq.v1.QueryRouteRequest result = new apache.rocketmq.v1.QueryRouteRequest(this);
      if (commonBuilder_ == null) {
        result.common_ = common_;
      } else {
        result.common_ = commonBuilder_.build();
      }
      if (topicBuilder_ == null) {
        result.topic_ = topic_;
      } else {
        result.topic_ = topicBuilder_.build();
      }
      onBuilt();
      return result;
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof apache.rocketmq.v1.QueryRouteRequest) {
        return mergeFrom((apache.rocketmq.v1.QueryRouteRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(apache.rocketmq.v1.QueryRouteRequest other) {
      if (other == apache.rocketmq.v1.QueryRouteRequest.getDefaultInstance()) return this;
      if (other.hasCommon()) {
        mergeCommon(other.getCommon());
      }
      if (other.hasTopic()) {
        mergeTopic(other.getTopic());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      apache.rocketmq.v1.QueryRouteRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (apache.rocketmq.v1.QueryRouteRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private apache.rocketmq.v1.RequestCommon common_;
    private com.google.protobuf.SingleFieldBuilderV3<
        apache.rocketmq.v1.RequestCommon, apache.rocketmq.v1.RequestCommon.Builder, apache.rocketmq.v1.RequestCommonOrBuilder> commonBuilder_;
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     * @return Whether the common field is set.
     */
    public boolean hasCommon() {
      return commonBuilder_ != null || common_ != null;
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     * @return The common.
     */
    public apache.rocketmq.v1.RequestCommon getCommon() {
      if (commonBuilder_ == null) {
        return common_ == null ? apache.rocketmq.v1.RequestCommon.getDefaultInstance() : common_;
      } else {
        return commonBuilder_.getMessage();
      }
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     */
    public Builder setCommon(apache.rocketmq.v1.RequestCommon value) {
      if (commonBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        common_ = value;
        onChanged();
      } else {
        commonBuilder_.setMessage(value);
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     */
    public Builder setCommon(
        apache.rocketmq.v1.RequestCommon.Builder builderForValue) {
      if (commonBuilder_ == null) {
        common_ = builderForValue.build();
        onChanged();
      } else {
        commonBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     */
    public Builder mergeCommon(apache.rocketmq.v1.RequestCommon value) {
      if (commonBuilder_ == null) {
        if (common_ != null) {
          common_ =
            apache.rocketmq.v1.RequestCommon.newBuilder(common_).mergeFrom(value).buildPartial();
        } else {
          common_ = value;
        }
        onChanged();
      } else {
        commonBuilder_.mergeFrom(value);
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     */
    public Builder clearCommon() {
      if (commonBuilder_ == null) {
        common_ = null;
        onChanged();
      } else {
        common_ = null;
        commonBuilder_ = null;
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     */
    public apache.rocketmq.v1.RequestCommon.Builder getCommonBuilder() {
      
      onChanged();
      return getCommonFieldBuilder().getBuilder();
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     */
    public apache.rocketmq.v1.RequestCommonOrBuilder getCommonOrBuilder() {
      if (commonBuilder_ != null) {
        return commonBuilder_.getMessageOrBuilder();
      } else {
        return common_ == null ?
            apache.rocketmq.v1.RequestCommon.getDefaultInstance() : common_;
      }
    }
    /**
     * <code>.apache.rocketmq.v1.RequestCommon common = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        apache.rocketmq.v1.RequestCommon, apache.rocketmq.v1.RequestCommon.Builder, apache.rocketmq.v1.RequestCommonOrBuilder> 
        getCommonFieldBuilder() {
      if (commonBuilder_ == null) {
        commonBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            apache.rocketmq.v1.RequestCommon, apache.rocketmq.v1.RequestCommon.Builder, apache.rocketmq.v1.RequestCommonOrBuilder>(
                getCommon(),
                getParentForChildren(),
                isClean());
        common_ = null;
      }
      return commonBuilder_;
    }

    private apache.rocketmq.v1.Resource topic_;
    private com.google.protobuf.SingleFieldBuilderV3<
        apache.rocketmq.v1.Resource, apache.rocketmq.v1.Resource.Builder, apache.rocketmq.v1.ResourceOrBuilder> topicBuilder_;
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     * @return Whether the topic field is set.
     */
    public boolean hasTopic() {
      return topicBuilder_ != null || topic_ != null;
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     * @return The topic.
     */
    public apache.rocketmq.v1.Resource getTopic() {
      if (topicBuilder_ == null) {
        return topic_ == null ? apache.rocketmq.v1.Resource.getDefaultInstance() : topic_;
      } else {
        return topicBuilder_.getMessage();
      }
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     */
    public Builder setTopic(apache.rocketmq.v1.Resource value) {
      if (topicBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        topic_ = value;
        onChanged();
      } else {
        topicBuilder_.setMessage(value);
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     */
    public Builder setTopic(
        apache.rocketmq.v1.Resource.Builder builderForValue) {
      if (topicBuilder_ == null) {
        topic_ = builderForValue.build();
        onChanged();
      } else {
        topicBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     */
    public Builder mergeTopic(apache.rocketmq.v1.Resource value) {
      if (topicBuilder_ == null) {
        if (topic_ != null) {
          topic_ =
            apache.rocketmq.v1.Resource.newBuilder(topic_).mergeFrom(value).buildPartial();
        } else {
          topic_ = value;
        }
        onChanged();
      } else {
        topicBuilder_.mergeFrom(value);
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     */
    public Builder clearTopic() {
      if (topicBuilder_ == null) {
        topic_ = null;
        onChanged();
      } else {
        topic_ = null;
        topicBuilder_ = null;
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     */
    public apache.rocketmq.v1.Resource.Builder getTopicBuilder() {
      
      onChanged();
      return getTopicFieldBuilder().getBuilder();
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     */
    public apache.rocketmq.v1.ResourceOrBuilder getTopicOrBuilder() {
      if (topicBuilder_ != null) {
        return topicBuilder_.getMessageOrBuilder();
      } else {
        return topic_ == null ?
            apache.rocketmq.v1.Resource.getDefaultInstance() : topic_;
      }
    }
    /**
     * <code>.apache.rocketmq.v1.Resource topic = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        apache.rocketmq.v1.Resource, apache.rocketmq.v1.Resource.Builder, apache.rocketmq.v1.ResourceOrBuilder> 
        getTopicFieldBuilder() {
      if (topicBuilder_ == null) {
        topicBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            apache.rocketmq.v1.Resource, apache.rocketmq.v1.Resource.Builder, apache.rocketmq.v1.ResourceOrBuilder>(
                getTopic(),
                getParentForChildren(),
                isClean());
        topic_ = null;
      }
      return topicBuilder_;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:apache.rocketmq.v1.QueryRouteRequest)
  }

  // @@protoc_insertion_point(class_scope:apache.rocketmq.v1.QueryRouteRequest)
  private static final apache.rocketmq.v1.QueryRouteRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new apache.rocketmq.v1.QueryRouteRequest();
  }

  public static apache.rocketmq.v1.QueryRouteRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<QueryRouteRequest>
      PARSER = new com.google.protobuf.AbstractParser<QueryRouteRequest>() {
    @java.lang.Override
    public QueryRouteRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new QueryRouteRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<QueryRouteRequest> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<QueryRouteRequest> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public apache.rocketmq.v1.QueryRouteRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


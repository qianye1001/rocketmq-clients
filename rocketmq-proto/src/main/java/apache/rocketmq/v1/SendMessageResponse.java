// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: apache/rocketmq/v1/service.proto

package apache.rocketmq.v1;

/**
 * Protobuf type {@code apache.rocketmq.v1.SendMessageResponse}
 */
public final class SendMessageResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:apache.rocketmq.v1.SendMessageResponse)
    SendMessageResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use SendMessageResponse.newBuilder() to construct.
  private SendMessageResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private SendMessageResponse() {
    messageId_ = "";
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new SendMessageResponse();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private SendMessageResponse(
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
            apache.rocketmq.v1.ResponseCommon.Builder subBuilder = null;
            if (common_ != null) {
              subBuilder = common_.toBuilder();
            }
            common_ = input.readMessage(apache.rocketmq.v1.ResponseCommon.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(common_);
              common_ = subBuilder.buildPartial();
            }

            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();

            messageId_ = s;
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
    return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_SendMessageResponse_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_SendMessageResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            apache.rocketmq.v1.SendMessageResponse.class, apache.rocketmq.v1.SendMessageResponse.Builder.class);
  }

  public static final int COMMON_FIELD_NUMBER = 1;
  private apache.rocketmq.v1.ResponseCommon common_;
  /**
   * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
   * @return Whether the common field is set.
   */
  @java.lang.Override
  public boolean hasCommon() {
    return common_ != null;
  }
  /**
   * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
   * @return The common.
   */
  @java.lang.Override
  public apache.rocketmq.v1.ResponseCommon getCommon() {
    return common_ == null ? apache.rocketmq.v1.ResponseCommon.getDefaultInstance() : common_;
  }
  /**
   * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
   */
  @java.lang.Override
  public apache.rocketmq.v1.ResponseCommonOrBuilder getCommonOrBuilder() {
    return getCommon();
  }

  public static final int MESSAGE_ID_FIELD_NUMBER = 2;
  private volatile java.lang.Object messageId_;
  /**
   * <code>string message_id = 2;</code>
   * @return The messageId.
   */
  @java.lang.Override
  public java.lang.String getMessageId() {
    java.lang.Object ref = messageId_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      messageId_ = s;
      return s;
    }
  }
  /**
   * <code>string message_id = 2;</code>
   * @return The bytes for messageId.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getMessageIdBytes() {
    java.lang.Object ref = messageId_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      messageId_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
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
    if (!getMessageIdBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, messageId_);
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
    if (!getMessageIdBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, messageId_);
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
    if (!(obj instanceof apache.rocketmq.v1.SendMessageResponse)) {
      return super.equals(obj);
    }
    apache.rocketmq.v1.SendMessageResponse other = (apache.rocketmq.v1.SendMessageResponse) obj;

    if (hasCommon() != other.hasCommon()) return false;
    if (hasCommon()) {
      if (!getCommon()
          .equals(other.getCommon())) return false;
    }
    if (!getMessageId()
        .equals(other.getMessageId())) return false;
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
    hash = (37 * hash) + MESSAGE_ID_FIELD_NUMBER;
    hash = (53 * hash) + getMessageId().hashCode();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static apache.rocketmq.v1.SendMessageResponse parseFrom(
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
  public static Builder newBuilder(apache.rocketmq.v1.SendMessageResponse prototype) {
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
   * Protobuf type {@code apache.rocketmq.v1.SendMessageResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:apache.rocketmq.v1.SendMessageResponse)
      apache.rocketmq.v1.SendMessageResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_SendMessageResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_SendMessageResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              apache.rocketmq.v1.SendMessageResponse.class, apache.rocketmq.v1.SendMessageResponse.Builder.class);
    }

    // Construct using apache.rocketmq.v1.SendMessageResponse.newBuilder()
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
      messageId_ = "";

      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return apache.rocketmq.v1.MQService.internal_static_apache_rocketmq_v1_SendMessageResponse_descriptor;
    }

    @java.lang.Override
    public apache.rocketmq.v1.SendMessageResponse getDefaultInstanceForType() {
      return apache.rocketmq.v1.SendMessageResponse.getDefaultInstance();
    }

    @java.lang.Override
    public apache.rocketmq.v1.SendMessageResponse build() {
      apache.rocketmq.v1.SendMessageResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public apache.rocketmq.v1.SendMessageResponse buildPartial() {
      apache.rocketmq.v1.SendMessageResponse result = new apache.rocketmq.v1.SendMessageResponse(this);
      if (commonBuilder_ == null) {
        result.common_ = common_;
      } else {
        result.common_ = commonBuilder_.build();
      }
      result.messageId_ = messageId_;
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
      if (other instanceof apache.rocketmq.v1.SendMessageResponse) {
        return mergeFrom((apache.rocketmq.v1.SendMessageResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(apache.rocketmq.v1.SendMessageResponse other) {
      if (other == apache.rocketmq.v1.SendMessageResponse.getDefaultInstance()) return this;
      if (other.hasCommon()) {
        mergeCommon(other.getCommon());
      }
      if (!other.getMessageId().isEmpty()) {
        messageId_ = other.messageId_;
        onChanged();
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
      apache.rocketmq.v1.SendMessageResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (apache.rocketmq.v1.SendMessageResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private apache.rocketmq.v1.ResponseCommon common_;
    private com.google.protobuf.SingleFieldBuilderV3<
        apache.rocketmq.v1.ResponseCommon, apache.rocketmq.v1.ResponseCommon.Builder, apache.rocketmq.v1.ResponseCommonOrBuilder> commonBuilder_;
    /**
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     * @return Whether the common field is set.
     */
    public boolean hasCommon() {
      return commonBuilder_ != null || common_ != null;
    }
    /**
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     * @return The common.
     */
    public apache.rocketmq.v1.ResponseCommon getCommon() {
      if (commonBuilder_ == null) {
        return common_ == null ? apache.rocketmq.v1.ResponseCommon.getDefaultInstance() : common_;
      } else {
        return commonBuilder_.getMessage();
      }
    }
    /**
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     */
    public Builder setCommon(apache.rocketmq.v1.ResponseCommon value) {
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
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     */
    public Builder setCommon(
        apache.rocketmq.v1.ResponseCommon.Builder builderForValue) {
      if (commonBuilder_ == null) {
        common_ = builderForValue.build();
        onChanged();
      } else {
        commonBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     */
    public Builder mergeCommon(apache.rocketmq.v1.ResponseCommon value) {
      if (commonBuilder_ == null) {
        if (common_ != null) {
          common_ =
            apache.rocketmq.v1.ResponseCommon.newBuilder(common_).mergeFrom(value).buildPartial();
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
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
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
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     */
    public apache.rocketmq.v1.ResponseCommon.Builder getCommonBuilder() {
      
      onChanged();
      return getCommonFieldBuilder().getBuilder();
    }
    /**
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     */
    public apache.rocketmq.v1.ResponseCommonOrBuilder getCommonOrBuilder() {
      if (commonBuilder_ != null) {
        return commonBuilder_.getMessageOrBuilder();
      } else {
        return common_ == null ?
            apache.rocketmq.v1.ResponseCommon.getDefaultInstance() : common_;
      }
    }
    /**
     * <code>.apache.rocketmq.v1.ResponseCommon common = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        apache.rocketmq.v1.ResponseCommon, apache.rocketmq.v1.ResponseCommon.Builder, apache.rocketmq.v1.ResponseCommonOrBuilder> 
        getCommonFieldBuilder() {
      if (commonBuilder_ == null) {
        commonBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            apache.rocketmq.v1.ResponseCommon, apache.rocketmq.v1.ResponseCommon.Builder, apache.rocketmq.v1.ResponseCommonOrBuilder>(
                getCommon(),
                getParentForChildren(),
                isClean());
        common_ = null;
      }
      return commonBuilder_;
    }

    private java.lang.Object messageId_ = "";
    /**
     * <code>string message_id = 2;</code>
     * @return The messageId.
     */
    public java.lang.String getMessageId() {
      java.lang.Object ref = messageId_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        messageId_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string message_id = 2;</code>
     * @return The bytes for messageId.
     */
    public com.google.protobuf.ByteString
        getMessageIdBytes() {
      java.lang.Object ref = messageId_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        messageId_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string message_id = 2;</code>
     * @param value The messageId to set.
     * @return This builder for chaining.
     */
    public Builder setMessageId(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      messageId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string message_id = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearMessageId() {
      
      messageId_ = getDefaultInstance().getMessageId();
      onChanged();
      return this;
    }
    /**
     * <code>string message_id = 2;</code>
     * @param value The bytes for messageId to set.
     * @return This builder for chaining.
     */
    public Builder setMessageIdBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      messageId_ = value;
      onChanged();
      return this;
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


    // @@protoc_insertion_point(builder_scope:apache.rocketmq.v1.SendMessageResponse)
  }

  // @@protoc_insertion_point(class_scope:apache.rocketmq.v1.SendMessageResponse)
  private static final apache.rocketmq.v1.SendMessageResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new apache.rocketmq.v1.SendMessageResponse();
  }

  public static apache.rocketmq.v1.SendMessageResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<SendMessageResponse>
      PARSER = new com.google.protobuf.AbstractParser<SendMessageResponse>() {
    @java.lang.Override
    public SendMessageResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new SendMessageResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<SendMessageResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<SendMessageResponse> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public apache.rocketmq.v1.SendMessageResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


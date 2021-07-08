#include "ClientConfig.h"
#include "IdentifiableMock.h"
#include "LogInterceptorFactory.h"
#include "MixAll.h"
#include "RpcClientImpl.h"
#include "Signature.h"
#include "TlsHelper.h"
#include "UtilAll.h"
#include "absl/container/flat_hash_set.h"
#include "apache/rocketmq/v1/service.pb.h"
#include "rocketmq/CredentialsProvider.h"
#include "spdlog/spdlog.h"
#include "gtest/gtest.h"
#include <iostream>
#include <thread>
#include <unordered_map>

using namespace testing;

ROCKETMQ_NAMESPACE_BEGIN

class RpcClientTest : public ::testing::Test {
protected:
  RpcClientTest() : completion_queue_(std::make_shared<grpc::CompletionQueue>()) {

    server_authorization_check_config_ = std::make_shared<grpc::experimental::TlsServerAuthorizationCheckConfig>(
        std::make_shared<TlsServerAuthorizationChecker>());
    std::vector<grpc::experimental::IdentityKeyCertPair> pem_list;
    grpc::experimental::IdentityKeyCertPair pair{.private_key = TlsHelper::client_private_key,
                                                 .certificate_chain = TlsHelper::client_certificate_chain};
    pem_list.emplace_back(pair);
    certificate_provider_ =
        std::make_shared<grpc::experimental::StaticDataCertificateProvider>(TlsHelper::CA, pem_list);
    tls_channel_credential_options_.set_certificate_provider(certificate_provider_);
    tls_channel_credential_options_.set_server_verification_option(GRPC_TLS_SKIP_ALL_SERVER_VERIFICATION);
    tls_channel_credential_options_.set_server_authorization_check_config(server_authorization_check_config_);
    tls_channel_credential_options_.watch_root_certs();
    tls_channel_credential_options_.watch_identity_key_cert_pairs();
    channel_credential_ = grpc::experimental::TlsCredentials(tls_channel_credential_options_);
    credentials_observable_ = std::make_shared<IdentifiableMock>();
    credentials_provider_ = std::make_shared<ConfigFileCredentialsProvider>();
    ON_CALL(*credentials_observable_, tenantId).WillByDefault(testing::ReturnRef(tenant_id_));
    ON_CALL(*credentials_observable_, credentialsProvider).WillByDefault(testing::Return(credentials_provider_));
  }

  void SetUp() override {

    client_config_.setCredentialsProvider(std::make_shared<ConfigFileCredentialsProvider>());
    client_config_.arn(arn_);
    client_config_.region(region_id_);
    client_config_.tenantId(tenant_id_);

    Signature::sign(&client_config_, metadata_);

    std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptor_factories;
    interceptor_factories.emplace_back(absl::make_unique<LogInterceptorFactory>());
    name_server_channel_ = grpc::experimental::CreateCustomChannelWithInterceptors(
        name_server_target_, channel_credential_, channel_arguments_, std::move(interceptor_factories));
  }

  void TearDown() override {}

  ~RpcClientTest() override { completion_queue_->Shutdown(); }

  bool brokerEndpoint(const std::string& topic, std::string& endpoint) {
    QueryRouteRequest route_request;
    route_request.mutable_topic()->set_name(topic.data());
    QueryRouteResponse route_response;
    RpcClientImpl name_server_client(completion_queue_, name_server_channel_);

    absl::Mutex mtx;
    absl::CondVar cv;
    bool completed = false;

    auto callback = [&](const grpc::Status& status, const grpc::ClientContext& context,
                        const QueryRouteResponse& response) {
      ASSERT_TRUE(status.ok());
      std::cout << "Route Response:" << route_response.DebugString() << std::endl;
      absl::MutexLock lk(&mtx);
      route_response = response;
      completed = true;
      cv.SignalAll();
    };

    auto invocation_context = new InvocationContext<QueryRouteResponse>();
    invocation_context->callback_ = callback;
    invocation_context->context_.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));

    for (const auto& item : metadata_) {
      invocation_context->context_.AddMetadata(item.first, item.second);
    }

    name_server_client.asyncQueryRoute(route_request, invocation_context);

    absl::flat_hash_set<std::string> broker_addresses;
    for (auto& partition : route_response.partitions()) {
      auto& broker = partition.broker();
      if (MixAll::MASTER_BROKER_ID == broker.id()) {
        for (auto& item : broker.endpoints().addresses()) {
          std::string connect_string(fmt::format("{}:{}", item.host(), item.port()));
          if (!broker_addresses.contains(connect_string)) {
            broker_addresses.insert(connect_string);
          }
        }
      } else {
        for (const auto& item : broker.endpoints().addresses()) {
          SPDLOG_WARN("Unexpected endpoint[{}:{}] with brokerId={}", item.host(), item.port(), broker.id());
        }
      }
    }
    if (broker_addresses.empty()) {
      return false;
    }

    endpoint = *broker_addresses.begin();
    return true;
  }

  void fillSendMessageRequest(SendMessageRequest& send_message_request) {
    send_message_request.mutable_message()->mutable_topic()->set_arn(arn_);
    send_message_request.mutable_message()->mutable_topic()->set_name(topic_);
    std::unordered_map<std::string, std::string> props;
    props["key"] = "value";
    props["Jack"] = "Bauer";
    send_message_request.mutable_message()->mutable_user_attribute()->insert(props.begin(), props.end());
    const char* msgId = "1EE10C841BF3D007000011480C6ACF07";
    auto system_attribute = send_message_request.mutable_message()->mutable_system_attribute();
    system_attribute->set_message_id(msgId);
    system_attribute->set_message_type(rmq::MessageType::NORMAL);
    system_attribute->set_body_encoding(rmq::Encoding::IDENTITY);
    system_attribute->set_born_host(UtilAll::getHostIPv4());
    system_attribute->set_tag("TagA");
    send_message_request.mutable_message()->set_body("Example data");
  }

  RpcClientSharedPtr brokerRpcClient() {
    std::string broker_endpoint;
    bool success = brokerEndpoint(topic_, broker_endpoint);
    if (!success) {
      return nullptr;
    }
    SPDLOG_INFO("Target broker address: {}", broker_endpoint);

    std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> interceptor_factories;
    interceptor_factories.emplace_back(absl::make_unique<LogInterceptorFactory>());
    auto broker_channel = grpc::experimental::CreateCustomChannelWithInterceptors(
        broker_endpoint, channel_credential_, channel_arguments_, std::move(interceptor_factories));
    auto client = std::make_shared<RpcClientImpl>(completion_queue_, broker_channel);
    return client;
  }

  void fillHeartbeatRequest(HeartbeatRequest& heartbeat_request) {
    auto heartbeat_entry = new rmq::HeartbeatEntry;
    heartbeat_entry->set_client_id("client_id_0");
    heartbeat_entry->mutable_consumer_group()->mutable_group()->set_name(group_);
    heartbeat_entry->mutable_consumer_group()->mutable_group()->set_arn(arn_);
    auto subscription_entry = new rmq::SubscriptionEntry;
    subscription_entry->mutable_topic()->set_name(topic_);
    subscription_entry->mutable_topic()->set_arn(arn_);
    subscription_entry->mutable_expression()->set_type(rmq::FilterType::TAG);
    subscription_entry->mutable_expression()->set_expression("*");
    heartbeat_entry->mutable_consumer_group()->mutable_subscriptions()->AddAllocated(subscription_entry);
    heartbeat_request.mutable_heartbeats()->AddAllocated(heartbeat_entry);
  }

  std::string name_server_target_{"11.165.223.199:9876"};
  std::shared_ptr<grpc::CompletionQueue> completion_queue_;
  std::shared_ptr<grpc::Channel> name_server_channel_;
  std::string topic_{"yc001"};
  std::string group_{"GID_group003"};
  std::string arn_{"MQ_INST_1973281269661160_BXmPlOA6"};
  std::string tenant_id_{"sample-tenant"};
  std::string region_id_{"cn-hangzhou"};
  std::string service_name_{"MQ"};
  ClientConfig client_config_;
  absl::flat_hash_map<std::string, std::string> metadata_;
  std::shared_ptr<IdentifiableMock> credentials_observable_;
  CredentialsProviderPtr credentials_provider_;
  std::shared_ptr<grpc::experimental::StaticDataCertificateProvider> certificate_provider_;
  grpc::experimental::TlsChannelCredentialsOptions tls_channel_credential_options_;
  std::shared_ptr<grpc::experimental::TlsServerAuthorizationCheckConfig> server_authorization_check_config_;
  std::shared_ptr<grpc::ChannelCredentials> channel_credential_;
  grpc::ChannelArguments channel_arguments_;
};

TEST_F(RpcClientTest, testRouteInfo) {
  RpcClientImpl client(completion_queue_, name_server_channel_);
  QueryRouteRequest request;
  request.mutable_topic()->set_name(topic_);
  request.mutable_topic()->set_arn(arn_);
  auto invocation_context = new InvocationContext<QueryRouteResponse>();
  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;
  auto callback = [&](const grpc::Status& status, const grpc::ClientContext& client_context,
                      const QueryRouteResponse& response) {
    if (!status.ok()) {
      std::cout << "code: " << status.error_code() << ", message: " << status.error_message() << std::endl;
    }
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(google::rpc::Code::OK == response.common().status().code());
    EXPECT_FALSE(response.partitions().empty());
    absl::MutexLock lk(&mtx);
    cv.SignalAll();
  };

  invocation_context->callback_ = callback;
  invocation_context->context_.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
  for (const auto& item : metadata_) {
    invocation_context->context_.AddMetadata(item.first, item.second);
  }
  client.asyncQueryRoute(request, invocation_context);

  while (!completed) {
    absl::MutexLock lk(&mtx);
    cv.Wait(&mtx);
  }
}

TEST_F(RpcClientTest, testSendMessageAsync) {
  SendMessageRequest request;
  fillSendMessageRequest(request);
  auto client = brokerRpcClient();
  auto context = new InvocationContext<SendMessageResponse>();

  for (const auto& entry : metadata_) {
    context->context_.AddMetadata(entry.first, entry.second);
  }

  context->callback_ = [](const grpc::Status& status, const grpc::ClientContext& client_context,
                          const SendMessageResponse& response) {
    if ((!status.ok())) {
      std::cout << "error code: " << status.error_code() << ", error message: " << status.error_message() << std::endl;
    }
    ASSERT_TRUE(status.ok());
  };
  client->asyncSend(request, context);
  std::thread th([&]() {
    InvocationContext<SendMessageResponse>* ctx;
    bool ok = false;
    completion_queue_->Next(reinterpret_cast<void**>(&ctx), &ok);
    if (ok) {
      ctx->onCompletion(ok);
    }
  });

  if (th.joinable()) {
    th.join();
  }
}

TEST_F(RpcClientTest, testHeartbeat) {
  auto client = brokerRpcClient();
  HeartbeatRequest heartbeat_request;
  HeartbeatResponse response;
  fillHeartbeatRequest(heartbeat_request);
  absl::Mutex mtx;
  absl::CondVar cv;
  bool completed = false;
  auto invocation_context = new InvocationContext<HeartbeatResponse>();

  for (const auto& entry : metadata_) {
    invocation_context->context_.AddMetadata(entry.first, entry.second);
  }

  invocation_context->callback_ = [&](const grpc::Status& status, const grpc::ClientContext& context,
                                      const HeartbeatResponse& response) {
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(google::rpc::Code::OK == response.common().status().code());
    {
      completed = true;
      absl::MutexLock lk(&mtx);
      cv.SignalAll();
    }
  };

  client->asyncHeartbeat(heartbeat_request, invocation_context);

  while (!completed) {
    absl::MutexLock lk(&mtx);
    cv.Wait(&mtx);
  }
}

TEST_F(RpcClientTest, testQueryAssignment) {
  auto client = brokerRpcClient();
  QueryAssignmentRequest request;
  request.mutable_topic()->set_name(topic_);
  request.mutable_topic()->set_arn(arn_);
  request.mutable_group()->set_arn(arn_);
  request.mutable_group()->set_name(group_);
  QueryAssignmentResponse response;

  auto invocation_context = new InvocationContext<QueryAssignmentResponse>();

  for (const auto& entry : metadata_) {
    invocation_context->context_.AddMetadata(entry.first, entry.second);
  }

  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;
  auto callback = [&](const grpc::Status& status, const grpc::ClientContext& client_context,
                      const QueryAssignmentResponse& response) {
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(response.assignments().empty());
    completed = true;
    {
      absl::MutexLock lk(&mtx);
      cv.SignalAll();
    }
  };
  invocation_context->callback_ = callback;
  client->asyncQueryAssignment(request, invocation_context);
  while (!completed) {
    absl::MutexLock lk(&mtx);
    cv.Wait(&mtx);
  }
}

TEST_F(RpcClientTest, testHealthCheck) {
  auto client = brokerRpcClient();
  HealthCheckRequest request;
  const std::string& client_host = UtilAll::getHostIPv4();
  request.set_client_host(client_host);
  HealthCheckResponse response;
  auto invocation_context = new InvocationContext<HealthCheckResponse>();

  for (const auto& entry : metadata_) {
    invocation_context->context_.AddMetadata(entry.first, entry.second);
  }

  absl::Mutex mtx;
  absl::CondVar cv;
  bool completed = false;
  invocation_context->callback_ = [&](const grpc::Status& status, const grpc::ClientContext& context,
                                      const HealthCheckResponse& response) {
    {
      absl::MutexLock lk(&mtx);
      completed = true;
      cv.SignalAll();
    }
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(google::rpc::Code::OK, response.common().status().code());
  };
  client->asyncHealthCheck(request, invocation_context);
  while (!completed) {
    absl::MutexLock lk(&mtx);
    cv.Wait(&mtx);
  }
}

ROCKETMQ_NAMESPACE_END
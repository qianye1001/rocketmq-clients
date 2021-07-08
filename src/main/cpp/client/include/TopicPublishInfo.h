#pragma once

#include <vector>

#include "ClientInstance.h"
#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "rocketmq/MQMessageQueue.h"

namespace rocketmq {

class TopicPublishInfo {
public:
  TopicPublishInfo(absl::string_view topic, TopicRouteDataPtr topic_route_data);

  /**
   * @param message_queue Reference to target message queue.
   * @return true if manage to select one; false otherwise.
   */
  bool selectOneMessageQueue(MQMessageQueue& message_queue) LOCKS_EXCLUDED(partition_list_mtx_);

  bool selectOneActiveMessageQueue(absl::flat_hash_set<std::string>& isolated, MQMessageQueue& message_queue)
      LOCKS_EXCLUDED(partition_list_mtx_);

  bool takeMessageQueues(absl::flat_hash_set<std::string>& isolated, std::vector<MQMessageQueue>& candidates, int count)
      LOCKS_EXCLUDED(partition_list_mtx_);

  void topicRouteData(TopicRouteDataPtr topic_route_data);

  /**
   * Expose partition list in perspective of message queue list.
   *
   * @return
   */
  std::vector<MQMessageQueue> getMessageQueueList() LOCKS_EXCLUDED(partition_list_mtx_);

private:
  std::vector<Partition> partition_list_ GUARDED_BY(partition_list_mtx_);
  absl::Mutex partition_list_mtx_; // protects message_queue_list_

  std::string topic_;
  TopicRouteDataPtr topic_route_data_;

  void updatePublishInfo();

  thread_local static uint32_t send_which_queue_;
};

using TopicPublishInfoPtr = std::shared_ptr<TopicPublishInfo>;
} // namespace rocketmq

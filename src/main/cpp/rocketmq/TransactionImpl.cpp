#include "TransactionImpl.h"
#include "DefaultMQProducerImpl.h"

ROCKETMQ_NAMESPACE_BEGIN

bool TransactionImpl::commit() {
  std::shared_ptr<DefaultMQProducerImpl> producer = producer_.lock();
  if (!producer) {
    return false;
  }

  return producer->commit(message_id_, transaction_id_, endpoint_);
}

bool TransactionImpl::rollback() {
  std::shared_ptr<DefaultMQProducerImpl> producer = producer_.lock();
  if (!producer) {
    return false;
  }
  return producer->rollback(message_id_, transaction_id_, endpoint_);
}

ROCKETMQ_NAMESPACE_END
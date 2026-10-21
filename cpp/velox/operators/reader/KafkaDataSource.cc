/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "KafkaDataSource.h"

#include <algorithm>
#include <chrono>
#include <limits>

#include <glog/logging.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace gluten {

namespace {

// Spark's default for kafkaConsumer.pollTimeoutMs.
constexpr int64_t kDefaultPollTimeoutMs = 120'000;
constexpr int kConsumeTimeoutMs = 100;
// Spark requires a group id on executors; librdkafka's KafkaConsumer does too.
const std::string kDefaultGroupId = "gluten-kafka-executor";

// Java client properties that have no librdkafka counterpart, or that the reader must control.
bool isIgnoredProperty(const std::string& key) {
  return key == "key.deserializer" || key == "value.deserializer" || key == "auto.offset.reset" ||
      key == "enable.auto.commit";
}

void setProperty(RdKafka::Conf& conf, const std::string& key, const std::string& value) {
  std::string errstr;
  VELOX_CHECK(
      conf.set(key, value, errstr) == RdKafka::Conf::CONF_OK, "Failed to set Kafka property {}: {}", key, errstr);
}

int64_t getLogEndOffset(RdKafka::KafkaConsumer& consumer, const std::string& topic, int32_t partition, int timeoutMs) {
  auto* client = consumer.c_ptr();
  std::unique_ptr<rd_kafka_AdminOptions_t, decltype(&rd_kafka_AdminOptions_destroy)> options(
      rd_kafka_AdminOptions_new(client, RD_KAFKA_ADMIN_OP_LISTOFFSETS), rd_kafka_AdminOptions_destroy);
  char errstr[512];
  const auto err = rd_kafka_AdminOptions_set_request_timeout(options.get(), timeoutMs, errstr, sizeof(errstr));
  VELOX_CHECK(err == RD_KAFKA_RESP_ERR_NO_ERROR, "Failed to set Kafka offset request timeout: {}", errstr);
  std::unique_ptr<rd_kafka_error_t, decltype(&rd_kafka_error_destroy)> isolationError(
      rd_kafka_AdminOptions_set_isolation_level(options.get(), RD_KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED),
      rd_kafka_error_destroy);
  if (isolationError) {
    VELOX_FAIL("Failed to set Kafka offset request isolation: {}", rd_kafka_error_string(isolationError.get()));
  }
  std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)> partitions(
      rd_kafka_topic_partition_list_new(1), rd_kafka_topic_partition_list_destroy);
  rd_kafka_topic_partition_list_add(partitions.get(), topic.c_str(), partition)->offset = RD_KAFKA_OFFSET_SPEC_LATEST;
  std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)> queue(
      rd_kafka_queue_new(client), rd_kafka_queue_destroy);
  rd_kafka_ListOffsets(client, partitions.get(), options.get(), queue.get());
  std::unique_ptr<rd_kafka_event_t, decltype(&rd_kafka_event_destroy)> event(
      rd_kafka_queue_poll(queue.get(), timeoutMs), rd_kafka_event_destroy);
  VELOX_CHECK_NOT_NULL(event, "Timed out getting Kafka log end offset for {}-{}", topic, partition);
  VELOX_CHECK(
      rd_kafka_event_error(event.get()) == RD_KAFKA_RESP_ERR_NO_ERROR,
      "Failed to get Kafka log end offset for {}-{}: {}",
      topic,
      partition,
      rd_kafka_event_error_string(event.get()));
  const auto* result = rd_kafka_event_ListOffsets_result(event.get());
  VELOX_CHECK_NOT_NULL(result, "Missing Kafka log end offset result for {}-{}", topic, partition);
  size_t count = 0;
  const auto** infos = rd_kafka_ListOffsets_result_infos(result, &count);
  VELOX_CHECK_EQ(count, 1, "Expected one Kafka log end offset result");
  const auto* info = rd_kafka_ListOffsetsResultInfo_topic_partition(infos[0]);
  VELOX_CHECK(
      info->err == RD_KAFKA_RESP_ERR_NO_ERROR,
      "Failed to get Kafka log end offset for {}-{}: {}",
      topic,
      partition,
      rd_kafka_err2str(info->err));
  VELOX_CHECK_GE(info->offset, 0, "Invalid Kafka log end offset for {}-{}", topic, partition);
  return info->offset;
}

int32_t toSparkTimestampType(RdKafka::MessageTimestamp::MessageTimestampType type) {
  // Matches org.apache.kafka.common.record.TimestampType ids.
  switch (type) {
    case RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME:
      return 0;
    case RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME:
      return 1;
    default:
      return -1;
  }
}

} // namespace

KafkaDataSource::KafkaDataSource(
    const RowTypePtr& outputType,
    const connector::ColumnHandleMap& columnHandles,
    memory::MemoryPool* pool)
    : outputType_(outputType), pool_(pool) {
  fields_.reserve(outputType_->size());
  for (const auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(it != columnHandles.end(), "Missing column handle for Kafka output column {}", outputName);
    auto handle = std::dynamic_pointer_cast<const KafkaColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(handle, "Wrong type of column handle for Kafka output column {}", outputName);
    fields_.push_back(handle->field());
  }
}

KafkaDataSource::~KafkaDataSource() {
  closeConsumer();
}

void KafkaDataSource::addSplit(std::shared_ptr<connector::ConnectorSplit> split) {
  VELOX_CHECK_NULL(split_, "Previous Kafka split has not been fully processed");
  VELOX_CHECK_NOT_NULL(split, "Kafka split must not be null");
  split_ = std::dynamic_pointer_cast<const KafkaConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split for KafkaDataSource: {}", split->toString());
  nextOffset_ = split_->startOffset;
  splitDone_ = nextOffset_ >= split_->endOffset;
  if (splitDone_) {
    return;
  }

  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  std::string errstr;
  for (const auto& [key, value] : split_->kafkaParams) {
    if (isIgnoredProperty(key)) {
      continue;
    }
    // The params are Java client properties; skip the ones librdkafka does not know.
    const auto result = conf->set(key, value, errstr);
    if (result == RdKafka::Conf::CONF_UNKNOWN) {
      VLOG(1) << "Ignoring unsupported Java Kafka property " << key;
    } else {
      VELOX_USER_CHECK(result == RdKafka::Conf::CONF_OK, "Invalid Kafka property {}: {}", key, errstr);
    }
  }
  if (split_->kafkaParams.find("group.id") == split_->kafkaParams.end()) {
    setProperty(*conf, "group.id", kDefaultGroupId);
  }
  // Match the Java client default; Spark computes end offsets from the high watermark.
  if (split_->kafkaParams.find("isolation.level") == split_->kafkaParams.end()) {
    setProperty(*conf, "isolation.level", "read_uncommitted");
  }
  // Offsets are tracked by Spark, never commit them to the consumer group.
  setProperty(*conf, "enable.auto.commit", "false");
  setProperty(*conf, "enable.auto.offset.store", "false");
  // Report aged-out offsets as errors instead of silently resetting.
  setProperty(*conf, "auto.offset.reset", "error");
  // Detects reaching the log end when trailing offsets (e.g. transaction markers) carry no record.
  setProperty(*conf, "enable.partition.eof", "true");

  consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  VELOX_CHECK_NOT_NULL(consumer_, "Failed to create Kafka consumer: {}", errstr);

  std::unique_ptr<RdKafka::TopicPartition> topicPartition(
      RdKafka::TopicPartition::create(split_->topic, split_->partition, split_->startOffset));
  std::vector<RdKafka::TopicPartition*> partitions{topicPartition.get()};
  const auto err = consumer_->assign(partitions);
  VELOX_CHECK(err == RdKafka::ERR_NO_ERROR, "Failed to assign {}: {}", split_->toString(), RdKafka::err2str(err));
}

std::optional<RowVectorPtr> KafkaDataSource::next(uint64_t size, ContinueFuture& /*future*/) {
  if (split_ == nullptr) {
    return nullptr;
  }
  if (!splitDone_) {
    auto messages = poll(std::max<uint64_t>(size, 1));
    if (!messages.empty()) {
      return toRowVector(messages);
    }
  }
  VELOX_CHECK(splitDone_);
  closeConsumer();
  split_.reset();
  return nullptr;
}

std::vector<std::unique_ptr<RdKafka::Message>> KafkaDataSource::poll(uint64_t maxRows) {
  std::vector<std::unique_ptr<RdKafka::Message>> messages;
  const auto pollTimeoutMs = split_->pollTimeoutMs > 0 ? split_->pollTimeoutMs : kDefaultPollTimeoutMs;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(pollTimeoutMs);

  while (!splitDone_ && messages.size() < maxRows) {
    // Only wait for more records while the batch is empty.
    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(messages.empty() ? kConsumeTimeoutMs : 0));
    switch (msg->err()) {
      case RdKafka::ERR_NO_ERROR: {
        const auto offset = msg->offset();
        if (offset < nextOffset_) {
          break;
        }
        if (offset >= split_->endOffset) {
          splitDone_ = true;
          break;
        }
        completedBytes_ += msg->len() + msg->key_len();
        messages.push_back(std::move(msg));
        nextOffset_ = offset + 1;
        splitDone_ = nextOffset_ >= split_->endOffset;
        break;
      }
      case RdKafka::ERR__PARTITION_EOF: {
        // Offsets below EOF without a record are compacted records or transaction markers.
        splitDone_ = msg->offset() >= split_->endOffset;
        if (!splitDone_) {
          // A read_committed EOF can be the last stable offset, or a follower can lag the leader.
          // Query the leader's physical log end before treating the missing tail as data loss.
          const auto timeoutMs = static_cast<int>(std::min<int64_t>(pollTimeoutMs, std::numeric_limits<int>::max()));
          const auto logEndOffset = getLogEndOffset(*consumer_, split_->topic, split_->partition, timeoutMs);
          if (logEndOffset < split_->endOffset) {
            VELOX_USER_CHECK(
                !split_->failOnDataLoss,
                "Kafka offsets [{}, {}) of {}-{} are no longer available. Some data may have been lost. "
                "Set the source option failOnDataLoss to false to ignore this.",
                logEndOffset,
                split_->endOffset,
                split_->topic,
                split_->partition);
            LOG(WARNING) << "Kafka offsets [" << logEndOffset << ", " << split_->endOffset << ") of " << split_->topic
                         << "-" << split_->partition << " are no longer available";
            splitDone_ = true;
          }
        }
        break;
      }
      case RdKafka::ERR__TIMED_OUT:
        if (!messages.empty()) {
          return messages;
        }
        break;
      case RdKafka::ERR__AUTO_OFFSET_RESET:
      case RdKafka::ERR_OFFSET_OUT_OF_RANGE: {
        VELOX_USER_CHECK(
            !split_->failOnDataLoss,
            "Kafka offset {} of {}-{} is no longer available: {}. Some data may have been lost. "
            "Set the source option failOnDataLoss to false to ignore this.",
            nextOffset_,
            split_->topic,
            split_->partition,
            msg->errstr());
        LOG(WARNING) << "Kafka offset " << nextOffset_ << " of " << split_->topic << "-" << split_->partition
                     << " is no longer available: " << msg->errstr();
        // Retention may have removed only the beginning of the split. Preserve the remaining
        // available records when Spark permits data loss instead of dropping the entire split.
        int64_t lowOffset;
        int64_t highOffset;
        const auto timeoutMs = static_cast<int>(std::min<int64_t>(pollTimeoutMs, std::numeric_limits<int>::max()));
        auto err =
            consumer_->query_watermark_offsets(split_->topic, split_->partition, &lowOffset, &highOffset, timeoutMs);
        VELOX_CHECK(err == RdKafka::ERR_NO_ERROR, "Failed to get Kafka offset range: {}", RdKafka::err2str(err));
        nextOffset_ = std::max(nextOffset_, lowOffset);
        splitDone_ = nextOffset_ >= std::min(highOffset, split_->endOffset);
        if (!splitDone_) {
          std::unique_ptr<RdKafka::TopicPartition> partition(
              RdKafka::TopicPartition::create(split_->topic, split_->partition, nextOffset_));
          err = consumer_->seek(*partition, timeoutMs);
          VELOX_CHECK(err == RdKafka::ERR_NO_ERROR, "Failed to seek Kafka offset: {}", RdKafka::err2str(err));
        }
        break;
      }
      default:
        // librdkafka retries transient errors itself; the poll timeout bounds how long we wait.
        LOG(WARNING) << "Kafka consume error on " << split_->toString() << ": " << msg->errstr();
        break;
    }

    if (!splitDone_ && std::chrono::steady_clock::now() >= deadline) {
      if (!messages.empty()) {
        return messages;
      }
      VELOX_FAIL(
          "Timed out after {} ms fetching offset {} of {}-{}",
          pollTimeoutMs,
          nextOffset_,
          split_->topic,
          split_->partition);
    }
  }
  return messages;
}

RowVectorPtr KafkaDataSource::toRowVector(const std::vector<std::unique_ptr<RdKafka::Message>>& messages) {
  std::vector<VectorPtr> children;
  children.reserve(fields_.size());
  for (size_t i = 0; i < fields_.size(); ++i) {
    children.push_back(makeColumn(fields_[i], outputType_->childAt(i), messages));
  }
  completedRows_ += messages.size();
  return std::make_shared<RowVector>(pool_, outputType_, BufferPtr(nullptr), messages.size(), std::move(children));
}

VectorPtr KafkaDataSource::makeColumn(
    KafkaField field,
    const TypePtr& type,
    const std::vector<std::unique_ptr<RdKafka::Message>>& messages) {
  const auto numRows = messages.size();
  switch (field) {
    case KafkaField::kKey:
    case KafkaField::kValue:
    case KafkaField::kTopic: {
      auto vector = BaseVector::create<FlatVector<StringView>>(type, numRows, pool_);
      for (size_t i = 0; i < numRows; ++i) {
        const auto& msg = *messages[i];
        // FlatVector<StringView>::set copies non-inlined strings into the vector's buffers.
        if (field == KafkaField::kTopic) {
          const auto topic = msg.topic_name();
          vector->set(i, StringView(topic));
        } else if (field == KafkaField::kKey) {
          if (msg.key_pointer() == nullptr) {
            vector->setNull(i, true);
          } else {
            vector->set(i, StringView(static_cast<const char*>(msg.key_pointer()), msg.key_len()));
          }
        } else if (msg.payload() == nullptr) {
          vector->setNull(i, true);
        } else {
          vector->set(i, StringView(static_cast<const char*>(msg.payload()), msg.len()));
        }
      }
      return vector;
    }
    case KafkaField::kPartition:
    case KafkaField::kTimestampType: {
      auto vector = BaseVector::create<FlatVector<int32_t>>(type, numRows, pool_);
      for (size_t i = 0; i < numRows; ++i) {
        vector->set(
            i,
            field == KafkaField::kPartition ? messages[i]->partition()
                                            : toSparkTimestampType(messages[i]->timestamp().type));
      }
      return vector;
    }
    case KafkaField::kOffset: {
      auto vector = BaseVector::create<FlatVector<int64_t>>(type, numRows, pool_);
      for (size_t i = 0; i < numRows; ++i) {
        vector->set(i, messages[i]->offset());
      }
      return vector;
    }
    case KafkaField::kTimestamp: {
      auto vector = BaseVector::create<FlatVector<Timestamp>>(type, numRows, pool_);
      for (size_t i = 0; i < numRows; ++i) {
        vector->set(i, Timestamp::fromMillis(messages[i]->timestamp().timestamp));
      }
      return vector;
    }
  }
  VELOX_UNREACHABLE();
}

void KafkaDataSource::closeConsumer() {
  if (consumer_ != nullptr) {
    consumer_->close();
    consumer_.reset();
  }
}

} // namespace gluten

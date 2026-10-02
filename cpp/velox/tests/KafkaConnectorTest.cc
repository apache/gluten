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

#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

#include "operators/reader/KafkaConnector.h"
#include "operators/reader/KafkaDataSource.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {
namespace {

constexpr char kConnectorId[] = "kafka-test";
constexpr char kTopic[] = "native-kafka-test";

class KafkaConnectorTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    auto* conf = rd_kafka_conf_new();
    char error[512];
    ASSERT_EQ(rd_kafka_conf_set(conf, "test.mock.num.brokers", "1", error, sizeof(error)), RD_KAFKA_CONF_OK);
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
    ASSERT_NE(producer_, nullptr) << error;
    cluster_ = rd_kafka_handle_mock_cluster(producer_);
    ASSERT_NE(cluster_, nullptr);
    ASSERT_EQ(rd_kafka_mock_topic_create(cluster_, kTopic, 1, 1), RD_KAFKA_RESP_ERR_NO_ERROR);
  }

  void TearDown() override {
    if (producer_ != nullptr) {
      rd_kafka_destroy(producer_);
    }
  }

  void produce(const std::optional<std::string>& key, const std::optional<std::string>& value) {
    ASSERT_EQ(
        rd_kafka_producev(
            producer_,
            RD_KAFKA_V_TOPIC(kTopic),
            RD_KAFKA_V_PARTITION(0),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_KEY(key ? key->data() : nullptr, key ? key->size() : 0),
            RD_KAFKA_V_VALUE(value ? const_cast<char*>(value->data()) : nullptr, value ? value->size() : 0),
            RD_KAFKA_V_END),
        RD_KAFKA_RESP_ERR_NO_ERROR);
    ASSERT_EQ(rd_kafka_flush(producer_, 10'000), RD_KAFKA_RESP_ERR_NO_ERROR);
  }

  std::shared_ptr<KafkaConnectorSplit> split(int64_t start, int64_t end, bool failOnDataLoss = true) {
    return std::make_shared<KafkaConnectorSplit>(
        kConnectorId,
        kTopic,
        0,
        start,
        end,
        10'000,
        failOnDataLoss,
        std::unordered_map<std::string, std::string>{
            {"bootstrap.servers", rd_kafka_mock_cluster_bootstraps(cluster_)}});
  }

  std::unique_ptr<KafkaDataSource> source(const RowTypePtr& type) {
    connector::ColumnHandleMap columns;
    for (size_t i = 0; i < type->size(); ++i) {
      columns.emplace(type->nameOf(i), std::make_shared<KafkaColumnHandle>(type->nameOf(i), type->childAt(i)));
    }
    return std::make_unique<KafkaDataSource>(type, columns, pool());
  }

  rd_kafka_t* producer_{nullptr};
  rd_kafka_mock_cluster_t* cluster_{nullptr};
};

TEST_F(KafkaConnectorTest, columnValidation) {
  EXPECT_EQ(KafkaColumnHandle("timestampType", INTEGER()).field(), KafkaField::kTimestampType);
  VELOX_ASSERT_THROW(KafkaColumnHandle("offset", VARCHAR()), "Kafka column offset must be of type BIGINT");
  VELOX_ASSERT_THROW(KafkaColumnHandle("headers", VARBINARY()), "Unsupported Kafka column: headers");
}

TEST_F(KafkaConnectorTest, emptyAndConsecutiveSplits) {
  auto reader = source(ROW({"offset"}, {BIGINT()}));
  ContinueFuture future = ContinueFuture::makeEmpty();
  // Empty ranges do not need consumer properties or a Kafka connection.
  auto empty = std::make_shared<KafkaConnectorSplit>(
      kConnectorId, kTopic, 0, 4, 4, 10'000, true, std::unordered_map<std::string, std::string>{});
  reader->addSplit(empty);
  VELOX_ASSERT_THROW(reader->addSplit(empty), "Previous Kafka split has not been fully processed");
  EXPECT_EQ(reader->next(10, future).value(), nullptr);
  reader->addSplit(empty);
  EXPECT_EQ(reader->next(10, future).value(), nullptr);
  EXPECT_EQ(reader->getCompletedRows(), 0);
  EXPECT_EQ(reader->getCompletedBytes(), 0);
  VELOX_ASSERT_THROW(reader->addSplit(nullptr), "Kafka split must not be null");
}

TEST_F(KafkaConnectorTest, exclusiveOffsetsAndProjectedBinaryRows) {
  const std::string key("binary\0key", 10);
  const std::string value("a long binary\0value preserved after polling", 43);
  produce("excluded", "before range");
  produce(key, value);
  produce(std::nullopt, std::nullopt);
  produce("", "");
  produce("excluded", "after range");

  // Prune and reorder the Spark Kafka schema. Reading in single-row batches also checks that
  // strings remain owned by the output vector after librdkafka messages have been destroyed.
  auto reader = source(ROW({"value", "offset", "key", "topic"}, {VARBINARY(), BIGINT(), VARBINARY(), VARCHAR()}));
  reader->addSplit(split(1, 3));
  ContinueFuture future = ContinueFuture::makeEmpty();
  auto first = reader->next(1, future).value();
  ASSERT_NE(first, nullptr);
  ASSERT_EQ(first->size(), 1);
  EXPECT_EQ(first->childAt(0)->as<FlatVector<StringView>>()->valueAt(0).str(), value);
  EXPECT_EQ(first->childAt(1)->as<FlatVector<int64_t>>()->valueAt(0), 1);
  EXPECT_EQ(first->childAt(2)->as<FlatVector<StringView>>()->valueAt(0).str(), key);
  EXPECT_EQ(first->childAt(3)->as<FlatVector<StringView>>()->valueAt(0).str(), kTopic);

  auto second = reader->next(1, future).value();
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(second->size(), 1);
  EXPECT_TRUE(second->childAt(0)->isNullAt(0));
  EXPECT_TRUE(second->childAt(2)->isNullAt(0));
  EXPECT_EQ(second->childAt(1)->as<FlatVector<int64_t>>()->valueAt(0), 2);
  EXPECT_EQ(reader->next(1, future).value(), nullptr);

  reader->addSplit(split(3, 4));
  auto third = reader->next(10, future).value();
  ASSERT_NE(third, nullptr);
  ASSERT_EQ(third->size(), 1);
  EXPECT_FALSE(third->childAt(0)->isNullAt(0));
  EXPECT_FALSE(third->childAt(2)->isNullAt(0));
  EXPECT_EQ(third->childAt(0)->as<FlatVector<StringView>>()->valueAt(0).str(), "");
  EXPECT_EQ(third->childAt(2)->as<FlatVector<StringView>>()->valueAt(0).str(), "");
  EXPECT_EQ(third->childAt(1)->as<FlatVector<int64_t>>()->valueAt(0), 3);
  EXPECT_EQ(reader->next(10, future).value(), nullptr);
  EXPECT_EQ(reader->getCompletedRows(), 3);
  EXPECT_EQ(reader->getCompletedBytes(), key.size() + value.size());
}

TEST_F(KafkaConnectorTest, offsetOutOfRange) {
  produce("key", "value");
  ContinueFuture future = ContinueFuture::makeEmpty();
  auto reader = source(ROW({"offset"}, {BIGINT()}));
  reader->addSplit(split(100, 101));
  VELOX_ASSERT_THROW(reader->next(1, future), "is no longer available");

  auto ignoreDataLoss = source(ROW({"offset"}, {BIGINT()}));
  ignoreDataLoss->addSplit(split(100, 101, false));
  EXPECT_EQ(ignoreDataLoss->next(1, future).value(), nullptr);
  EXPECT_EQ(ignoreDataLoss->getCompletedRows(), 0);
}

TEST_F(KafkaConnectorTest, invalidKafkaProperty) {
  auto reader = source(ROW({"offset"}, {BIGINT()}));
  auto invalid = std::make_shared<KafkaConnectorSplit>(
      kConnectorId,
      kTopic,
      0,
      0,
      1,
      10'000,
      true,
      std::unordered_map<std::string, std::string>{{"isolation.level", "invalid"}});
  VELOX_ASSERT_THROW(reader->addSplit(invalid), "Invalid Kafka property isolation.level");
}

} // namespace
} // namespace gluten

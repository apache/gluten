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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "operators/reader/KafkaConnector.h"
#include "operators/reader/KafkaSplit.h"
#include "velox/connectors/Connector.h"

namespace RdKafka {
class KafkaConsumer;
class Message;
} // namespace RdKafka

namespace gluten {

/// Reads the offset range [startOffset, endOffset) of one Kafka topic partition per split and
/// produces rows in Spark's Kafka source schema. Offsets are never committed back to Kafka; Spark
/// tracks progress in its own checkpoint.
class KafkaDataSource : public facebook::velox::connector::DataSource {
 public:
  KafkaDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::memory::MemoryPool* pool);

  ~KafkaDataSource() override;

  void addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) override;

  std::optional<facebook::velox::RowVectorPtr> next(uint64_t size, facebook::velox::ContinueFuture& future) override;

  void addDynamicFilter(
      facebook::velox::column_index_t /*outputChannel*/,
      const std::shared_ptr<facebook::velox::common::Filter>& /*filter*/) override {
    VELOX_UNSUPPORTED("KafkaDataSource does not support dynamic filters");
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

 private:
  // Polls up to 'maxRows' messages of the current split. Sets 'splitDone_' once the end offset is
  // reached.
  std::vector<std::unique_ptr<RdKafka::Message>> poll(uint64_t maxRows);

  facebook::velox::RowVectorPtr toRowVector(const std::vector<std::unique_ptr<RdKafka::Message>>& messages);

  facebook::velox::VectorPtr makeColumn(
      KafkaField field,
      const facebook::velox::TypePtr& type,
      const std::vector<std::unique_ptr<RdKafka::Message>>& messages);

  void closeConsumer();

  const facebook::velox::RowTypePtr outputType_;
  facebook::velox::memory::MemoryPool* const pool_;
  // The Kafka field for each output column.
  std::vector<KafkaField> fields_;

  std::shared_ptr<const KafkaConnectorSplit> split_;
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  // The next offset expected from the current split.
  int64_t nextOffset_{0};
  bool splitDone_{false};

  uint64_t completedBytes_{0};
  uint64_t completedRows_{0};
};

} // namespace gluten

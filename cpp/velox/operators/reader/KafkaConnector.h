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

#include "velox/connectors/Connector.h"

namespace gluten {

class KafkaTableHandle : public facebook::velox::connector::ConnectorTableHandle {
 public:
  explicit KafkaTableHandle(std::string connectorId) : ConnectorTableHandle(std::move(connectorId)) {}

  const std::string& name() const override {
    static const std::string kName = "KafkaTableHandle";
    return kName;
  }

  folly::dynamic serialize() const override {
    VELOX_NYI();
  }
};

/// A column of Spark's Kafka source schema.
enum class KafkaField { kKey, kValue, kTopic, kPartition, kOffset, kTimestamp, kTimestampType };

/// Column handle naming one of the Kafka source columns: key, value, topic, partition, offset,
/// timestamp or timestampType. Throws a user error for other columns, e.g. headers.
class KafkaColumnHandle : public facebook::velox::connector::ColumnHandle {
 public:
  KafkaColumnHandle(std::string name, facebook::velox::TypePtr type);

  const std::string& name() const override {
    return name_;
  }

  const facebook::velox::TypePtr& type() const {
    return type_;
  }

  KafkaField field() const {
    return field_;
  }

 private:
  const std::string name_;
  const facebook::velox::TypePtr type_;
  KafkaField field_;
};

/// Connector reading Kafka topic partitions through librdkafka. Each KafkaConnectorSplit is read
/// by a KafkaDataSource.
class KafkaConnector : public facebook::velox::connector::Connector {
 public:
  KafkaConnector(const std::string& id, std::shared_ptr<const facebook::velox::config::ConfigBase> config)
      : Connector(id, std::move(config)) {}

  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<facebook::velox::connector::DataSink> createDataSink(
      facebook::velox::RowTypePtr inputType,
      facebook::velox::connector::ConnectorInsertTableHandlePtr connectorInsertTableHandle,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      facebook::velox::connector::CommitStrategy commitStrategy) override {
    VELOX_UNSUPPORTED("KafkaConnector does not support data sinks");
  }
};

} // namespace gluten

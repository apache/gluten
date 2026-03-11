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

#include "compute/ResultIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/plannodes/IteratorSplit.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

namespace gluten {

class RowVectorStream {
 public:
  virtual ~RowVectorStream() = default;

  explicit RowVectorStream(
      facebook::velox::memory::MemoryPool* pool,
      std::shared_ptr<ResultIterator> iterator,
      const facebook::velox::RowTypePtr& outputType)
      : pool_(pool), outputType_(outputType), iterator_(iterator) {}

  bool hasNext();

  // Convert arrow batch to row vector, construct the new Rowvector with new outputType.
  virtual facebook::velox::RowVectorPtr next();

 protected:
  // Get the next batch from iterator_.
  std::shared_ptr<ColumnarBatch> nextInternal();

  facebook::velox::memory::MemoryPool* pool_;
  const facebook::velox::RowTypePtr outputType_;
  std::shared_ptr<ResultIterator> iterator_;

  bool finished_{false};
};

/// DataSource implementation that reads from ResultIterator instances.
/// This allows iterator-based data to be consumed via Velox's standard
/// connector/split mechanism, enabling proper integration with Task::addSplit().
class ValueStreamDataSource : public facebook::velox::connector::DataSource {
 public:
  ValueStreamDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx);

  void addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) override;

  std::optional<facebook::velox::RowVectorPtr> next(uint64_t size, facebook::velox::ContinueFuture& future) override;

  void addDynamicFilter(
      facebook::velox::column_index_t outputChannel,
      const std::shared_ptr<facebook::velox::common::Filter>& filter) override {
    // Iterator-based sources don't support dynamic filtering
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, facebook::velox::RuntimeMetric> getRuntimeStats() override {
    return {};
  }

 private:
  const facebook::velox::RowTypePtr outputType_;
  facebook::velox::memory::MemoryPool* pool_;

  std::vector<std::shared_ptr<RowVectorStream>> pendingIterators_;
  std::shared_ptr<RowVectorStream> currentIterator_{nullptr};
  uint64_t completedBytes_{0};
  uint64_t completedRows_{0};
};

/// Table handle for iterator-based scans
class ValueStreamTableHandle : public facebook::velox::connector::ConnectorTableHandle {
 public:
  explicit ValueStreamTableHandle(std::string connectorId) : ConnectorTableHandle(connectorId) {}

  const std::string& name() const override {
    static const std::string kName = "ValueStreamTableHandle";
    return kName;
  }

  folly::dynamic serialize() const override {
    VELOX_NYI();
  }
};

/// Column handle for iterator-based scans
class ValueStreamColumnHandle : public facebook::velox::connector::ColumnHandle {
 public:
  ValueStreamColumnHandle(std::string name, facebook::velox::TypePtr type)
      : name_(std::move(name)), type_(std::move(type)) {}

  const std::string& name() const {
    return name_;
  }

  const facebook::velox::TypePtr& type() const {
    return type_;
  }

 private:
  std::string name_;
  facebook::velox::TypePtr type_;
};

/// Connector implementation for iterator-based data sources
class ValueStreamConnector : public facebook::velox::connector::Connector {
 public:
  explicit ValueStreamConnector(
      const std::string& id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config)
      : Connector(id, config) {}

  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx) override {
    return std::make_unique<ValueStreamDataSource>(outputType, tableHandle, columnHandles, connectorQueryCtx);
  }

  std::unique_ptr<facebook::velox::connector::DataSink> createDataSink(
      facebook::velox::RowTypePtr inputType,
      facebook::velox::connector::ConnectorInsertTableHandlePtr connectorInsertTableHandle,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      facebook::velox::connector::CommitStrategy commitStrategy) override {
    VELOX_UNSUPPORTED("ValueStreamConnector does not support data sinks");
  }
};

/// Factory for creating ValueStreamConnector instances
class ValueStreamConnectorFactory : public facebook::velox::connector::ConnectorFactory {
 public:
  static constexpr const char* kValueStreamConnectorName = "value-stream";

  static std::string nodeIdOf(int32_t streamIdx) {
    return fmt::format("{}:{}", kValueStreamConnectorName, streamIdx);
  }

  ValueStreamConnectorFactory() : ConnectorFactory(kValueStreamConnectorName) {}

  std::shared_ptr<facebook::velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const facebook::velox::config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<ValueStreamConnector>(id, config);
  }
};

} // namespace gluten

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

#include "IcebergNestedField.pb.h"
#include "memory/VeloxColumnarBatch.h"
#include "utils/Metrics.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"

namespace gluten {

struct WriteStats {
  uint64_t numWrittenBytes{0};
  uint32_t numWrittenFiles{0};
  uint64_t writeIOTimeNs{0};
  uint64_t writeWallNs{0};

  bool empty() const;

  std::string toString() const;
};

class IcebergWriter {
 public:
  IcebergWriter(
      const facebook::velox::RowTypePtr& rowType,
      int32_t format,
      const std::string& outputDirectory,
      facebook::velox::common::CompressionKind compressionKind,
      int32_t partitionId,
      int64_t taskId,
      const std::string& operationId,
      std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec> spec,
      const gluten::IcebergNestedField& field,
      const std::unordered_map<std::string, std::string>& sparkConfs,
      std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool);

  void write(const VeloxColumnarBatch& batch);

  std::vector<std::string> commit();

  WriteStats writeStats() const;

 private:
  facebook::velox::RowTypePtr rowType_;
  const facebook::velox::connector::hive::iceberg::IcebergNestedField field_;
  int32_t partitionId_;
  int64_t taskId_;
  std::string operationId_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool_;
  std::shared_ptr<facebook::velox::connector::hive::HiveConfig> connectorConfig_;
  std::shared_ptr<facebook::velox::config::ConfigBase> connectorSessionProperties_;

  std::unique_ptr<facebook::velox::connector::ConnectorQueryCtx> connectorQueryCtx_;

  std::unique_ptr<facebook::velox::connector::hive::iceberg::IcebergDataSink> dataSink_;

  // Records the writer creation time in ns.
  const uint64_t createTimeNs_{0};
};

std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec>
parseIcebergPartitionSpec(const uint8_t* data, const int32_t length, facebook::velox::RowTypePtr rowType);
} // namespace gluten

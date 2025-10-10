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
#include "memory/BoltColumnarBatch.h"
#include "bolt/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "bolt/connectors/hive/iceberg/IcebergDataSink.h"

namespace gluten {

class IcebergWriter {
 public:
  IcebergWriter(
      const bytedance::bolt::RowTypePtr& rowType,
      int32_t format,
      const std::string& outputDirectory,
      bytedance::bolt::common::CompressionKind compressionKind,
      std::shared_ptr<const bytedance::bolt::connector::hive::iceberg::IcebergPartitionSpec> spec,
      const gluten::IcebergNestedField& field,
      const std::unordered_map<std::string, std::string>& sparkConfs,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> memoryPool,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> connectorPool);

  void write(const BoltColumnarBatch& batch);

  std::vector<std::string> commit();

 private:
  bytedance::bolt::RowTypePtr rowType_;
  const bytedance::bolt::connector::hive::iceberg::IcebergNestedField field_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> connectorPool_;
  std::shared_ptr<bytedance::bolt::connector::hive::HiveConfig> connectorConfig_;
  std::shared_ptr<bytedance::bolt::config::ConfigBase> connectorSessionProperties_;

  std::unique_ptr<bytedance::bolt::connector::ConnectorQueryCtx> connectorQueryCtx_;

  std::unique_ptr<bytedance::bolt::connector::hive::iceberg::IcebergDataSink> dataSink_;
};

std::shared_ptr<const bytedance::bolt::connector::hive::iceberg::IcebergPartitionSpec>
parseIcebergPartitionSpec(const uint8_t* data, const int32_t length, bytedance::bolt::RowTypePtr rowType);
} // namespace gluten

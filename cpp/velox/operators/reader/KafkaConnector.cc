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

#include "KafkaConnector.h"
#include "KafkaDataSource.h"

#include <folly/String.h>

namespace gluten {

KafkaColumnHandle::KafkaColumnHandle(std::string name, facebook::velox::TypePtr type)
    : name_(std::move(name)), type_(std::move(type)) {
  using namespace facebook::velox;
  static const std::unordered_map<std::string, std::pair<KafkaField, TypePtr>> kFields = {
      {"key", {KafkaField::kKey, VARBINARY()}},
      {"value", {KafkaField::kValue, VARBINARY()}},
      {"topic", {KafkaField::kTopic, VARCHAR()}},
      {"partition", {KafkaField::kPartition, INTEGER()}},
      {"offset", {KafkaField::kOffset, BIGINT()}},
      {"timestamp", {KafkaField::kTimestamp, TIMESTAMP()}},
      {"timestamptype", {KafkaField::kTimestampType, INTEGER()}},
  };
  auto lowerName = name_;
  folly::toLowerAscii(lowerName);
  auto it = kFields.find(lowerName);
  VELOX_USER_CHECK(it != kFields.end(), "Unsupported Kafka column: {}", name_);
  VELOX_USER_CHECK(
      type_->equivalent(*it->second.second),
      "Kafka column {} must be of type {}, got {}",
      name_,
      it->second.second->toString(),
      type_->toString());
  field_ = it->second.first;
}

std::unique_ptr<facebook::velox::connector::DataSource> KafkaConnector::createDataSource(
    const facebook::velox::RowTypePtr& outputType,
    const facebook::velox::connector::ConnectorTableHandlePtr& /*tableHandle*/,
    const facebook::velox::connector::ColumnHandleMap& columnHandles,
    facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<KafkaDataSource>(outputType, columnHandles, connectorQueryCtx->memoryPool());
}

} // namespace gluten

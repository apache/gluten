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

#include <string>
#include <unordered_map>

#include "velox/connectors/Connector.h"

namespace gluten {

/// Split for one Kafka topic partition, covering offsets [startOffset, endOffset).
/// Built from the substrait ReadRel.StreamKafka split info sent by the JVM, which
/// mirrors Spark's KafkaBatchInputPartition.
class KafkaConnectorSplit : public facebook::velox::connector::ConnectorSplit {
 public:
  KafkaConnectorSplit(
      const std::string& connectorId,
      std::string topic,
      int32_t partition,
      int64_t startOffset,
      int64_t endOffset,
      int64_t pollTimeoutMs,
      bool failOnDataLoss,
      std::unordered_map<std::string, std::string> kafkaParams)
      : ConnectorSplit(connectorId),
        topic(std::move(topic)),
        partition(partition),
        startOffset(startOffset),
        endOffset(endOffset),
        pollTimeoutMs(pollTimeoutMs),
        failOnDataLoss(failOnDataLoss),
        kafkaParams(std::move(kafkaParams)) {}

  std::string toString() const override {
    return fmt::format(
        "KafkaConnectorSplit[topic={}, partition={}, startOffset={}, endOffset={}]",
        topic,
        partition,
        startOffset,
        endOffset);
  }

  const std::string topic;
  const int32_t partition;
  const int64_t startOffset;
  // Exclusive.
  const int64_t endOffset;
  const int64_t pollTimeoutMs;
  const bool failOnDataLoss;
  // Kafka consumer properties with the "kafka." prefix already stripped, e.g. bootstrap.servers.
  const std::unordered_map<std::string, std::string> kafkaParams;
};

} // namespace gluten

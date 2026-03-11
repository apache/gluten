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
#include "velox/connectors/Connector.h"

namespace gluten {

/// Custom connector ID for iterator-based splits
constexpr const char* kIteratorConnectorId = "value-stream";

/// A custom split type that wraps a ResultIterator
/// This allows iterators to be treated as splits and added dynamically to tasks
class IteratorConnectorSplit : public facebook::velox::connector::ConnectorSplit {
 public:
  explicit IteratorConnectorSplit(const std::string& connectorId, std::shared_ptr<ResultIterator> iterator)
      : ConnectorSplit(connectorId), iterator_(std::move(iterator)) {}

  std::shared_ptr<ResultIterator> iterator() const {
    return iterator_;
  }

  std::string toString() const override {
    return fmt::format("IteratorSplit[{}]", connectorId);
  }

 private:
  std::shared_ptr<ResultIterator> iterator_;
};

} // namespace gluten

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
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/connectors/hive/HiveConnector.h"

namespace gluten::delta {

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

class DeltaConnector final : public HiveConnector {
 public:
  DeltaConnector(const std::string& id, std::shared_ptr<const config::ConfigBase> config, folly::Executor* ioExecutor)
      : HiveConnector(id, std::move(config), ioExecutor) {}

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;
};

class DeltaConnectorFactory final : public ConnectorFactory {
 public:
  static constexpr const char* kDeltaConnectorName = "delta";

  DeltaConnectorFactory() : ConnectorFactory(kDeltaConnectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      [[maybe_unused]] folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<DeltaConnector>(id, std::move(config), ioExecutor);
  }
};

} // namespace gluten::delta

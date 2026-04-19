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

#include "velox/connectors/hive/HiveDataSource.h"

#ifndef GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
#if __has_include("velox/connectors/hive/FileSplitReader.h")
#define GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER 1
#else
#define GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER 0
#endif
#endif

#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
#include "velox/connectors/hive/FileSplitReader.h"
#else
namespace facebook::velox::connector::hive {
class SplitReader;
}
#endif

namespace gluten::delta {

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

class DeltaDataSource : public HiveDataSource {
 public:
  DeltaDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& assignments,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* ioExecutor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<HiveConfig>& hiveConfig);

 protected:
#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
  std::unique_ptr<FileSplitReader> createSplitReader() override;
#else
  std::unique_ptr<SplitReader> createSplitReader() override;
#endif
};

} // namespace gluten::delta

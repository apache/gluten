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

#include "compute/delta/DeltaDataSource.h"

#include "compute/delta/DeltaSplitReader.h"

namespace gluten::delta {

DeltaDataSource::DeltaDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& assignments,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* ioExecutor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : HiveDataSource(
          outputType,
          tableHandle,
          assignments,
          fileHandleFactory,
          ioExecutor,
          connectorQueryCtx,
          hiveConfig) {}

std::unique_ptr<FileSplitReader> DeltaDataSource::createSplitReader() {
  auto bucketChannels = prepareSplit();
  auto hiveSplit = checkedPointerCast<const HiveConnectorSplit>(split_);

  return std::make_unique<DeltaSplitReader>(
      hiveSplit,
      tableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      fileConfig_,
      readerOutputType_,
      ioStatistics_,
      ioStats_,
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_,
      &infoColumns_,
      std::move(bucketChannels),
      /*subfieldFiltersForValidation=*/getFilters());
}

} // namespace gluten::delta

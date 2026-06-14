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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "compute/delta/DeltaSplit.h"
#include "substrait/SubstraitToVeloxPlan.h"

namespace gluten {

struct DeltaSplitInfo : SplitInfo {
  std::vector<std::shared_ptr<std::string>> deletionVectorPayloads;
  std::vector<std::optional<delta::DeltaDeletionVectorDescriptor>> deletionVectors;
  std::vector<delta::DeltaRowIndexFilterType> rowIndexFilterTypes;

  DeltaSplitInfo(const SplitInfo& splitInfo) : SplitInfo(splitInfo) {
    deletionVectors.reserve(splitInfo.paths.capacity());
    deletionVectorPayloads.reserve(splitInfo.paths.capacity());
    rowIndexFilterTypes.reserve(splitInfo.paths.capacity());

    const auto previousFileCount = splitInfo.paths.empty() ? 0 : splitInfo.paths.size() - 1;
    deletionVectors.resize(previousFileCount, std::nullopt);
    rowIndexFilterTypes.resize(previousFileCount, delta::DeltaRowIndexFilterType::kKeepAll);
  }
};

} // namespace gluten

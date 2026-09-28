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

#include <folly/dynamic.h>
#include <folly/json.h>
#include <algorithm>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bolt/exec/PlanNodeStats.h"
#include "utils/Metrics.h"

namespace gluten::bolt_metrics {

using PlanStats = std::unordered_map<bytedance::bolt::core::PlanNodeId, bytedance::bolt::exec::PlanNodeStats>;

inline int64_t runtimeMetric(
    const std::unordered_map<std::string, bytedance::bolt::RuntimeMetric>& runtimeStats,
    const std::string& metricId,
    bool useCount = false) {
  const auto it = runtimeStats.find(metricId);
  if (it == runtimeStats.end()) {
    return 0;
  }
  return useCount ? it->second.count : it->second.sum;
}

inline std::unique_ptr<Metrics> toMetrics(
    const PlanStats& planStats,
    const std::vector<bytedance::bolt::core::PlanNodeId>& orderedNodeIds,
    const std::unordered_set<bytedance::bolt::core::PlanNodeId>& omittedNodeIds,
    int64_t loadLazyVectorTime) {
  folly::dynamic orderedNodeIdsJson = folly::dynamic::array();
  folly::dynamic omittedNodeIdsJson = folly::dynamic::array();
  folly::dynamic nodeStatsJson = folly::dynamic::object();
  unsigned int statsNum = 0;

  for (const auto& nodeId : orderedNodeIds) {
    orderedNodeIdsJson.push_back(nodeId);
    const auto statsIt = planStats.find(nodeId);
    if (statsIt == planStats.end()) {
      if (omittedNodeIds.find(nodeId) == omittedNodeIds.end()) {
        throw std::runtime_error("Node id cannot be found in plan status: " + nodeId);
      }
      omittedNodeIdsJson.push_back(nodeId);
      ++statsNum;
      continue;
    }

    folly::dynamic operatorStats = folly::dynamic::array();
    for (const auto& entry : statsIt->second.operatorStats) {
      const auto& opStats = entry.second;
      folly::dynamic customStats = folly::dynamic::object();
      for (const auto& customMetric : opStats->customStats) {
        customStats[customMetric.first] = folly::dynamic::object("sum", customMetric.second.sum)(
            "count", customMetric.second.count)("min", customMetric.second.min)("max", customMetric.second.max);
      }

      operatorStats.push_back(folly::dynamic::object("inputRows", opStats->inputRows)(
          "inputVectors", opStats->inputVectors)("inputBytes", opStats->inputBytes)(
          "rawInputRows", opStats->rawInputRows)("rawInputBytes", opStats->rawInputBytes)(
          "outputRows", opStats->outputRows)("outputVectors", opStats->outputVectors)(
          "outputBytes", opStats->outputBytes)("cpuCount", opStats->cpuWallTiming.count)(
          "wallNanos", opStats->cpuWallTiming.wallNanos)("peakMemoryBytes", opStats->peakMemoryBytes)(
          "numMemoryAllocations", opStats->numMemoryAllocations)("spilledInputBytes", opStats->spilledInputBytes)(
          "spilledBytes", opStats->spilledBytes)("spilledRows", opStats->spilledRows)(
          "spilledPartitions", opStats->spilledPartitions)("spilledFiles", opStats->spilledFiles)(
          "physicalWrittenBytes", opStats->physicalWrittenBytes)("customStats", customStats));
    }

    statsNum += static_cast<unsigned int>(operatorStats.size());
    nodeStatsJson[nodeId] = folly::dynamic::object("operatorStats", operatorStats);
  }

  folly::dynamic payload = folly::dynamic::object("orderedNodeIds", orderedNodeIdsJson)(
      "omittedNodeIds", omittedNodeIdsJson)("loadLazyVectorTime", loadLazyVectorTime)("nodeStats", nodeStatsJson);
  return std::make_unique<Metrics>(statsNum, folly::toJson(payload));
}

} // namespace gluten::bolt_metrics

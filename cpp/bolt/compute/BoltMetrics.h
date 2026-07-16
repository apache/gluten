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
  size_t statsNum = 0;
  for (const auto& nodeId : orderedNodeIds) {
    const auto statsIt = planStats.find(nodeId);
    if (statsIt == planStats.end()) {
      if (omittedNodeIds.find(nodeId) == omittedNodeIds.end()) {
        throw std::runtime_error("Node id cannot be found in plan status: " + nodeId);
      }
      ++statsNum;
      continue;
    }
    statsNum += statsIt->second.operatorStats.size();
  }

  if (statsNum > std::numeric_limits<unsigned int>::max() / Metrics::kNum) {
    throw std::overflow_error("The number of Bolt metrics exceeds the Metrics protocol limit.");
  }

  auto metrics = std::make_unique<Metrics>(static_cast<unsigned int>(statsNum));
  std::fill_n(metrics->arrayRawPtr, statsNum * Metrics::kNum, 0L);

  size_t metricIndex = 0;
  for (const auto& nodeId : orderedNodeIds) {
    const auto statsIt = planStats.find(nodeId);
    if (statsIt == planStats.end()) {
      // Keep one fully zeroed slot so omitted filters preserve plan traversal order.
      ++metricIndex;
      continue;
    }

    for (const auto& entry : statsIt->second.operatorStats) {
      const auto& opStats = entry.second;
      metrics->get(Metrics::kInputRows)[metricIndex] = opStats->inputRows;
      metrics->get(Metrics::kInputVectors)[metricIndex] = opStats->inputVectors;
      metrics->get(Metrics::kInputBytes)[metricIndex] = opStats->inputBytes;
      metrics->get(Metrics::kRawInputRows)[metricIndex] = opStats->rawInputRows;
      metrics->get(Metrics::kRawInputBytes)[metricIndex] = opStats->rawInputBytes;
      metrics->get(Metrics::kOutputRows)[metricIndex] = opStats->outputRows;
      metrics->get(Metrics::kOutputVectors)[metricIndex] = opStats->outputVectors;
      metrics->get(Metrics::kOutputBytes)[metricIndex] = opStats->outputBytes;
      metrics->get(Metrics::kCpuCount)[metricIndex] = opStats->cpuWallTiming.count;
      metrics->get(Metrics::kWallNanos)[metricIndex] = opStats->cpuWallTiming.wallNanos;
      metrics->get(Metrics::kPeakMemoryBytes)[metricIndex] = opStats->peakMemoryBytes;
      metrics->get(Metrics::kNumMemoryAllocations)[metricIndex] = opStats->numMemoryAllocations;
      metrics->get(Metrics::kSpilledInputBytes)[metricIndex] = opStats->spilledInputBytes;
      metrics->get(Metrics::kSpilledBytes)[metricIndex] = opStats->spilledBytes;
      metrics->get(Metrics::kSpilledRows)[metricIndex] = opStats->spilledRows;
      metrics->get(Metrics::kSpilledPartitions)[metricIndex] = opStats->spilledPartitions;
      metrics->get(Metrics::kSpilledFiles)[metricIndex] = opStats->spilledFiles;
      metrics->get(Metrics::kNumDynamicFiltersProduced)[metricIndex] =
          runtimeMetric(opStats->customStats, "dynamicFiltersProduced");
      metrics->get(Metrics::kNumDynamicFiltersAccepted)[metricIndex] =
          runtimeMetric(opStats->customStats, "dynamicFiltersAccepted");
      metrics->get(Metrics::kNumReplacedWithDynamicFilterRows)[metricIndex] =
          runtimeMetric(opStats->customStats, "replacedWithDynamicFilterRows");
      metrics->get(Metrics::kNumDynamicFilterInputRows)[metricIndex] =
          runtimeMetric(opStats->customStats, "dynamicFilterInputRows");
      metrics->get(Metrics::kFlushRowCount)[metricIndex] = runtimeMetric(opStats->customStats, "flushRowCount");
      metrics->get(Metrics::kAbandonedPartialAggregationRows)[metricIndex] =
          runtimeMetric(opStats->customStats, "abandonedPartialAggregationRows");
      metrics->get(Metrics::kLoadedToValueHook)[metricIndex] = runtimeMetric(opStats->customStats, "loadedToValueHook");
      metrics->get(Metrics::kBloomFilterBlocksByteSize)[metricIndex] =
          runtimeMetric(opStats->customStats, "bloomFilterSize");
      metrics->get(Metrics::kScanTime)[metricIndex] = runtimeMetric(opStats->customStats, "totalScanTime");
      metrics->get(Metrics::kSkippedSplits)[metricIndex] = runtimeMetric(opStats->customStats, "skippedSplits");
      metrics->get(Metrics::kProcessedSplits)[metricIndex] = runtimeMetric(opStats->customStats, "processedSplits");
      metrics->get(Metrics::kSkippedStrides)[metricIndex] = runtimeMetric(opStats->customStats, "skippedStrides");
      metrics->get(Metrics::kProcessedStrides)[metricIndex] = runtimeMetric(opStats->customStats, "processedStrides");
      metrics->get(Metrics::kRemainingFilterTime)[metricIndex] =
          runtimeMetric(opStats->customStats, "totalRemainingFilterTime");
      metrics->get(Metrics::kIoWaitTime)[metricIndex] = runtimeMetric(opStats->customStats, "ioWaitWallNanos");
      metrics->get(Metrics::kStorageReadBytes)[metricIndex] = runtimeMetric(opStats->customStats, "storageReadBytes");
      metrics->get(Metrics::kStorageReads)[metricIndex] = runtimeMetric(opStats->customStats, "storageReadBytes", true);
      metrics->get(Metrics::kLocalReadBytes)[metricIndex] = runtimeMetric(opStats->customStats, "localReadBytes");
      metrics->get(Metrics::kRamReadBytes)[metricIndex] = runtimeMetric(opStats->customStats, "ramReadBytes");
      metrics->get(Metrics::kPreloadSplits)[metricIndex] = runtimeMetric(opStats->customStats, "readyPreloadedSplits");
      metrics->get(Metrics::kPageLoadTime)[metricIndex] = runtimeMetric(opStats->customStats, "pageLoadTimeNs");
      metrics->get(Metrics::kDataSourceAddSplitWallNanos)[metricIndex] =
          runtimeMetric(opStats->customStats, "dataSourceAddSplitWallNanos") +
          runtimeMetric(opStats->customStats, "waitForPreloadSplitNanos");
      metrics->get(Metrics::kDataSourceReadWallNanos)[metricIndex] =
          runtimeMetric(opStats->customStats, "dataSourceReadWallNanos");
      metrics->get(Metrics::kPhysicalWrittenBytes)[metricIndex] = opStats->physicalWrittenBytes;
      metrics->get(Metrics::kWriteIOTime)[metricIndex] = runtimeMetric(opStats->customStats, "writeIOWallNanos");
      metrics->get(Metrics::kNumWrittenFiles)[metricIndex] = runtimeMetric(opStats->customStats, "numWrittenFiles");
      ++metricIndex;
    }
  }

  if (statsNum > 0) {
    metrics->get(Metrics::kLoadLazyVectorTime)[statsNum - 1] = loadLazyVectorTime;
  }
  return metrics;
}

} // namespace gluten::bolt_metrics

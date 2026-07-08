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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "compute/delta/DeltaDeletionVectorReader.h"
#include "compute/delta/RoaringBitmapArray.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"

using gluten::delta::DeltaDeletionVectorReader;
using gluten::delta::RoaringBitmapArray;
using namespace facebook::velox;

namespace {

enum class RowIndexPattern {
  kContiguous,
  kSparse,
  kClustered,
  kMultiBucket,
};

enum class PartialDistribution {
  kContiguous,
  kRoundRobin,
};

struct RowIndexSummary {
  uint64_t rowSpan{0};
  size_t bucketCount{0};
  double densityPercent{0};
};

std::vector<uint64_t> makeRowIndexes(size_t rowCount, RowIndexPattern pattern) {
  std::vector<uint64_t> rows;
  rows.reserve(rowCount);
  for (size_t i = 0; i < rowCount; ++i) {
    switch (pattern) {
      case RowIndexPattern::kContiguous:
        rows.push_back(i);
        break;
      case RowIndexPattern::kSparse:
        rows.push_back(i * 97);
        break;
      case RowIndexPattern::kClustered:
        rows.push_back((i / 64) * 4096 + (i % 64));
        break;
      case RowIndexPattern::kMultiBucket:
        rows.push_back((static_cast<uint64_t>(i % 4) << 32) + (i / 4));
        break;
    }
  }
  return rows;
}

RowIndexSummary summarizeRowIndexes(const std::vector<uint64_t>& rows) {
  if (rows.empty()) {
    return {};
  }

  const auto [minIt, maxIt] = std::minmax_element(rows.begin(), rows.end());
  std::set<uint32_t> buckets;
  for (const auto row : rows) {
    buckets.insert(static_cast<uint32_t>(row >> 32));
  }

  const auto rowSpan = *maxIt - *minIt + 1;
  return RowIndexSummary{
      rowSpan, buckets.size(), static_cast<double>(rows.size()) * 100.0 / static_cast<double>(rowSpan)};
}

std::string buildPayload(const std::vector<uint64_t>& rows, bool optimize) {
  RoaringBitmapArray bitmap;
  for (const auto row : rows) {
    bitmap.addSafe(row);
  }
  return bitmap.serializeToString(optimize);
}

std::vector<std::string> buildPartialPayloads(
    const std::vector<uint64_t>& rows,
    size_t partialCount,
    bool optimize,
    PartialDistribution distribution) {
  std::vector<RoaringBitmapArray> partials(partialCount);
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto partialIndex = distribution == PartialDistribution::kRoundRobin
        ? i % partialCount
        : std::min(i * partialCount / rows.size(), partialCount - 1);
    partials[partialIndex].addSafe(rows[i]);
  }

  std::vector<std::string> payloads;
  payloads.reserve(partialCount);
  for (const auto& partial : partials) {
    payloads.push_back(partial.serializeToString(optimize));
  }
  return payloads;
}

std::vector<uint64_t> makeProbeRows(const std::vector<uint64_t>& rows) {
  const auto hitProbeCount = std::min<size_t>(rows.size(), 4096);
  std::vector<uint64_t> probes;
  probes.reserve(hitProbeCount * 2);
  if (hitProbeCount == 0) {
    return probes;
  }

  const auto stride = std::max<size_t>(rows.size() / hitProbeCount, 1);
  for (size_t i = 0; i < rows.size() && probes.size() < hitProbeCount * 2; i += stride) {
    probes.push_back(rows[i]);
    probes.push_back(rows.back() + 4096 + probes.size());
  }
  return probes;
}

void setCounters(
    benchmark::State& state,
    size_t rowCount,
    size_t payloadBytes,
    RowIndexSummary summary,
    size_t partialCount = 0) {
  state.counters["rows"] = benchmark::Counter(rowCount);
  state.counters["payload_bytes"] = benchmark::Counter(payloadBytes);
  state.counters["payload_bytes_per_row"] = benchmark::Counter(static_cast<double>(payloadBytes) / rowCount);
  state.counters["row_span"] = benchmark::Counter(summary.rowSpan);
  state.counters["bucket_count"] = benchmark::Counter(summary.bucketCount);
  state.counters["density_pct"] = benchmark::Counter(summary.densityPercent);
  if (partialCount > 0) {
    state.counters["partials"] = benchmark::Counter(partialCount);
  }
}

void BM_BuildAndSerialize(benchmark::State& state, RowIndexPattern pattern) {
  const auto rows = makeRowIndexes(state.range(0), pattern);
  const auto summary = summarizeRowIndexes(rows);
  size_t payloadBytes = 0;
  uint64_t cardinality = 0;

  for (auto _ : state) {
    RoaringBitmapArray bitmap;
    for (const auto row : rows) {
      bitmap.addSafe(row);
    }
    const auto payload = bitmap.serializeToString(true);
    payloadBytes = payload.size();
    cardinality = bitmap.cardinality();
    VELOX_CHECK_EQ(cardinality, rows.size());
    benchmark::DoNotOptimize(payload);
  }

  state.SetItemsProcessed(state.iterations() * rows.size());
  state.SetBytesProcessed(state.iterations() * rows.size() * sizeof(uint64_t));
  setCounters(state, rows.size(), payloadBytes, summary);
  state.counters["cardinality"] = benchmark::Counter(cardinality);
}

void BM_DeserializeAndProbe(benchmark::State& state, RowIndexPattern pattern) {
  const auto rows = makeRowIndexes(state.range(0), pattern);
  const auto summary = summarizeRowIndexes(rows);
  const auto payload = buildPayload(rows, true);
  const auto probes = makeProbeRows(rows);
  uint64_t hits = 0;

  for (auto _ : state) {
    RoaringBitmapArray bitmap;
    bitmap.deserialize(payload.data(), payload.size());
    VELOX_CHECK_EQ(bitmap.cardinality(), rows.size());
    uint64_t localHits = 0;
    for (const auto probe : probes) {
      localHits += bitmap.containsSafe(probe) ? 1 : 0;
    }
    hits = localHits;
    benchmark::DoNotOptimize(hits);
  }

  state.SetItemsProcessed(state.iterations() * probes.size());
  state.SetBytesProcessed(state.iterations() * payload.size());
  setCounters(state, rows.size(), payload.size(), summary);
  state.counters["probes"] = benchmark::Counter(probes.size());
  state.counters["hits"] = benchmark::Counter(hits);
}

void BM_MergePartials(benchmark::State& state, RowIndexPattern pattern, PartialDistribution distribution) {
  const auto rows = makeRowIndexes(state.range(0), pattern);
  const auto summary = summarizeRowIndexes(rows);
  const auto partialCount = static_cast<size_t>(state.range(1));
  const auto payloads = buildPartialPayloads(rows, partialCount, false, distribution);
  size_t mergedPayloadBytes = 0;
  uint64_t cardinality = 0;

  for (auto _ : state) {
    RoaringBitmapArray merged;
    for (const auto& payload : payloads) {
      RoaringBitmapArray partial;
      partial.deserialize(payload.data(), payload.size());
      merged.merge(partial);
    }
    const auto mergedPayload = merged.serializeToString(true);
    mergedPayloadBytes = mergedPayload.size();
    cardinality = merged.cardinality();
    VELOX_CHECK_EQ(cardinality, rows.size());
    benchmark::DoNotOptimize(mergedPayload);
  }

  state.SetItemsProcessed(state.iterations() * rows.size());
  setCounters(state, rows.size(), mergedPayloadBytes, summary, partialCount);
  state.counters["cardinality"] = benchmark::Counter(cardinality);
}

} // namespace

BENCHMARK_CAPTURE(BM_BuildAndSerialize, Contiguous_1M, RowIndexPattern::kContiguous)
    ->Arg(1 << 20)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_BuildAndSerialize, Sparse_1M, RowIndexPattern::kSparse)
    ->Arg(1 << 20)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_BuildAndSerialize, Clustered_1M, RowIndexPattern::kClustered)
    ->Arg(1 << 20)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_BuildAndSerialize, MultiBucket_256K, RowIndexPattern::kMultiBucket)
    ->Arg(1 << 18)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_DeserializeAndProbe, Contiguous_1M, RowIndexPattern::kContiguous)
    ->Arg(1 << 20)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DeserializeAndProbe, Sparse_1M, RowIndexPattern::kSparse)
    ->Arg(1 << 20)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DeserializeAndProbe, MultiBucket_256K, RowIndexPattern::kMultiBucket)
    ->Arg(1 << 18)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(
    BM_MergePartials,
    Contiguous_1M_64Partials,
    RowIndexPattern::kContiguous,
    PartialDistribution::kContiguous)
    ->Args({1 << 20, 64})
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(
    BM_MergePartials,
    Contiguous_1M_64RoundRobinPartials,
    RowIndexPattern::kContiguous,
    PartialDistribution::kRoundRobin)
    ->Args({1 << 20, 64})
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_MergePartials, Sparse_1M_64Partials, RowIndexPattern::kSparse, PartialDistribution::kContiguous)
    ->Args({1 << 20, 64})
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(
    BM_MergePartials,
    MultiBucket_256K_64Partials,
    RowIndexPattern::kMultiBucket,
    PartialDistribution::kContiguous)
    ->Args({1 << 18, 64})
    ->Unit(benchmark::kMillisecond);

// Benchmark for applyDeletionFilter: measures the hot path where a batch of
// rows is checked against the deletion vector bitmap.
// deletionPercent: fraction of rows in the total file that are deleted.
// batchSize: number of rows per batch (typical Velox batch size).
void BM_ApplyDeletionFilter(benchmark::State& state, double deletionPercent) {
  const auto batchSize = static_cast<uint64_t>(state.range(0));
  const uint64_t totalFileRows = 1000000; // 1M row file
  const auto numDeleted = static_cast<uint64_t>(totalFileRows * deletionPercent / 100.0);

  // Build a DV with deletions spread across the file.
  RoaringBitmapArray bitmap;
  const uint64_t stride = numDeleted > 0 ? totalFileRows / numDeleted : 0;
  for (uint64_t i = 0; i < numDeleted; ++i) {
    bitmap.addSafe(i * stride);
  }
  const auto payload = bitmap.serializeToString(true);

  // Load the DV reader.
  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(std::string_view(payload.data(), payload.size()));

  // Allocate the output bitmap buffer.
  auto pool = memory::memoryManager()->addLeafPool();
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(batchSize), pool.get());

  // Simulate scanning through the file in batches.
  const uint64_t numBatches = totalFileRows / batchSize;
  // Only count rows actually processed (drop tail < batchSize).
  const uint64_t rowsProcessed = numBatches * batchSize;
  uint64_t totalDeletedFound = 0;

  for (auto _ : state) {
    totalDeletedFound = 0;
    for (uint64_t batch = 0; batch < numBatches; ++batch) {
      reader.applyDeletionFilter(batch * batchSize, batchSize, deleteBitmap);
      // Count bits set to prevent dead-code elimination.
      // Padding bits in the last word are safe: applyDeletionFilter memsets
      // the entire bitmap to zero before setting only in-range bits.
      auto* raw = deleteBitmap->as<uint64_t>();
      for (uint64_t w = 0; w < bits::nwords(batchSize); ++w) {
        totalDeletedFound += __builtin_popcountll(raw[w]);
      }
    }
    benchmark::DoNotOptimize(totalDeletedFound);
  }

  state.SetItemsProcessed(state.iterations() * rowsProcessed);
  state.counters["batch_size"] = benchmark::Counter(batchSize);
  state.counters["deletion_pct"] = benchmark::Counter(deletionPercent);
  state.counters["deleted_found"] = benchmark::Counter(totalDeletedFound);
  state.counters["total_batches"] = benchmark::Counter(numBatches);
}

// Sparse deletions (1%) - the common case for MERGE/UPDATE operations.
BENCHMARK_CAPTURE(BM_ApplyDeletionFilter, Sparse_1pct, 1.0)->Arg(4096)->Unit(benchmark::kMillisecond);
// Moderate deletions (10%).
BENCHMARK_CAPTURE(BM_ApplyDeletionFilter, Moderate_10pct, 10.0)->Arg(4096)->Unit(benchmark::kMillisecond);
// Dense deletions (50%).
BENCHMARK_CAPTURE(BM_ApplyDeletionFilter, Dense_50pct, 50.0)->Arg(4096)->Unit(benchmark::kMillisecond);
// Very dense deletions (90%).
BENCHMARK_CAPTURE(BM_ApplyDeletionFilter, VeryDense_90pct, 90.0)->Arg(4096)->Unit(benchmark::kMillisecond);
// Sparse with large batch (typical Velox max batch).
BENCHMARK_CAPTURE(BM_ApplyDeletionFilter, Sparse_1pct_LargeBatch, 1.0)->Arg(10000)->Unit(benchmark::kMillisecond);

int main(int argc, char** argv) {
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

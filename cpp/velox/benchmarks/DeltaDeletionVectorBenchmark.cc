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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "benchmarks/common/BenchmarkUtils.h"
#include "compute/delta/DeltaDeletionVectorReader.h"
#include "compute/delta/DeltaUuidUtils.h"
#include "compute/delta/RoaringBitmapArray.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Crc.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"

namespace gluten::delta {

using namespace facebook::velox;

namespace {

constexpr uint32_t kAutoCpu = 0xffffffff;
constexpr uint64_t kInlineRowCount = 1 << 16;
constexpr uint64_t kStoredRowCount = 1 << 20;
constexpr uint64_t kBatchSize = 1 << 16;
constexpr uint64_t kStoredPayloadLengthBytes = 4;
constexpr uint64_t kStoredChecksumBytes = 4;

struct BenchmarkData {
  uint64_t rowCount;
  uint64_t cardinality;
  std::string serializedPayload;
  std::string inlinePayload;
  std::string storedPath;
  uint64_t storedOffset;
  uint64_t storedSizeInBytes;
};

std::string keyFor(uint64_t rowCount, uint64_t cardinality) {
  return std::to_string(rowCount) + "_" + std::to_string(cardinality);
}

std::filesystem::path benchmarkDirectory() {
  static const auto dir = [] {
    auto path = std::filesystem::temp_directory_path() / "gluten-delta-dv-benchmark";
    std::filesystem::create_directories(path);
    return path;
  }();
  return dir;
}

void appendBigEndianInt(std::string& output, uint32_t value) {
  output.push_back(static_cast<char>((value >> 24) & 0xff));
  output.push_back(static_cast<char>((value >> 16) & 0xff));
  output.push_back(static_cast<char>((value >> 8) & 0xff));
  output.push_back(static_cast<char>(value & 0xff));
}

uint32_t readUint32BigEndian(const char* data) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  return (static_cast<uint32_t>(bytes[0]) << 24) | (static_cast<uint32_t>(bytes[1]) << 16) |
      (static_cast<uint32_t>(bytes[2]) << 8) | static_cast<uint32_t>(bytes[3]);
}

std::vector<uint64_t> makeDeletedRows(uint64_t rowCount, uint64_t cardinality) {
  std::vector<uint64_t> deletedRows;
  deletedRows.reserve(cardinality);
  const auto stride = std::max<uint64_t>(1, rowCount / cardinality);
  uint64_t row = 0;
  for (uint64_t i = 0; i < cardinality; ++i) {
    deletedRows.push_back(row);
    row += stride;
  }
  return deletedRows;
}

std::string_view extractStoredPayload(std::string_view storedRange, uint64_t expectedPayloadSize) {
  VELOX_CHECK_GE(
      storedRange.size(),
      kStoredPayloadLengthBytes + kStoredChecksumBytes,
      "Stored deletion vector range is too small");

  const auto storedSize = readUint32BigEndian(storedRange.data());
  VELOX_CHECK_EQ(storedSize, expectedPayloadSize, "Stored deletion vector payload size mismatch");

  const auto expectedRangeSize = kStoredPayloadLengthBytes + expectedPayloadSize + kStoredChecksumBytes;
  VELOX_CHECK_EQ(storedRange.size(), expectedRangeSize, "Stored deletion vector range size mismatch");

  const auto payload = storedRange.substr(kStoredPayloadLengthBytes, expectedPayloadSize);
  const auto storedChecksum = readUint32BigEndian(storedRange.data() + kStoredPayloadLengthBytes + expectedPayloadSize);

  bits::Crc32 crc;
  crc.process_bytes(payload.data(), payload.size());
  VELOX_CHECK_EQ(storedChecksum, crc.checksum(), "Stored deletion vector checksum mismatch");

  return payload;
}

std::string readStoredPayloadFromFile(const BenchmarkData& data) {
  auto fs = filesystems::getFileSystem(data.storedPath, nullptr);
  auto readFile = fs->openFileForRead(data.storedPath);
  const auto bytesToRead = kStoredPayloadLengthBytes + data.storedSizeInBytes + kStoredChecksumBytes;
  const auto buffer = readFile->pread(data.storedOffset, bytesToRead);
  VELOX_CHECK_EQ(buffer.size(), bytesToRead, "Failed to read stored deletion vector payload");
  const auto payload = extractStoredPayload(std::string_view(buffer.data(), buffer.size()), data.storedSizeInBytes);
  return std::string(payload);
}

const BenchmarkData& getOrCreateData(uint64_t rowCount, uint64_t cardinality) {
  static std::mutex mutex;
  static std::unordered_map<std::string, BenchmarkData> cache;

  const auto key = keyFor(rowCount, cardinality);
  std::lock_guard<std::mutex> guard(mutex);
  auto it = cache.find(key);
  if (it != cache.end()) {
    return it->second;
  }

  RoaringBitmapArray bitmap;
  for (auto row : makeDeletedRows(rowCount, cardinality)) {
    bitmap.addSafe(row);
  }

  BenchmarkData data;
  data.rowCount = rowCount;
  data.cardinality = cardinality;
  data.serializedPayload.resize(bitmap.serializedSizeInBytes());
  bitmap.serialize(data.serializedPayload.data());
  data.inlinePayload = DeltaUuidUtils::encodeBytesToBase85(data.serializedPayload);

  bits::Crc32 crc;
  crc.process_bytes(data.serializedPayload.data(), data.serializedPayload.size());

  std::string storedBytes;
  storedBytes.reserve(1 + kStoredPayloadLengthBytes + data.serializedPayload.size() + kStoredChecksumBytes);
  storedBytes.push_back('\x01');
  data.storedOffset = storedBytes.size();
  appendBigEndianInt(storedBytes, static_cast<uint32_t>(data.serializedPayload.size()));
  storedBytes.append(data.serializedPayload);
  appendBigEndianInt(storedBytes, crc.checksum());
  data.storedSizeInBytes = data.serializedPayload.size();

  data.storedPath = (benchmarkDirectory() / ("dv_" + key + ".bin")).string();
  std::ofstream out(data.storedPath, std::ios::binary | std::ios::trunc);
  out.write(storedBytes.data(), static_cast<std::streamsize>(storedBytes.size()));
  out.close();

  auto [inserted, _] = cache.emplace(key, std::move(data));
  return inserted->second;
}

template <typename Fn>
void runBenchmarkCase(benchmark::State& state, uint64_t rowCount, Fn&& loadFn) {
  const auto cpu = state.range(0);
  const auto cardinality = static_cast<uint64_t>(state.range(1));
  if (cpu == kAutoCpu) {
    setCpu(state.thread_index());
  } else {
    setCpu(cpu);
  }

  const auto& data = getOrCreateData(rowCount, cardinality);
  auto pool = memory::memoryManager()->addLeafPool();
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(kBatchSize), pool.get());

  for (auto _ : state) {
    loadFn(data, pool.get(), ioStats, deleteBitmap);
    benchmark::DoNotOptimize(deleteBitmap->size());
    benchmark::ClobberMemory();
  }

  state.counters["payload_bytes"] = benchmark::Counter(
      data.serializedPayload.size(), benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1024);
  state.counters["cardinality"] =
      benchmark::Counter(data.cardinality, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
}

void BM_InlineCurrentLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kInlineRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        DeltaDeletionVectorReader reader(nullptr, pool, ioStats);
        reader.loadInlineDeletionVector(data.inlinePayload, data.serializedPayload.size(), data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void BM_InlineHandoffTotalLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kInlineRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        auto decoded = DeltaUuidUtils::decodeBase85ToBytes(data.inlinePayload, data.serializedPayload.size());
        std::string handedOff = decoded;
        DeltaDeletionVectorReader reader(nullptr, pool, ioStats);
        reader.loadSerializedDeletionVector(handedOff, data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void BM_InlineSerializedConsumerOnlyLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kInlineRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        std::string handedOff = data.serializedPayload;
        DeltaDeletionVectorReader reader(nullptr, pool, ioStats);
        reader.loadSerializedDeletionVector(handedOff, data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void BM_InlineSerializedDirectViewLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kInlineRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        DeltaDeletionVectorReader reader(nullptr, pool, ioStats);
        reader.loadSerializedDeletionVector(
            std::string_view(data.serializedPayload.data(), data.serializedPayload.size()), data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void BM_StoredCurrentLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kStoredRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        auto fs = filesystems::getFileSystem(data.storedPath, nullptr);
        DeltaDeletionVectorReader reader(fs, pool, ioStats);
        reader.loadDeletionVector(data.storedPath, data.storedOffset, data.storedSizeInBytes, data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void BM_StoredHandoffTotalLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kStoredRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        auto payload = readStoredPayloadFromFile(data);
        std::string handedOff = payload;
        DeltaDeletionVectorReader reader(nullptr, pool, ioStats);
        reader.loadSerializedDeletionVector(handedOff, data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void BM_StoredSerializedConsumerOnlyLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kStoredRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        std::string handedOff = data.serializedPayload;
        DeltaDeletionVectorReader reader(nullptr, pool, ioStats);
        reader.loadSerializedDeletionVector(handedOff, data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void BM_StoredSerializedDirectViewLoadApply(benchmark::State& state) {
  runBenchmarkCase(
      state,
      kStoredRowCount,
      [](const BenchmarkData& data,
         memory::MemoryPool* pool,
         const std::shared_ptr<io::IoStatistics>& ioStats,
         BufferPtr deleteBitmap) {
        DeltaDeletionVectorReader reader(nullptr, pool, ioStats);
        reader.loadSerializedDeletionVector(
            std::string_view(data.serializedPayload.data(), data.serializedPayload.size()), data.cardinality);
        reader.applyDeletionFilter(0, kBatchSize, deleteBitmap);
      });
}

void registerBenchmarks(uint32_t cpu) {
  for (const auto cardinality : {256ULL, 2048ULL}) {
    benchmark::RegisterBenchmark("DeltaDV/InlineCurrentLoadApply", BM_InlineCurrentLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);

    benchmark::RegisterBenchmark("DeltaDV/InlineHandoffTotalLoadApply", BM_InlineHandoffTotalLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);

    benchmark::RegisterBenchmark(
        "DeltaDV/InlineSerializedConsumerOnlyLoadApply", BM_InlineSerializedConsumerOnlyLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);

    benchmark::RegisterBenchmark("DeltaDV/InlineSerializedDirectViewLoadApply", BM_InlineSerializedDirectViewLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);
  }

  for (const auto cardinality : {16384ULL, 131072ULL}) {
    benchmark::RegisterBenchmark("DeltaDV/StoredCurrentLoadApply", BM_StoredCurrentLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);

    benchmark::RegisterBenchmark("DeltaDV/StoredHandoffTotalLoadApply", BM_StoredHandoffTotalLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);

    benchmark::RegisterBenchmark(
        "DeltaDV/StoredSerializedConsumerOnlyLoadApply", BM_StoredSerializedConsumerOnlyLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);

    benchmark::RegisterBenchmark("DeltaDV/StoredSerializedDirectViewLoadApply", BM_StoredSerializedDirectViewLoadApply)
        ->Args({cpu, static_cast<int64_t>(cardinality)})
        ->Iterations(FLAGS_iterations)
        ->Threads(FLAGS_threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kMicrosecond);
  }
}

} // namespace

} // namespace gluten::delta

int main(int argc, char** argv) {
  facebook::velox::memory::MemoryManager::testingSetInstance(facebook::velox::memory::MemoryManager::Options{});
  facebook::velox::filesystems::registerLocalFileSystem();

  uint32_t cpu = gluten::delta::kAutoCpu;
  for (int i = 0; i < argc; ++i) {
    if (strcmp(argv[i], "--cpu") == 0 && i + 1 < argc) {
      cpu = static_cast<uint32_t>(std::atoll(argv[i + 1]));
    }
  }

  benchmark::Initialize(&argc, argv);
  gluten::delta::registerBenchmarks(cpu);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

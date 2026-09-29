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
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>
#include <benchmark/benchmark.h>

#include "memory/ColumnarBatchIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "shuffle/Payload.h"
#include "utils/Exception.h"
#include "utils/VeloxBatchResizer.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace gluten {
namespace {

constexpr int32_t kInputBatches = 64;
constexpr int32_t kRowsPerBatch = 64;
constexpr int32_t kTotalRows = kInputBatches * kRowsPerBatch;
constexpr int64_t kPreferredBatchBytes = std::numeric_limits<int64_t>::max();

enum class DenseVectorKind {
  kMixed,
  kFixedWidth,
  kStringOnly,
  kBoolHeavy,
};

struct DenseBenchmarkScenario {
  int32_t inputBatches;
  int32_t rowsPerBatch;
  DenseVectorKind kind;
  int32_t fixedWidthColumns;
  int32_t stringBytes;
  int32_t boolColumns;
  bool nullable;
};

constexpr DenseBenchmarkScenario kMixed64x64{kInputBatches, kRowsPerBatch, DenseVectorKind::kMixed, 0, 16, 1, true};
constexpr DenseBenchmarkScenario kMixed16x256{16, 256, DenseVectorKind::kMixed, 0, 16, 1, true};
constexpr DenseBenchmarkScenario kMixed256x16{256, 16, DenseVectorKind::kMixed, 0, 16, 1, true};
constexpr DenseBenchmarkScenario
    kFixed2_64x64{kInputBatches, kRowsPerBatch, DenseVectorKind::kFixedWidth, 2, 0, 0, false};
constexpr DenseBenchmarkScenario
    kFixed16_64x64{kInputBatches, kRowsPerBatch, DenseVectorKind::kFixedWidth, 16, 0, 0, false};
constexpr DenseBenchmarkScenario
    kLongString64x64{kInputBatches, kRowsPerBatch, DenseVectorKind::kStringOnly, 0, 64, 0, false};
constexpr DenseBenchmarkScenario
    kBoolHeavy64x64{kInputBatches, kRowsPerBatch, DenseVectorKind::kBoolHeavy, 0, 0, 8, false};

enum class EncodedVectorKind {
  kDictionary,
  kConstant,
};

struct EncodedBenchmarkScenario {
  int32_t inputBatches;
  int32_t rowsPerBatch;
  EncodedVectorKind kind;
  int32_t columns;
};

constexpr EncodedBenchmarkScenario kDictionaryHeavy64x64{
    kInputBatches,
    kRowsPerBatch,
    EncodedVectorKind::kDictionary,
    8,
};
constexpr EncodedBenchmarkScenario kConstantHeavy64x64{
    kInputBatches,
    kRowsPerBatch,
    EncodedVectorKind::kConstant,
    8,
};

class ColumnarBatchArray : public ColumnarBatchIterator {
 public:
  explicit ColumnarBatchArray(std::vector<std::shared_ptr<ColumnarBatch>> batches) : batches_(std::move(batches)) {}

  std::shared_ptr<ColumnarBatch> next() override {
    if (cursor_ >= batches_.size()) {
      return nullptr;
    }
    return batches_[cursor_++];
  }

 private:
  std::vector<std::shared_ptr<ColumnarBatch>> batches_;
  size_t cursor_{0};
};

std::string makeStringValue(int32_t value, int32_t bytes) {
  auto stringValue = std::to_string(value);
  if (stringValue.size() < bytes) {
    stringValue.append(bytes - stringValue.size(), 'x');
  }
  return stringValue;
}

RowVectorPtr makeMixedVector(memory::MemoryPool* pool, const DenseBenchmarkScenario& scenario, int32_t start) {
  const auto rows = scenario.rowsPerBatch;
  auto i32 = BaseVector::create<FlatVector<int32_t>>(INTEGER(), rows, pool);
  auto i64 = BaseVector::create<FlatVector<int64_t>>(BIGINT(), rows, pool);
  auto flag = BaseVector::create<FlatVector<bool>>(BOOLEAN(), rows, pool);
  auto str = BaseVector::create<FlatVector<StringView>>(VARCHAR(), rows, pool);

  for (auto row = 0; row < rows; ++row) {
    const auto value = start + row;
    i32->set(row, value);
    if (scenario.nullable && row % 7 == 0) {
      i64->setNull(row, true);
    } else {
      i64->set(row, value);
    }
    flag->set(row, row % 2 == 0);
    const auto stringValue = makeStringValue(value, scenario.stringBytes);
    str->set(row, StringView(stringValue));
  }

  return std::make_shared<RowVector>(
      pool,
      ROW({INTEGER(), BIGINT(), BOOLEAN(), VARCHAR()}),
      nullptr,
      rows,
      std::vector<VectorPtr>{i32, i64, flag, str});
}

RowVectorPtr makeFixedWidthVector(memory::MemoryPool* pool, const DenseBenchmarkScenario& scenario, int32_t start) {
  const auto rows = scenario.rowsPerBatch;
  std::vector<VectorPtr> children;
  std::vector<TypePtr> types;
  children.reserve(scenario.fixedWidthColumns);
  types.reserve(scenario.fixedWidthColumns);
  for (auto channel = 0; channel < scenario.fixedWidthColumns; ++channel) {
    auto vector = BaseVector::create<FlatVector<int64_t>>(BIGINT(), rows, pool);
    for (auto row = 0; row < rows; ++row) {
      vector->set(row, static_cast<int64_t>(start + row + channel));
    }
    children.push_back(std::move(vector));
    types.push_back(BIGINT());
  }

  return std::make_shared<RowVector>(pool, ROW(std::move(types)), nullptr, rows, std::move(children));
}

RowVectorPtr makeStringVector(memory::MemoryPool* pool, const DenseBenchmarkScenario& scenario, int32_t start) {
  const auto rows = scenario.rowsPerBatch;
  auto str = BaseVector::create<FlatVector<StringView>>(VARCHAR(), rows, pool);
  for (auto row = 0; row < rows; ++row) {
    const auto value = start + row;
    const auto stringValue = makeStringValue(value, scenario.stringBytes);
    str->set(row, StringView(stringValue));
  }

  return std::make_shared<RowVector>(pool, ROW({VARCHAR()}), nullptr, rows, std::vector<VectorPtr>{str});
}

RowVectorPtr makeBoolHeavyVector(memory::MemoryPool* pool, const DenseBenchmarkScenario& scenario, int32_t start) {
  const auto rows = scenario.rowsPerBatch;
  std::vector<VectorPtr> children;
  std::vector<TypePtr> types;
  children.reserve(scenario.boolColumns);
  types.reserve(scenario.boolColumns);
  for (auto channel = 0; channel < scenario.boolColumns; ++channel) {
    auto vector = BaseVector::create<FlatVector<bool>>(BOOLEAN(), rows, pool);
    for (auto row = 0; row < rows; ++row) {
      vector->set(row, (start + row + channel) % 2 == 0);
    }
    children.push_back(std::move(vector));
    types.push_back(BOOLEAN());
  }

  return std::make_shared<RowVector>(pool, ROW(std::move(types)), nullptr, rows, std::move(children));
}

RowVectorPtr makeDenseVector(memory::MemoryPool* pool, const DenseBenchmarkScenario& scenario, int32_t start) {
  switch (scenario.kind) {
    case DenseVectorKind::kMixed:
      return makeMixedVector(pool, scenario, start);
    case DenseVectorKind::kFixedWidth:
      return makeFixedWidthVector(pool, scenario, start);
    case DenseVectorKind::kStringOnly:
      return makeStringVector(pool, scenario, start);
    case DenseVectorKind::kBoolHeavy:
      return makeBoolHeavyVector(pool, scenario, start);
  }
  VELOX_UNREACHABLE();
}

std::vector<RowVectorPtr> makeSmallVectors(memory::MemoryPool* pool, const DenseBenchmarkScenario& scenario) {
  std::vector<RowVectorPtr> vectors;
  vectors.reserve(scenario.inputBatches);
  for (auto batch = 0; batch < scenario.inputBatches; ++batch) {
    vectors.push_back(makeDenseVector(pool, scenario, batch * scenario.rowsPerBatch));
  }
  return vectors;
}

RowVectorPtr
makeDictionaryHeavyVector(memory::MemoryPool* pool, const EncodedBenchmarkScenario& scenario, int32_t start) {
  const auto rows = scenario.rowsPerBatch;
  const auto dictionarySize = std::max<int32_t>(1, rows / 4);
  std::vector<VectorPtr> children;
  std::vector<TypePtr> types;
  children.reserve(scenario.columns);
  types.reserve(scenario.columns);
  for (auto channel = 0; channel < scenario.columns; ++channel) {
    auto base = BaseVector::create<FlatVector<int64_t>>(BIGINT(), dictionarySize, pool);
    for (auto row = 0; row < dictionarySize; ++row) {
      base->set(row, static_cast<int64_t>(start + row + channel));
    }

    auto indices = allocateIndices(rows, pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (auto row = 0; row < rows; ++row) {
      rawIndices[row] = (start + row + channel) % dictionarySize;
    }
    children.push_back(BaseVector::wrapInDictionary(nullptr, std::move(indices), rows, std::move(base)));
    types.push_back(BIGINT());
  }

  return std::make_shared<RowVector>(pool, ROW(std::move(types)), nullptr, rows, std::move(children));
}

RowVectorPtr
makeConstantHeavyVector(memory::MemoryPool* pool, const EncodedBenchmarkScenario& scenario, int32_t start) {
  const auto rows = scenario.rowsPerBatch;
  std::vector<VectorPtr> children;
  std::vector<TypePtr> types;
  children.reserve(scenario.columns);
  types.reserve(scenario.columns);
  for (auto channel = 0; channel < scenario.columns; ++channel) {
    children.push_back(BaseVector::createConstant(BIGINT(), static_cast<int64_t>(start + channel), rows, pool));
    types.push_back(BIGINT());
  }

  return std::make_shared<RowVector>(pool, ROW(std::move(types)), nullptr, rows, std::move(children));
}

RowVectorPtr makeEncodedVector(memory::MemoryPool* pool, const EncodedBenchmarkScenario& scenario, int32_t start) {
  switch (scenario.kind) {
    case EncodedVectorKind::kDictionary:
      return makeDictionaryHeavyVector(pool, scenario, start);
    case EncodedVectorKind::kConstant:
      return makeConstantHeavyVector(pool, scenario, start);
  }
  VELOX_UNREACHABLE();
}

std::vector<RowVectorPtr> makeSmallVectors(memory::MemoryPool* pool, const EncodedBenchmarkScenario& scenario) {
  std::vector<RowVectorPtr> vectors;
  vectors.reserve(scenario.inputBatches);
  for (auto batch = 0; batch < scenario.inputBatches; ++batch) {
    vectors.push_back(makeEncodedVector(pool, scenario, batch * scenario.rowsPerBatch));
  }
  return vectors;
}

std::unique_ptr<ColumnarBatchIterator> makeIterator(const std::vector<RowVectorPtr>& vectors) {
  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  batches.reserve(vectors.size());
  for (const auto& vector : vectors) {
    batches.push_back(std::make_shared<VeloxColumnarBatch>(vector));
  }
  return std::make_unique<ColumnarBatchArray>(std::move(batches));
}

int64_t totalRows(const DenseBenchmarkScenario& scenario) {
  return static_cast<int64_t>(scenario.inputBatches) * scenario.rowsPerBatch;
}

int64_t totalRows(const EncodedBenchmarkScenario& scenario) {
  return static_cast<int64_t>(scenario.inputBatches) * scenario.rowsPerBatch;
}

VeloxBatchResizer makeResizeBenchmarkResizer(
    memory::MemoryPool* pool,
    int64_t outputBatchSize,
    std::unique_ptr<ColumnarBatchIterator> iterator,
    std::optional<bool> enableCopyRanges) {
  if (enableCopyRanges.has_value()) {
    return VeloxBatchResizer(
        pool,
        outputBatchSize,
        std::numeric_limits<int32_t>::max(),
        kPreferredBatchBytes,
        std::move(iterator),
        enableCopyRanges.value());
  }
  return VeloxBatchResizer(
      pool, outputBatchSize, std::numeric_limits<int32_t>::max(), kPreferredBatchBytes, std::move(iterator));
}

void runResizeBenchmark(
    benchmark::State& state,
    const DenseBenchmarkScenario& scenario,
    std::optional<bool> enableCopyRanges) {
  auto pool = memory::memoryManager()->addLeafPool("VeloxBatchResizerBenchmark");
  const auto vectors = makeSmallVectors(pool.get(), scenario);
  int64_t rows = 0;

  for (auto _ : state) {
    auto resizer = makeResizeBenchmarkResizer(pool.get(), totalRows(scenario), makeIterator(vectors), enableCopyRanges);
    while (auto out = resizer.next()) {
      rows += out->numRows();
    }
  }

  benchmark::DoNotOptimize(rows);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * totalRows(scenario));
}

void runFallbackResizeBenchmark(
    benchmark::State& state,
    const EncodedBenchmarkScenario& scenario,
    std::optional<bool> enableCopyRanges) {
  auto pool = memory::memoryManager()->addLeafPool("VeloxBatchResizerFallbackBenchmark");
  const auto vectors = makeSmallVectors(pool.get(), scenario);
  int64_t rows = 0;

  for (auto _ : state) {
    auto resizer = makeResizeBenchmarkResizer(pool.get(), totalRows(scenario), makeIterator(vectors), enableCopyRanges);
    while (auto out = resizer.next()) {
      rows += out->numRows();
    }
  }

  benchmark::DoNotOptimize(rows);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * totalRows(scenario));
}

void runDirectChildCopyRangesBenchmark(benchmark::State& state, const DenseBenchmarkScenario& scenario) {
  auto pool = memory::memoryManager()->addLeafPool("VeloxBatchResizerBenchmarkDirectCopy");
  const auto vectors = makeSmallVectors(pool.get(), scenario);
  int64_t rows = 0;

  for (auto _ : state) {
    auto output = RowVector::createEmpty(vectors[0]->type(), pool.get());
    output->resize(totalRows(scenario));
    vector_size_t offset = 0;
    for (const auto& input : vectors) {
      const BaseVector::CopyRange range{0, offset, input->size()};
      for (auto channel = 0; channel < input->children().size(); ++channel) {
        output->childAt(channel)->copyRanges(input->childAt(channel)->loadedVector(), folly::Range(&range, 1));
      }
      offset += input->size();
    }
    rows += output->size();
    benchmark::DoNotOptimize(output);
  }

  benchmark::DoNotOptimize(rows);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * totalRows(scenario));
}

std::shared_ptr<arrow::ResizableBuffer> allocatePayloadBuffer(arrow::MemoryPool* pool, int64_t size) {
  std::shared_ptr<arrow::ResizableBuffer> buffer;
  GLUTEN_ASSIGN_OR_THROW(buffer, arrow::AllocateResizableBuffer(size, pool));
  memset(buffer->mutable_data(), 0x5A, size);
  return buffer;
}

std::shared_ptr<arrow::ResizableBuffer> allocateEmptyPayloadBuffer(arrow::MemoryPool* pool, int64_t size) {
  std::shared_ptr<arrow::ResizableBuffer> buffer;
  GLUTEN_ASSIGN_OR_THROW(buffer, arrow::AllocateResizableBuffer(size, pool));
  return buffer;
}

void addFixedWidthRawBuffers(
    arrow::MemoryPool* pool,
    int32_t rows,
    int32_t columns,
    int32_t valueBytes,
    std::vector<bool>& validityBuffers,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  for (auto channel = 0; channel < columns; ++channel) {
    validityBuffers.push_back(true);
    buffers.push_back(nullptr);
    validityBuffers.push_back(false);
    buffers.push_back(allocatePayloadBuffer(pool, rows * valueBytes));
  }
}

void addFixedWidthRawLayout(int32_t columns, std::vector<bool>& validityBuffers) {
  for (auto channel = 0; channel < columns; ++channel) {
    validityBuffers.push_back(true);
    validityBuffers.push_back(false);
  }
}

void addStringRawBuffers(
    arrow::MemoryPool* pool,
    int32_t rows,
    int32_t stringBytes,
    bool nullable,
    std::vector<bool>& validityBuffers,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  validityBuffers.push_back(true);
  buffers.push_back(nullable ? allocatePayloadBuffer(pool, arrow::bit_util::BytesForBits(rows)) : nullptr);
  validityBuffers.push_back(false);
  buffers.push_back(allocatePayloadBuffer(pool, rows * sizeof(int32_t)));
  validityBuffers.push_back(false);
  buffers.push_back(allocatePayloadBuffer(pool, rows * stringBytes));
}

void addStringRawLayout(std::vector<bool>& validityBuffers) {
  validityBuffers.push_back(true);
  validityBuffers.push_back(false);
  validityBuffers.push_back(false);
}

void addBoolRawBuffers(
    arrow::MemoryPool* pool,
    int32_t rows,
    int32_t columns,
    std::vector<bool>& validityBuffers,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  for (auto channel = 0; channel < columns; ++channel) {
    validityBuffers.push_back(true);
    buffers.push_back(nullptr);
    validityBuffers.push_back(true);
    buffers.push_back(allocatePayloadBuffer(pool, arrow::bit_util::BytesForBits(rows)));
  }
}

void addBoolRawLayout(int32_t columns, std::vector<bool>& validityBuffers) {
  for (auto channel = 0; channel < columns; ++channel) {
    validityBuffers.push_back(true);
    validityBuffers.push_back(true);
  }
}

std::vector<bool> makeRawPayloadValidityBuffers(const DenseBenchmarkScenario& scenario) {
  std::vector<bool> validityBuffers;
  switch (scenario.kind) {
    case DenseVectorKind::kMixed:
      addFixedWidthRawLayout(1, validityBuffers);
      validityBuffers.push_back(true);
      validityBuffers.push_back(false);
      addBoolRawLayout(scenario.boolColumns, validityBuffers);
      addStringRawLayout(validityBuffers);
      break;
    case DenseVectorKind::kFixedWidth:
      addFixedWidthRawLayout(scenario.fixedWidthColumns, validityBuffers);
      break;
    case DenseVectorKind::kStringOnly:
      addStringRawLayout(validityBuffers);
      break;
    case DenseVectorKind::kBoolHeavy:
      addBoolRawLayout(scenario.boolColumns, validityBuffers);
      break;
  }
  return validityBuffers;
}

std::unique_ptr<InMemoryPayload> makeRawPayload(
    arrow::MemoryPool* pool,
    const DenseBenchmarkScenario& scenario,
    const std::vector<bool>& validityBuffers) {
  const auto rows = scenario.rowsPerBatch;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  buffers.reserve(validityBuffers.size());
  std::vector<bool> generatedValidityBuffers;
  switch (scenario.kind) {
    case DenseVectorKind::kMixed:
      addFixedWidthRawBuffers(pool, rows, 1, sizeof(int32_t), generatedValidityBuffers, buffers);
      generatedValidityBuffers.push_back(true);
      buffers.push_back(scenario.nullable ? allocatePayloadBuffer(pool, arrow::bit_util::BytesForBits(rows)) : nullptr);
      generatedValidityBuffers.push_back(false);
      buffers.push_back(allocatePayloadBuffer(pool, rows * sizeof(int64_t)));
      addBoolRawBuffers(pool, rows, scenario.boolColumns, generatedValidityBuffers, buffers);
      addStringRawBuffers(pool, rows, scenario.stringBytes, false, generatedValidityBuffers, buffers);
      break;
    case DenseVectorKind::kFixedWidth:
      addFixedWidthRawBuffers(
          pool, rows, scenario.fixedWidthColumns, sizeof(int64_t), generatedValidityBuffers, buffers);
      break;
    case DenseVectorKind::kStringOnly:
      addStringRawBuffers(pool, rows, scenario.stringBytes, scenario.nullable, generatedValidityBuffers, buffers);
      break;
    case DenseVectorKind::kBoolHeavy:
      addBoolRawBuffers(pool, rows, scenario.boolColumns, generatedValidityBuffers, buffers);
      break;
  }
  GLUTEN_CHECK(generatedValidityBuffers == validityBuffers, "Invalid raw payload buffer layout");
  return std::make_unique<InMemoryPayload>(rows, &validityBuffers, nullptr, std::move(buffers));
}

std::vector<std::unique_ptr<InMemoryPayload>> makeRawPayloads(
    arrow::MemoryPool* pool,
    const DenseBenchmarkScenario& scenario,
    const std::vector<bool>& validityBuffers) {
  std::vector<std::unique_ptr<InMemoryPayload>> payloads;
  payloads.reserve(scenario.inputBatches);
  for (auto batch = 0; batch < scenario.inputBatches; ++batch) {
    payloads.push_back(makeRawPayload(pool, scenario, validityBuffers));
  }
  return payloads;
}

std::unique_ptr<InMemoryPayload> mergeRawPayloadsBulkCopy(
    std::vector<std::unique_ptr<InMemoryPayload>> payloads,
    const std::vector<bool>& validityBuffers,
    arrow::MemoryPool* pool) {
  GLUTEN_CHECK(!payloads.empty(), "Cannot merge empty payloads");

  const auto numBuffers = payloads[0]->numBuffers();
  std::vector<uint32_t> payloadRows;
  payloadRows.reserve(payloads.size());
  uint32_t totalRows = 0;
  std::vector<std::vector<std::shared_ptr<arrow::Buffer>>> inputBuffers(payloads.size());
  std::vector<int64_t> outputSizes(numBuffers, 0);
  std::vector<bool> hasBuffer(numBuffers, false);

  for (auto payloadIdx = 0; payloadIdx < payloads.size(); ++payloadIdx) {
    const auto rows = payloads[payloadIdx]->numRows();
    payloadRows.push_back(rows);
    totalRows += rows;
    inputBuffers[payloadIdx].reserve(numBuffers);
    for (auto bufferIdx = 0; bufferIdx < numBuffers; ++bufferIdx) {
      GLUTEN_ASSIGN_OR_THROW(auto buffer, payloads[payloadIdx]->readBufferAt(bufferIdx));
      if (buffer != nullptr) {
        hasBuffer[bufferIdx] = true;
        if (validityBuffers[bufferIdx]) {
          outputSizes[bufferIdx] = arrow::bit_util::BytesForBits(totalRows);
        } else {
          outputSizes[bufferIdx] += buffer->size();
        }
      }
      inputBuffers[payloadIdx].push_back(std::move(buffer));
    }
  }

  std::vector<std::shared_ptr<arrow::Buffer>> outputBuffers(numBuffers);
  for (auto bufferIdx = 0; bufferIdx < numBuffers; ++bufferIdx) {
    if (hasBuffer[bufferIdx]) {
      outputBuffers[bufferIdx] = allocateEmptyPayloadBuffer(pool, outputSizes[bufferIdx]);
    }
  }

  std::vector<int64_t> byteOffsets(numBuffers, 0);
  uint32_t rowOffset = 0;
  for (auto payloadIdx = 0; payloadIdx < inputBuffers.size(); ++payloadIdx) {
    const auto rows = payloadRows[payloadIdx];
    for (auto bufferIdx = 0; bufferIdx < numBuffers; ++bufferIdx) {
      auto& output = outputBuffers[bufferIdx];
      if (output == nullptr) {
        continue;
      }

      const auto& input = inputBuffers[payloadIdx][bufferIdx];
      if (validityBuffers[bufferIdx]) {
        if (input == nullptr) {
          arrow::bit_util::SetBitsTo(output->mutable_data(), rowOffset, rows, true);
        } else {
          arrow::internal::CopyBitmap(input->data(), 0, rows, output->mutable_data(), rowOffset);
        }
      } else if (input != nullptr) {
        memcpy(output->mutable_data() + byteOffsets[bufferIdx], input->data(), input->size());
        byteOffsets[bufferIdx] += input->size();
      }
    }
    rowOffset += rows;
  }

  return std::make_unique<InMemoryPayload>(totalRows, &validityBuffers, nullptr, std::move(outputBuffers));
}

void BM_VeloxBatchResizerAppendOptOutBaseline(benchmark::State& state, DenseBenchmarkScenario scenario) {
  runResizeBenchmark(state, scenario, false);
}

void BM_VeloxBatchResizerDefaultCopyRanges(benchmark::State& state, DenseBenchmarkScenario scenario) {
  runResizeBenchmark(state, scenario, std::nullopt);
}

void BM_VeloxBatchResizerFallbackAppendOptOutBaseline(benchmark::State& state, EncodedBenchmarkScenario scenario) {
  runFallbackResizeBenchmark(state, scenario, false);
}

void BM_VeloxBatchResizerDefaultCopyRangesFallback(benchmark::State& state, EncodedBenchmarkScenario scenario) {
  runFallbackResizeBenchmark(state, scenario, std::nullopt);
}

void BM_DirectChildCopyRanges(benchmark::State& state, DenseBenchmarkScenario scenario) {
  runDirectChildCopyRangesBenchmark(state, scenario);
}

void BM_ReaderSideRawPayloadBulkCopyModel(benchmark::State& state, DenseBenchmarkScenario scenario) {
  auto* pool = arrow::default_memory_pool();
  const auto validityBuffers = makeRawPayloadValidityBuffers(scenario);
  int64_t rows = 0;

  for (auto _ : state) {
    state.PauseTiming();
    auto payloads = makeRawPayloads(pool, scenario, validityBuffers);
    state.ResumeTiming();

    auto merged = mergeRawPayloadsBulkCopy(std::move(payloads), validityBuffers, pool);
    rows += merged->numRows();
    benchmark::DoNotOptimize(merged);
  }

  benchmark::DoNotOptimize(rows);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * totalRows(scenario));
}

void BM_ReaderSidePreMergedBatchModel(benchmark::State& state, DenseBenchmarkScenario scenario) {
  auto pool = memory::memoryManager()->addLeafPool("VeloxBatchResizerBenchmarkRawMergeModel");
  auto mergedScenario = scenario;
  mergedScenario.inputBatches = 1;
  mergedScenario.rowsPerBatch = totalRows(scenario);
  const std::vector<RowVectorPtr> mergedVector{makeDenseVector(pool.get(), mergedScenario, 0)};
  int64_t rows = 0;

  for (auto _ : state) {
    VeloxBatchResizer resizer(
        pool.get(),
        totalRows(scenario),
        std::numeric_limits<int32_t>::max(),
        kPreferredBatchBytes,
        makeIterator(mergedVector),
        false);
    while (auto out = resizer.next()) {
      rows += out->numRows();
    }
  }

  benchmark::DoNotOptimize(rows);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * totalRows(scenario));
}

#define REGISTER_DENSE_SCENARIO_BENCHMARKS(name, scenario)                     \
  BENCHMARK_CAPTURE(BM_VeloxBatchResizerAppendOptOutBaseline, name, scenario); \
  BENCHMARK_CAPTURE(BM_VeloxBatchResizerDefaultCopyRanges, name, scenario);    \
  BENCHMARK_CAPTURE(BM_DirectChildCopyRanges, name, scenario);                 \
  BENCHMARK_CAPTURE(BM_ReaderSideRawPayloadBulkCopyModel, name, scenario);     \
  BENCHMARK_CAPTURE(BM_ReaderSidePreMergedBatchModel, name, scenario)

#define REGISTER_FALLBACK_SCENARIO_BENCHMARKS(name, scenario)                          \
  BENCHMARK_CAPTURE(BM_VeloxBatchResizerFallbackAppendOptOutBaseline, name, scenario); \
  BENCHMARK_CAPTURE(BM_VeloxBatchResizerDefaultCopyRangesFallback, name, scenario)

REGISTER_DENSE_SCENARIO_BENCHMARKS(Mixed_64x64, kMixed64x64);
REGISTER_DENSE_SCENARIO_BENCHMARKS(Mixed_16x256, kMixed16x256);
REGISTER_DENSE_SCENARIO_BENCHMARKS(Mixed_256x16, kMixed256x16);
REGISTER_DENSE_SCENARIO_BENCHMARKS(Fixed2_64x64, kFixed2_64x64);
REGISTER_DENSE_SCENARIO_BENCHMARKS(Fixed16_64x64, kFixed16_64x64);
REGISTER_DENSE_SCENARIO_BENCHMARKS(LongString_64x64, kLongString64x64);
REGISTER_DENSE_SCENARIO_BENCHMARKS(BoolHeavy_64x64, kBoolHeavy64x64);
REGISTER_FALLBACK_SCENARIO_BENCHMARKS(DictionaryHeavy_64x64, kDictionaryHeavy64x64);
REGISTER_FALLBACK_SCENARIO_BENCHMARKS(ConstantHeavy_64x64, kConstantHeavy64x64);

#undef REGISTER_DENSE_SCENARIO_BENCHMARKS
#undef REGISTER_FALLBACK_SCENARIO_BENCHMARKS

} // namespace
} // namespace gluten

int main(int argc, char** argv) {
  facebook::velox::memory::MemoryManager::initialize(facebook::velox::memory::MemoryManager::Options{});
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}

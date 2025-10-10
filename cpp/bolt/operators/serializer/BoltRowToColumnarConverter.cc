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

#include "BoltRowToColumnarConverter.h"
#include "memory/BoltColumnarBatch.h"
#include "bolt/row/UnsafeRowDeserializers.h"
// #include "bolt/row/UnsafeRowFast.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/arrow/Bridge.h"

using namespace bytedance::bolt;
namespace gluten {
namespace {

inline int64_t calculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) / 64) * 8;
}

inline int64_t getFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
  return nullBitsetWidthInBytes + 8L * index;
}

inline bool isNull(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (static_cast<int64_t>(index) & 0x3f); // mod 64 and shift
  int64_t wordOffset = (static_cast<int64_t>(index) >> 6) * 8;
  int64_t value = *reinterpret_cast<int64_t*>(buffer_address + wordOffset);
  return (value & mask) != 0;
}

int32_t getTotalStringSize(
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress) {
  size_t size = 0;
  for (auto pos = 0; pos < numRows; pos++) {
    if (isNull(memoryAddress + offsets[pos], columnIdx)) {
      continue;
    }

    int64_t offsetAndSize = *(reinterpret_cast<int64_t*>(memoryAddress + offsets[pos] + fieldOffset));
    int32_t length = static_cast<int32_t>(offsetAndSize);
    if (!StringView::isInline(length)) {
      size += length;
    }
  }
  return size;
}

template <TypeKind Kind>
VectorPtr createFlatVector(
    const TypePtr& type,
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<Kind>::NativeType;
  auto typeWidth = sizeof(T);
  auto column = BaseVector::create<FlatVector<T>>(type, numRows, pool);
  auto rawValues = column->template mutableRawValues<uint8_t>();
  auto shift = __builtin_ctz((uint32_t)typeWidth);
  for (auto pos = 0; pos < numRows; pos++) {
    if (!isNull(memoryAddress + offsets[pos], columnIdx)) {
      const uint8_t* srcptr = (memoryAddress + offsets[pos] + fieldOffset);
      uint8_t* destptr = rawValues + (pos << shift);
      memcpy(destptr, srcptr, typeWidth);
    } else {
      column->setNull(pos, true);
    }
  }
  return column;
}

template <>
VectorPtr createFlatVector<TypeKind::HUGEINT>(
    const TypePtr& type,
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress,
    memory::MemoryPool* pool) {
  auto column = BaseVector::create<FlatVector<int128_t>>(type, numRows, pool);
  auto rawValues = column->mutableRawValues<uint8_t>();
  auto typeWidth = sizeof(int128_t);
  auto shift = __builtin_ctz(static_cast<uint32_t>(typeWidth));
  for (auto pos = 0; pos < numRows; pos++) {
    if (!isNull(memoryAddress + offsets[pos], columnIdx)) {
      uint8_t* destptr = rawValues + (pos << shift);
      int64_t offsetAndSize = *reinterpret_cast<int64_t*>(memoryAddress + offsets[pos] + fieldOffset);
      int32_t length = static_cast<int32_t>(offsetAndSize);
      int32_t wordoffset = static_cast<int32_t>(offsetAndSize >> 32);
      std::vector<uint8_t> bytesValue(length);
      memcpy(bytesValue.data(), memoryAddress + offsets[pos] + wordoffset, length);
      uint8_t bytesValue2[16]{};
      GLUTEN_CHECK(length <= 16, "array out of bounds exception");
      for (int k = length - 1; k >= 0; k--) {
        bytesValue2[length - 1 - k] = bytesValue[k];
      }
      if (static_cast<int8_t>(bytesValue[0]) < 0) {
        memset(bytesValue2 + length, 255, 16 - length);
      }
      memcpy(destptr, bytesValue2, typeWidth);
    } else {
      column->setNull(pos, true);
    }
  }
  return column;
}

template <>
VectorPtr createFlatVector<TypeKind::BOOLEAN>(
    const TypePtr& type,
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress,
    memory::MemoryPool* pool) {
  auto column = BaseVector::create<FlatVector<bool>>(type, numRows, pool);
  auto rawValues = column->mutableRawValues<uint64_t>();
  for (auto pos = 0; pos < numRows; pos++) {
    if (!isNull(memoryAddress + offsets[pos], columnIdx)) {
      bool value = *(reinterpret_cast<bool*>(memoryAddress + offsets[pos] + fieldOffset));
      bits::setBit(rawValues, pos, value);
    } else {
      column->setNull(pos, true);
    }
  }
  return column;
}

template <>
VectorPtr createFlatVector<TypeKind::TIMESTAMP>(
    const TypePtr& type,
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress,
    memory::MemoryPool* pool) {
  auto column = BaseVector::create<FlatVector<Timestamp>>(type, numRows, pool);
  for (auto pos = 0; pos < numRows; pos++) {
    if (!isNull(memoryAddress + offsets[pos], columnIdx)) {
      int64_t value = *reinterpret_cast<int64_t*>(memoryAddress + offsets[pos] + fieldOffset);
      column->set(pos, Timestamp::fromMicros(value));
    } else {
      column->setNull(pos, true);
    }
  }
  return column;
}

VectorPtr createFlatVectorStringView(
    const TypePtr& type,
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress,
    memory::MemoryPool* pool) {
  auto column = BaseVector::create<FlatVector<StringView>>(type, numRows, pool);
  auto size = getTotalStringSize(columnIdx, numRows, fieldOffset, offsets, memoryAddress);
  char* rawBuffer = column->getRawStringBufferWithSpace(size, true);
  for (auto pos = 0; pos < numRows; pos++) {
    if (!isNull(memoryAddress + offsets[pos], columnIdx)) {
      int64_t offsetAndSize = *(reinterpret_cast<int64_t*>(memoryAddress + offsets[pos] + fieldOffset));
      int32_t length = static_cast<int32_t>(offsetAndSize);
      int32_t wordoffset = static_cast<int32_t>(offsetAndSize >> 32);
      auto valueSrcPtr = memoryAddress + offsets[pos] + wordoffset;
      if (StringView::isInline(length)) {
        column->set(pos, StringView(reinterpret_cast<char*>(valueSrcPtr), length));
      } else {
        memcpy(rawBuffer, valueSrcPtr, length);
        column->setNoCopy(pos, StringView(rawBuffer, length));
        rawBuffer += length;
      }
    } else {
      column->setNull(pos, true);
    }
  }
  return column;
}

template <>
VectorPtr createFlatVector<TypeKind::VARCHAR>(
    const TypePtr& type,
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress,
    memory::MemoryPool* pool) {
  return createFlatVectorStringView(type, columnIdx, numRows, fieldOffset, offsets, memoryAddress, pool);
}

template <>
VectorPtr createFlatVector<TypeKind::VARBINARY>(
    const TypePtr& type,
    int32_t columnIdx,
    int32_t numRows,
    int64_t fieldOffset,
    std::vector<int64_t>& offsets,
    uint8_t* memoryAddress,
    memory::MemoryPool* pool) {
  return createFlatVectorStringView(type, columnIdx, numRows, fieldOffset, offsets, memoryAddress, pool);
}

template <>
VectorPtr createFlatVector<TypeKind::UNKNOWN>(
    const TypePtr& /*type*/,
    int32_t /*columnIdx*/,
    int32_t numRows,
    int64_t /*fieldOffset*/,
    std::vector<int64_t>& /*offsets*/,
    uint8_t* /*memoryAddress*/,
    memory::MemoryPool* pool) {
  auto nulls = allocateNulls(numRows, pool, bits::kNull);
  return std::make_shared<FlatVector<UnknownValue>>(
      pool,
      UNKNOWN(),
      nulls,
      numRows,
      nullptr, // values
      std::vector<BufferPtr>{}); // stringBuffers
}

bool supporteType(const RowTypePtr rowType) {
  for (auto i = 0; i < rowType->size(); i++) {
    auto kind = rowType->childAt(i)->kind();
    switch (kind) {
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::ROW:
        return false;
      default:
        break;
    }
  }
  return true;
}

} // namespace

BoltRowToColumnarConverter::BoltRowToColumnarConverter(
    struct ArrowSchema* cSchema,
    std::shared_ptr<memory::MemoryPool> memoryPool)
    : RowToColumnarConverter(), pool_(memoryPool) {
  rowType_ = importFromArrow(*cSchema); // otherwise the c schema leaks memory
  ArrowSchemaRelease(cSchema);
}

std::shared_ptr<ColumnarBatch>
BoltRowToColumnarConverter::convert(int64_t numRows, int64_t* rowLength, uint8_t* memoryAddress) {
  if (supporteType(asRowType(rowType_))) {
    return convertPrimitive(numRows, rowLength, memoryAddress);
  }

  std::vector<std::optional<std::string_view>> data;
  int64_t offset = 0;
  for (auto i = 0; i < numRows; i++) {
    data.emplace_back(std::string_view(reinterpret_cast<const char*>(memoryAddress + offset), rowLength[i]));
    offset += rowLength[i];
  }
  auto vp = row::UnsafeRowDeserializer::deserialize(data, rowType_, pool_.get());
  return std::make_shared<BoltColumnarBatch>(std::dynamic_pointer_cast<RowVector>(vp));
}

std::shared_ptr<ColumnarBatch>
BoltRowToColumnarConverter::convertPrimitive(int64_t numRows, int64_t* rowLength, uint8_t* memoryAddress) {
  auto numFields = rowType_->size();
  int64_t nullBitsetWidthInBytes = calculateBitSetWidthInBytes(numFields);
  std::vector<int64_t> offsets;
  offsets.resize(numRows);
  for (auto i = 1; i < numRows; i++) {
    offsets[i] = offsets[i - 1] + rowLength[i - 1];
  }

  std::vector<VectorPtr> columns;
  columns.resize(numFields);

  for (auto i = 0; i < numFields; i++) {
    auto fieldOffset = getFieldOffset(nullBitsetWidthInBytes, i);
    auto& type = rowType_->childAt(i);
    columns[i] = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        createFlatVector, type->kind(), type, i, numRows, fieldOffset, offsets, memoryAddress, pool_.get());
  }

  auto rowVector = std::make_shared<RowVector>(pool_.get(), rowType_, BufferPtr(nullptr), numRows, std::move(columns));
  return std::make_shared<BoltColumnarBatch>(rowVector);
}

} // namespace gluten

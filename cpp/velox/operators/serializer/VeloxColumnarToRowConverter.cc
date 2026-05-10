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

#include "VeloxColumnarToRowConverter.h"
#include <velox/common/base/SuccinctPrinter.h>
#include <cstdint>

#include "memory/VeloxColumnarBatch.h"
#include "utils/Exception.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"

using namespace facebook;

namespace gluten {
namespace {

constexpr int64_t kMicrosToNanos = 1000;

bool isTimeMicroUtc(const velox::TypePtr& type) {
  return type->equivalent(*velox::TIME_MICRO_UTC());
}

bool containsTimeMicroUtc(const velox::TypePtr& type) {
  if (isTimeMicroUtc(type)) {
    return true;
  }

  switch (type->kind()) {
    case velox::TypeKind::ARRAY:
      return containsTimeMicroUtc(type->asArray().elementType());
    case velox::TypeKind::MAP:
      return containsTimeMicroUtc(type->asMap().keyType()) || containsTimeMicroUtc(type->asMap().valueType());
    case velox::TypeKind::ROW: {
      const auto& rowType = type->asRow();
      for (const auto& child : rowType.children()) {
        if (containsTimeMicroUtc(child)) {
          return true;
        }
      }
      return false;
    }
    default:
      return false;
  }
}

velox::VectorPtr normalizeTimeForSparkUnsafeRow(const velox::VectorPtr& vector, velox::memory::MemoryPool* pool);

velox::VectorPtr normalizeTimeScalarForSparkUnsafeRow(const velox::VectorPtr& vector, velox::memory::MemoryPool* pool) {
  velox::DecodedVector decoded(*vector);
  auto normalized = velox::BaseVector::create(velox::BIGINT(), vector->size(), pool);
  auto* flat = normalized->asFlatVector<int64_t>();

  for (auto row = 0; row < vector->size(); ++row) {
    if (decoded.isNullAt(row)) {
      flat->setNull(row, true);
    } else {
      flat->set(row, decoded.valueAt<int64_t>(row) * kMicrosToNanos);
    }
  }
  return normalized;
}

velox::VectorPtr loadedFlatVector(const velox::VectorPtr& vector) {
  auto loaded = velox::BaseVector::loadedVectorShared(vector);
  velox::BaseVector::flattenVector(loaded);
  if (loaded->isLazy()) {
    loaded = loaded->as<velox::LazyVector>()->loadedVectorShared();
  }
  return loaded;
}

velox::VectorPtr normalizeArrayForSparkUnsafeRow(const velox::VectorPtr& vector, velox::memory::MemoryPool* pool) {
  auto array = loadedFlatVector(vector)->as<velox::ArrayVector>();
  auto elements = normalizeTimeForSparkUnsafeRow(array->elements(), pool);
  if (elements == array->elements()) {
    return vector;
  }
  return std::make_shared<velox::ArrayVector>(
      pool,
      velox::ARRAY(elements->type()),
      array->nulls(),
      array->size(),
      array->offsets(),
      array->sizes(),
      elements,
      array->getNullCount());
}

velox::VectorPtr normalizeMapForSparkUnsafeRow(const velox::VectorPtr& vector, velox::memory::MemoryPool* pool) {
  auto map = loadedFlatVector(vector)->as<velox::MapVector>();
  auto keys = normalizeTimeForSparkUnsafeRow(map->mapKeys(), pool);
  auto values = normalizeTimeForSparkUnsafeRow(map->mapValues(), pool);
  if (keys == map->mapKeys() && values == map->mapValues()) {
    return vector;
  }
  return std::make_shared<velox::MapVector>(
      pool,
      velox::MAP(keys->type(), values->type()),
      map->nulls(),
      map->size(),
      map->offsets(),
      map->sizes(),
      keys,
      values,
      map->getNullCount(),
      map->hasSortedKeys());
}

velox::RowVectorPtr normalizeRowForSparkUnsafeRow(
    const velox::RowVectorPtr& rowVector,
    velox::memory::MemoryPool* pool) {
  std::vector<velox::VectorPtr> children;
  children.reserve(rowVector->childrenSize());
  bool changed = false;
  for (const auto& child : rowVector->children()) {
    auto normalized = normalizeTimeForSparkUnsafeRow(child, pool);
    changed = changed || normalized != child;
    children.emplace_back(std::move(normalized));
  }

  if (!changed) {
    return rowVector;
  }

  std::vector<velox::TypePtr> childTypes;
  childTypes.reserve(children.size());
  for (const auto& child : children) {
    childTypes.emplace_back(child->type());
  }
  return std::make_shared<velox::RowVector>(
      pool,
      velox::ROW(velox::asRowType(rowVector->type())->names(), std::move(childTypes)),
      rowVector->nulls(),
      rowVector->size(),
      std::move(children),
      rowVector->getNullCount());
}

velox::VectorPtr normalizeTimeForSparkUnsafeRow(const velox::VectorPtr& vector, velox::memory::MemoryPool* pool) {
  if (!containsTimeMicroUtc(vector->type())) {
    return vector;
  }

  if (isTimeMicroUtc(vector->type())) {
    return normalizeTimeScalarForSparkUnsafeRow(vector, pool);
  }

  switch (vector->typeKind()) {
    case velox::TypeKind::ARRAY:
      return normalizeArrayForSparkUnsafeRow(vector, pool);
    case velox::TypeKind::MAP:
      return normalizeMapForSparkUnsafeRow(vector, pool);
    case velox::TypeKind::ROW:
      return normalizeRowForSparkUnsafeRow(std::dynamic_pointer_cast<velox::RowVector>(loadedFlatVector(vector)), pool);
    default:
      return vector;
  }
}

} // namespace

void VeloxColumnarToRowConverter::refreshStates(facebook::velox::RowVectorPtr rowVector, int64_t startRow) {
  rowVectorForUnsafeRow_ = normalizeRowForSparkUnsafeRow(rowVector, veloxPool_.get());

  auto vectorLength = rowVectorForUnsafeRow_->size();
  numCols_ = rowVectorForUnsafeRow_->childrenSize();

  fast_ = std::make_unique<velox::row::UnsafeRowFast>(rowVectorForUnsafeRow_);

  int64_t totalMemorySize;

  if (auto fixedRowSize = velox::row::UnsafeRowFast::fixedRowSize(velox::asRowType(rowVectorForUnsafeRow_->type()))) {
    auto rowSize = fixedRowSize.value();
    // make sure it has at least one row
    numRows_ = std::max<int32_t>(1, std::min<int64_t>(memThreshold_ / rowSize, vectorLength - startRow));
    totalMemorySize = numRows_ * rowSize;
  } else {
    // Calculate the first row size
    totalMemorySize = fast_->rowSize(startRow);

    auto endRow = startRow + 1;
    for (; endRow < vectorLength; ++endRow) {
      auto rowSize = fast_->rowSize(endRow);
      if (UNLIKELY(totalMemorySize + rowSize > memThreshold_)) {
        break;
      } else {
        totalMemorySize += rowSize;
      }
    }
    // Make sure the threshold is larger than the first row size
    numRows_ = endRow - startRow;
  }

  if (nullptr == veloxBuffers_ || veloxBuffers_->capacity() < totalMemorySize) {
    veloxBuffers_ = velox::AlignedBuffer::allocate<uint8_t>(totalMemorySize, veloxPool_.get());
  }

  bufferAddress_ = veloxBuffers_->asMutable<uint8_t>();
  memset(bufferAddress_, 0, sizeof(int8_t) * totalMemorySize);
}

void VeloxColumnarToRowConverter::convert(std::shared_ptr<ColumnarBatch> cb, int64_t startRow) {
  auto veloxBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
  refreshStates(veloxBatch->getRowVector(), startRow);

  // Initialize the offsets_ , lengths_
  lengths_.clear();
  offsets_.clear();
  lengths_.resize(numRows_, 0);
  offsets_.resize(numRows_, 0);

  size_t offset = 0;
  for (auto i = 0; i < numRows_; ++i) {
    auto rowSize = fast_->serialize(startRow + i, reinterpret_cast<char*>(bufferAddress_ + offset));
    lengths_[i] = rowSize;
    if (i > 0) {
      offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
    }
    offset += rowSize;
  }
}

} // namespace gluten

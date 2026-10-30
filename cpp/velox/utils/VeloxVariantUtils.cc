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

#include "utils/VeloxVariantUtils.h"

#include <cstring>

#include "velox/type/variant/VariantType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace gluten {

namespace {

constexpr size_t kValueSizeBytes = sizeof(int32_t);

VectorPtr flattened(VectorPtr vector) {
  BaseVector::flattenVector(vector);
  return vector;
}

// VARIANT to [int32 size of value][value][metadata].
VectorPtr encodeVariant(const VectorPtr& vector, memory::MemoryPool* pool) {
  const auto* variant = vector->asChecked<RowVector>();
  const auto* metadata = variant->childAt(VariantType::kMetadataIndex)->asFlatVector<StringView>();
  const auto* value = variant->childAt(VariantType::kValueIndex)->asFlatVector<StringView>();
  VELOX_CHECK(metadata != nullptr && value != nullptr, "Expected flat Variant fields, got {}", vector->toString());
  const auto size = vector->size();

  auto result = BaseVector::create<FlatVector<StringView>>(VARBINARY(), size, pool);
  std::string buffer;
  for (vector_size_t i = 0; i < size; ++i) {
    // A Variant with null fields is invalid. Treat it as a null Variant.
    if (variant->isNullAt(i) || metadata->isNullAt(i) || value->isNullAt(i)) {
      result->setNull(i, true);
      continue;
    }
    const auto metadataBytes = metadata->valueAt(i);
    const auto valueBytes = value->valueAt(i);
    const int32_t valueSize = valueBytes.size();
    buffer.resize(kValueSizeBytes + valueBytes.size() + metadataBytes.size());
    std::memcpy(buffer.data(), &valueSize, kValueSizeBytes);
    std::memcpy(buffer.data() + kValueSizeBytes, valueBytes.data(), valueBytes.size());
    std::memcpy(buffer.data() + kValueSizeBytes + valueBytes.size(), metadataBytes.data(), metadataBytes.size());
    // set() copies the bytes into the string buffers of 'result'.
    result->set(i, StringView(buffer));
  }
  return result;
}

// [int32 size of value][value][metadata] to VARIANT. The result points into
// the string buffers of 'vector'.
VectorPtr decodeVariant(const VectorPtr& vector, memory::MemoryPool* pool) {
  const auto* binary = vector->asFlatVector<StringView>();
  VELOX_CHECK_NOT_NULL(binary, "Expected flat VARBINARY for Variant, got {}", vector->toString());
  const auto size = vector->size();

  auto metadata = BaseVector::create<FlatVector<StringView>>(VARBINARY(), size, pool);
  auto value = BaseVector::create<FlatVector<StringView>>(VARBINARY(), size, pool);
  metadata->setStringBuffers(binary->stringBuffers());
  value->setStringBuffers(binary->stringBuffers());
  for (vector_size_t i = 0; i < size; ++i) {
    if (binary->isNullAt(i)) {
      // A null Variant still needs non-null children to stay valid.
      metadata->setNoCopy(i, StringView());
      value->setNoCopy(i, StringView());
      continue;
    }
    const auto bytes = binary->valueAt(i);
    VELOX_CHECK_GE(bytes.size(), kValueSizeBytes, "Variant binary is truncated.");
    int32_t valueSize;
    std::memcpy(&valueSize, bytes.data(), kValueSizeBytes);
    VELOX_CHECK(
        valueSize >= 0 && static_cast<size_t>(valueSize) <= bytes.size() - kValueSizeBytes,
        "Variant binary has an invalid value size.");
    value->setNoCopy(i, StringView(bytes.data() + kValueSizeBytes, valueSize));
    metadata->setNoCopy(
        i,
        StringView(
            bytes.data() + kValueSizeBytes + valueSize,
            static_cast<int32_t>(bytes.size() - kValueSizeBytes - valueSize)));
  }
  return std::make_shared<RowVector>(
      pool, VARIANT(), vector->nulls(), size, std::vector<VectorPtr>{std::move(metadata), std::move(value)});
}

// Rebuilds the complex vector 'vector' with type 'newType' and the children
// returned by 'convertChild'. 'vector' must be flat.
template <typename ConvertChild>
VectorPtr
rebuild(const VectorPtr& vector, const TypePtr& newType, memory::MemoryPool* pool, ConvertChild&& convertChild) {
  switch (vector->typeKind()) {
    case TypeKind::ROW: {
      const auto* row = vector->asChecked<RowVector>();
      std::vector<VectorPtr> children;
      children.reserve(row->childrenSize());
      for (auto i = 0; i < row->childrenSize(); ++i) {
        children.push_back(convertChild(row->childAt(i), newType->childAt(i)));
      }
      return std::make_shared<RowVector>(pool, newType, row->nulls(), row->size(), std::move(children));
    }
    case TypeKind::ARRAY: {
      const auto* array = vector->asChecked<ArrayVector>();
      return std::make_shared<ArrayVector>(
          pool,
          newType,
          array->nulls(),
          array->size(),
          array->offsets(),
          array->sizes(),
          convertChild(array->elements(), newType->childAt(0)));
    }
    case TypeKind::MAP: {
      const auto* map = vector->asChecked<MapVector>();
      return std::make_shared<MapVector>(
          pool,
          newType,
          map->nulls(),
          map->size(),
          map->offsets(),
          map->sizes(),
          convertChild(map->mapKeys(), newType->childAt(0)),
          convertChild(map->mapValues(), newType->childAt(1)));
    }
    default:
      VELOX_UNREACHABLE("Unexpected type: {}", vector->type()->toString());
  }
}

} // namespace

bool containsVariantType(const TypePtr& type) {
  if (isVariantType(type)) {
    return true;
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (containsVariantType(type->childAt(i))) {
      return true;
    }
  }
  return false;
}

TypePtr variantToUnsafeRowType(const TypePtr& type) {
  if (isVariantType(type)) {
    return VARBINARY();
  }
  if (!containsVariantType(type)) {
    return type;
  }
  switch (type->kind()) {
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      std::vector<TypePtr> children;
      children.reserve(rowType.size());
      for (const auto& child : rowType.children()) {
        children.push_back(variantToUnsafeRowType(child));
      }
      return ROW(std::vector<std::string>(rowType.names()), std::move(children));
    }
    case TypeKind::ARRAY:
      return ARRAY(variantToUnsafeRowType(type->childAt(0)));
    case TypeKind::MAP:
      return MAP(variantToUnsafeRowType(type->childAt(0)), variantToUnsafeRowType(type->childAt(1)));
    default:
      VELOX_UNREACHABLE("Unexpected type: {}", type->toString());
  }
}

VectorPtr variantToUnsafeRowBinary(VectorPtr vector, memory::MemoryPool* pool) {
  if (!containsVariantType(vector->type())) {
    return vector;
  }
  vector = flattened(std::move(vector));
  if (isVariantType(vector->type())) {
    return encodeVariant(vector, pool);
  }
  return rebuild(vector, variantToUnsafeRowType(vector->type()), pool, [&](const VectorPtr& child, const TypePtr&) {
    return variantToUnsafeRowBinary(child, pool);
  });
}

VectorPtr unsafeRowBinaryToVariant(VectorPtr vector, const TypePtr& type, memory::MemoryPool* pool) {
  if (!containsVariantType(type)) {
    return vector;
  }
  vector = flattened(std::move(vector));
  if (isVariantType(type)) {
    return decodeVariant(vector, pool);
  }
  return rebuild(vector, type, pool, [&](const VectorPtr& child, const TypePtr& childType) {
    return unsafeRowBinaryToVariant(child, childType, pool);
  });
}

} // namespace gluten

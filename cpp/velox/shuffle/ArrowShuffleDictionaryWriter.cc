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

#include "shuffle/ArrowShuffleDictionaryWriter.h"
#include "shuffle/Utils.h"
#include "utils/VeloxArrowUtils.h"

#include <arrow/array/builder_dict.h>

namespace gluten {

static constexpr double kDictionaryFactor = 0.5;

using ArrowDictionaryIndexType = int32_t;

template <typename T>
using is_dictionary_binary_type =
    std::integral_constant<bool, std::is_same_v<arrow::BinaryType, T> || std::is_same_v<arrow::StringType, T>>;

template <typename T>
using is_dictionary_primitive_type = std::integral_constant<
    bool,
    std::is_same_v<arrow::Int64Type, T> || std::is_same_v<arrow::DoubleType, T> ||
        std::is_same_v<arrow::TimestampType, T>>;

template <typename T, typename R = void>
using enable_if_dictionary_type =
    std::enable_if_t<is_dictionary_primitive_type<T>::value || is_dictionary_binary_type<T>::value, R>;

namespace {
arrow::Status writeDictionaryBuffer(
    const uint8_t* data,
    size_t size,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    arrow::io::OutputStream* out) {
  ARROW_RETURN_NOT_OK(out->Write(&size, sizeof(size_t)));

  if (size == 0) {
    return arrow::Status::OK();
  }

  GLUTEN_DCHECK(data != nullptr, "Cannot write null data");

  if (codec != nullptr) {
    size_t compressedLength = codec->MaxCompressedLen(size, data);
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(compressedLength, pool));
    ARROW_ASSIGN_OR_RAISE(
        compressedLength, codec->Compress(size, data, compressedLength, buffer->mutable_data_as<uint8_t>()));

    ARROW_RETURN_NOT_OK(out->Write(&compressedLength, sizeof(size_t)));
    ARROW_RETURN_NOT_OK(out->Write(buffer->data_as<void>(), compressedLength));
  } else {
    ARROW_RETURN_NOT_OK(out->Write(data, size));
  }

  return arrow::Status::OK();
}

} // namespace

template <typename ArrowType>
class DictionaryStorageImpl : public ShuffleDictionaryStorage {
 public:
  using ValueType = typename ArrowType::c_type;

  DictionaryStorageImpl(arrow::MemoryPool* pool, arrow::util::Codec* codec) : pool_(pool), codec_(codec) {
    table_ = std::make_shared<arrow::internal::DictionaryMemoTable>(pool, std::make_shared<ArrowType>());
  }

  arrow::Result<ArrowDictionaryIndexType> getOrUpdate(const ValueType& value) {
    ArrowDictionaryIndexType memoIndex;
    ARROW_RETURN_NOT_OK(table_->GetOrInsert<ArrowType>(value, &memoIndex));
    return memoIndex;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    std::shared_ptr<arrow::ArrayData> data;
    ARROW_RETURN_NOT_OK(table_->GetArrayData(0, &data));

    DLOG(INFO) << "ShuffleDictionaryStorage::serialize num elements: " << data->length;

    ARROW_RETURN_IF(
        data->buffers.size() != 2, arrow::Status::Invalid("Invalid dictionary for type: ", data->type->ToString()));

    const auto& values = data->buffers[1];
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(values->data(), values->size(), pool_, codec_, out));

    return arrow::Status::OK();
  }

  int32_t size() const {
    return table_->size();
  }

 private:
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  std::shared_ptr<arrow::internal::DictionaryMemoTable> table_;
};

class BinaryShuffleDictionaryStorage : public ShuffleDictionaryStorage {
 public:
  using MemoType = arrow::LargeBinaryType;

  BinaryShuffleDictionaryStorage(arrow::MemoryPool* pool, arrow::util::Codec* codec) : pool_(pool), codec_(codec) {
    table_ = std::make_shared<arrow::internal::DictionaryMemoTable>(pool, std::make_shared<MemoType>());
  }

  arrow::Result<ArrowDictionaryIndexType> getOrUpdate(const std::string_view& view) const {
    ArrowDictionaryIndexType memoIndex;
    ARROW_RETURN_NOT_OK(table_->GetOrInsert<MemoType>(view, &memoIndex));
    return memoIndex;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    std::shared_ptr<arrow::ArrayData> data;
    ARROW_RETURN_NOT_OK(table_->GetArrayData(0, &data));

    ARROW_RETURN_IF(data->buffers.size() != 3, arrow::Status::Invalid("Invalid dictionary for binary type"));

    DLOG(INFO) << "BinaryShuffleDictionaryStorage::serialize num elements: " << table_->size();

    ARROW_ASSIGN_OR_RAISE(auto lengths, offsetToLength(data->length, data->buffers[1]));
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(lengths->data(), lengths->size(), pool_, codec_, out));

    const auto& values = data->buffers[2];
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(values->data(), values->size(), pool_, codec_, out));

    return arrow::Status::OK();
  }

  int32_t size() const {
    return table_->size();
  }

 private:
  arrow::Result<std::shared_ptr<arrow::Buffer>> offsetToLength(
      size_t numElements,
      const std::shared_ptr<arrow::Buffer>& offsets) const {
    ARROW_ASSIGN_OR_RAISE(auto lengths, arrow::AllocateBuffer(sizeof(StringLengthType) * numElements, pool_));
    auto* rawLengths = lengths->mutable_data_as<StringLengthType>();
    const auto* rawOffsets = offsets->data_as<arrow::TypeTraits<MemoType>::OffsetType::c_type>();

    for (auto i = 0; i < numElements; ++i) {
      rawLengths[i] = static_cast<StringLengthType>(rawOffsets[i + 1] - rawOffsets[i]);
    }

    return lengths;
  }

  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  std::shared_ptr<arrow::internal::DictionaryMemoTable> table_;
};

template <>
class DictionaryStorageImpl<arrow::BinaryType> : public BinaryShuffleDictionaryStorage {
 public:
  DictionaryStorageImpl(arrow::MemoryPool* pool, arrow::util::Codec* codec)
      : BinaryShuffleDictionaryStorage(pool, codec) {}
};

template <>
class DictionaryStorageImpl<arrow::StringType> : public BinaryShuffleDictionaryStorage {
 public:
  DictionaryStorageImpl(arrow::MemoryPool* pool, arrow::util::Codec* codec)
      : BinaryShuffleDictionaryStorage(pool, codec) {}
};

namespace {
template <typename ArrowType>
arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionary(
    int32_t numRows,
    const std::shared_ptr<arrow::Buffer>& validityBuffer,
    const std::shared_ptr<arrow::Buffer>& valueBuffer,
    const std::shared_ptr<arrow::Buffer>&,
    const std::shared_ptr<DictionaryStorageImpl<ArrowType>>& dictionary,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(ArrowDictionaryIndexType) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<ArrowDictionaryIndexType>();

  const auto* values = valueBuffer->data_as<typename ArrowType::c_type>();

  if (validityBuffer != nullptr) {
    const auto* nulls = validityBuffer->data();
    for (auto i = 0; i < numRows; ++i) {
      if (arrow::bit_util::GetBit(nulls, i)) {
        ARROW_ASSIGN_OR_RAISE(*rawIndices, dictionary->getOrUpdate(values[i]));
      }
      ++rawIndices;
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      ARROW_ASSIGN_OR_RAISE(*rawIndices++, dictionary->getOrUpdate(values[i]));
    }
  }

  return indices;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionaryForBinary(
    int32_t numRows,
    const std::shared_ptr<arrow::Buffer>& validityBuffer,
    const std::shared_ptr<arrow::Buffer>& lengthBuffer,
    const std::shared_ptr<arrow::Buffer>& valueBuffer,
    const std::shared_ptr<BinaryShuffleDictionaryStorage>& dictionary,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(ArrowDictionaryIndexType) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<ArrowDictionaryIndexType>();

  const auto* lengths = lengthBuffer->data_as<uint32_t>();
  const auto* values = valueBuffer->data_as<char>();

  size_t offset = 0;

  if (validityBuffer != nullptr) {
    const auto* nulls = validityBuffer->data();
    for (auto i = 0; i < numRows; ++i) {
      if (arrow::bit_util::GetBit(nulls, i)) {
        std::string_view view(values + offset, lengths[i]);
        offset += lengths[i];
        ARROW_ASSIGN_OR_RAISE(*rawIndices, dictionary->getOrUpdate(view));
      }
      ++rawIndices;
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      std::string_view view(values + offset, lengths[i]);
      offset += lengths[i];
      ARROW_ASSIGN_OR_RAISE(*rawIndices++, dictionary->getOrUpdate(view));
    }
  }

  return indices;
}

template <>
arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionary<arrow::BinaryType>(
    int32_t numRows,
    const std::shared_ptr<arrow::Buffer>& validityBuffer,
    const std::shared_ptr<arrow::Buffer>& lengthBuffer,
    const std::shared_ptr<arrow::Buffer>& valueBuffer,
    const std::shared_ptr<DictionaryStorageImpl<arrow::BinaryType>>& dictionary,
    arrow::MemoryPool* pool) {
  return updateDictionaryForBinary(numRows, validityBuffer, lengthBuffer, valueBuffer, dictionary, pool);
}

template <>
arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionary<arrow::StringType>(
    int32_t numRows,
    const std::shared_ptr<arrow::Buffer>& validityBuffer,
    const std::shared_ptr<arrow::Buffer>& lengthBuffer,
    const std::shared_ptr<arrow::Buffer>& valueBuffer,
    const std::shared_ptr<DictionaryStorageImpl<arrow::StringType>>& dictionary,
    arrow::MemoryPool* pool) {
  return updateDictionaryForBinary(numRows, validityBuffer, lengthBuffer, valueBuffer, dictionary, pool);
}
} // namespace

class ValueUpdater {
 public:
  template <typename ArrowType>
  enable_if_dictionary_type<ArrowType, arrow::Status> Visit(const ArrowType&) {
    bool dictionaryExists = false;
    std::shared_ptr<DictionaryStorageImpl<ArrowType>> dictionary;
    if (const auto& it = writer->dictionaries_.find(fieldIdx); it != writer->dictionaries_.end()) {
      dictionaryExists = true;
      dictionary = std::dynamic_pointer_cast<DictionaryStorageImpl<ArrowType>>(it->second);
    } else {
      dictionary = std::make_shared<DictionaryStorageImpl<ArrowType>>(writer->dictionaryPool_.get(), writer->codec_);
    }

    ARROW_ASSIGN_OR_RAISE(
        auto indices, updateDictionary<ArrowType>(numRows, nulls, values, binaryValues, dictionary, pool));

    results.push_back(nulls);

    // Discard dictionary.
    if (!dictionaryExists && dictionary->size() > numRows * kDictionaryFactor) {
      dictionaryCreated = false;
      results.push_back(values);
      if (binaryValues != nullptr) {
        results.push_back(binaryValues);
      }
      return arrow::Status::OK();
    }

    if (!dictionaryExists) {
      writer->dictionaries_[fieldIdx] = dictionary;
    }

    dictionaryCreated = true;
    results.push_back(indices);

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Decimal128Type& type) {
    // Only support short decimal.
    return Visit(arrow::Int64Type());
  }

  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::TypeError("Not implemented for type: ", type.ToString());
  }

  ArrowShuffleDictionaryWriter* writer;
  arrow::MemoryPool* pool;
  int32_t fieldIdx;
  int32_t numRows;
  std::shared_ptr<arrow::Buffer> nulls{nullptr};
  std::shared_ptr<arrow::Buffer> values{nullptr};
  std::shared_ptr<arrow::Buffer> binaryValues{nullptr};

  std::vector<std::shared_ptr<arrow::Buffer>>& results;
  bool& dictionaryCreated;
};

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> ArrowShuffleDictionaryWriter::updateAndGet(
    const std::shared_ptr<arrow::Schema>& schema,
    int32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  ARROW_RETURN_NOT_OK(initSchema(schema));

  std::vector<std::shared_ptr<arrow::Buffer>> results;

  size_t bufferIdx = 0;
  for (auto i = 0; i < schema->num_fields(); ++i) {
    switch (fieldTypes_[i]) {
      case FieldType::kNull:
      case FieldType::kComplex:
        break;
      case FieldType::kFixedWidth:
        results.emplace_back(buffers[bufferIdx++]);
        results.emplace_back(buffers[bufferIdx++]);
        break;
      case FieldType::kBinary:
        results.emplace_back(buffers[bufferIdx++]);
        results.emplace_back(buffers[bufferIdx++]);
        results.emplace_back(buffers[bufferIdx++]);
        break;
      case FieldType::kSupportsDictionary: {
        const auto fieldType = schema_->field(i)->type();
        bool isBinaryType =
            fieldType->id() == arrow::BinaryType::type_id || fieldType->id() == arrow::StringType::type_id;

        bool isDictionaryCreated = false;
        ValueUpdater valueUpdater{
            this,
            memoryManager_->defaultArrowMemoryPool(),
            i,
            numRows,
            buffers[bufferIdx++],
            buffers[bufferIdx++],
            isBinaryType ? buffers[bufferIdx++] : nullptr,
            results,
            isDictionaryCreated};

        ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*fieldType, &valueUpdater));

        if (!isDictionaryCreated) {
          ARROW_RETURN_NOT_OK(blackList(i));
        }

        break;
      }
    }
  }

  if (hasComplexType_) {
    results.emplace_back(buffers[bufferIdx++]);
  }

  GLUTEN_DCHECK(bufferIdx == buffers.size(), "Not all buffers are consumed.");

  return results;
}

arrow::Status ArrowShuffleDictionaryWriter::serialize(arrow::io::OutputStream* out) {
  auto bitMapSize = arrow::bit_util::RoundUpToMultipleOf8(schema_->num_fields());
  std::vector<uint8_t> bitMap(bitMapSize);

  for (auto fieldIdx : dictionaryFields_) {
    arrow::bit_util::SetBit(bitMap.data(), fieldIdx);
  }

  ARROW_RETURN_NOT_OK(out->Write(bitMap.data(), bitMapSize));

  for (auto fieldIdx : dictionaryFields_) {
    auto it = dictionaries_.find(fieldIdx);
    GLUTEN_DCHECK(
        it != dictionaries_.end(),
        "Invalid dictionary field index: " + std::to_string(fieldIdx));

    ARROW_RETURN_NOT_OK(it->second->serialize(out));

    dictionaries_.erase(it);
  }

  return arrow::Status::OK();
}

int64_t ArrowShuffleDictionaryWriter::numDictionaryFields() {
  return dictionaryFields_.size();
}

int64_t ArrowShuffleDictionaryWriter::getDictionarySize() {
  return dictionaryPool_->bytes_allocated();
}

arrow::Status ArrowShuffleDictionaryWriter::initSchema(const std::shared_ptr<arrow::Schema>& schema) {
  if (schema_ == nullptr) {
    schema_ = schema;

    rowType_ = fromArrowSchema(schema);
    fieldTypes_.resize(rowType_->size());

    for (auto i = 0; i < rowType_->size(); ++i) {
      switch (rowType_->childAt(i)->kind()) {
        case facebook::velox::TypeKind::UNKNOWN:
          fieldTypes_[i] = FieldType::kNull;
          break;
        case facebook::velox::TypeKind::ARRAY:
        case facebook::velox::TypeKind::MAP:
        case facebook::velox::TypeKind::ROW:
          fieldTypes_[i] = FieldType::kComplex;
          hasComplexType_ = true;
          break;
        case facebook::velox::TypeKind::VARBINARY:
        case facebook::velox::TypeKind::VARCHAR:
        case facebook::velox::TypeKind::DOUBLE:
        case facebook::velox::TypeKind::BIGINT:
          fieldTypes_[i] = FieldType::kSupportsDictionary;
          dictionaryFields_.emplace(i);
          break;
        default:
          fieldTypes_[i] = FieldType::kFixedWidth;
          break;
      }
    }
  } else if (schema_ != schema) {
    return arrow::Status::Invalid("Schema mismatch");
  }
  return arrow::Status::OK();
}

arrow::Status ArrowShuffleDictionaryWriter::blackList(int32_t fieldId) {
  switch (const auto typeId = schema_->field(fieldId)->type()->id()) {
    case arrow::BinaryType::type_id:
    case arrow::StringType::type_id:
      fieldTypes_[fieldId] = FieldType::kBinary;
      break;
    default: {
      if (!arrow::is_fixed_width(typeId)) {
        return arrow::Status::Invalid("Invalid field type: ", schema_->field(fieldId)->type()->ToString());
      }
      fieldTypes_[fieldId] = FieldType::kFixedWidth;
      break;
    }
  }

  dictionaryFields_.erase(fieldId);

  return arrow::Status::OK();
}
} // namespace gluten

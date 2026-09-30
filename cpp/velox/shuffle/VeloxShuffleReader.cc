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

#include "shuffle/VeloxShuffleReader.h"

#include <arrow/array/array_binary.h>
#include <arrow/io/buffered.h>

#include "compute/VeloxBackend.h"
#include "memory/VeloxColumnarBatch.h"
#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/Common.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"

#include "velox/common/memory/ByteStream.h"
#include "velox/row/CompactRow.h"
#include "velox/serializers/PrestoHeader.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/PrestoSerializerSerializationUtils.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <algorithm>
#include <array>
#include <sstream>

#include "VeloxGpuAsyncShuffleReader.h"
#include "config/VeloxConfig.h"

#ifdef GLUTEN_ENABLE_GPU
#include "shuffle/VeloxGpuAsyncShuffleReader.h"
#include "shuffle/VeloxGpuShuffleReader.h"
#endif

using namespace facebook::velox;

namespace gluten {

namespace {

arrow::Result<BlockType> readBlockType(arrow::io::InputStream* inputStream) {
  BlockType type;
  ARROW_ASSIGN_OR_RAISE(auto bytes, inputStream->Read(sizeof(BlockType), &type));
  if (bytes == 0) {
    // Reach EOS.
    return BlockType::kEndOfStream;
  }
  return type;
}

uint32_t validateHashShuffleReaderBatchSize(int32_t batchSize) {
  GLUTEN_CHECK(batchSize > 0, fmt::format("Hash shuffle reader batch size must be positive, but got {}", batchSize));
  return static_cast<uint32_t>(batchSize);
}

struct BufferViewReleaser {
  BufferViewReleaser() : BufferViewReleaser(nullptr) {}

  BufferViewReleaser(std::shared_ptr<arrow::Buffer> arrowBuffer) : bufferReleaser_(std::move(arrowBuffer)) {}

  void addRef() const {}

  void release() const {}

 private:
  const std::shared_ptr<arrow::Buffer> bufferReleaser_;
};

BufferPtr wrapInBufferViewAsOwner(const void* buffer, size_t length, std::shared_ptr<arrow::Buffer> bufferReleaser) {
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer), length, {std::move(bufferReleaser)});
}

BufferPtr convertToVeloxBuffer(std::shared_ptr<arrow::Buffer> buffer) {
  if (buffer == nullptr) {
    return nullptr;
  }
  return wrapInBufferViewAsOwner(buffer->data(), buffer->size(), buffer);
}

template <TypeKind Kind, typename T = typename TypeTraits<Kind>::NativeType>
VectorPtr readFlatVector(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto valuesOrIndices = buffers[bufferIdx++];

  nulls = nulls == nullptr || nulls->size() == 0 ? BufferPtr(nullptr) : nulls;

  if (dictionary != nullptr) {
    return BaseVector::wrapInDictionary(nulls, valuesOrIndices, length, dictionary);
  }

  return std::make_shared<FlatVector<T>>(
      pool, type, nulls, length, std::move(valuesOrIndices), std::vector<BufferPtr>{});
}

template <>
VectorPtr readFlatVector<TypeKind::UNKNOWN>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  return BaseVector::createNullConstant(type, length, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::HUGEINT>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto valuesOrIndices = buffers[bufferIdx++];

  // Because if buffer does not compress, it will get from netty, the address maynot aligned 16B, which will cause
  // int128_t = xxx coredump by instruction movdqa
  const auto* addr = valuesOrIndices->as<facebook::velox::int128_t>();
  if ((reinterpret_cast<uintptr_t>(addr) & 0xf) != 0) {
    auto alignedBuffer = AlignedBuffer::allocate<char>(valuesOrIndices->size(), pool);
    fastCopy(alignedBuffer->asMutable<char>(), valuesOrIndices->as<char>(), valuesOrIndices->size());
    valuesOrIndices = alignedBuffer;
  }

  nulls = nulls == nullptr || nulls->size() == 0 ? BufferPtr(nullptr) : nulls;

  if (dictionary != nullptr) {
    return BaseVector::wrapInDictionary(nulls, valuesOrIndices, length, dictionary);
  }

  return std::make_shared<FlatVector<int128_t>>(
      pool, type, nulls, length, std::move(valuesOrIndices), std::vector<BufferPtr>{});
}

VectorPtr readFlatVectorStringView(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto lengthOrIndices = buffers[bufferIdx++];

  nulls = nulls == nullptr || nulls->size() == 0 ? BufferPtr(nullptr) : nulls;

  if (dictionary != nullptr) {
    return BaseVector::wrapInDictionary(nulls, lengthOrIndices, length, dictionary);
  }

  auto valueBuffer = buffers[bufferIdx++];

  const auto* rawLength = lengthOrIndices->as<StringLengthType>();
  const auto* valueBufferPtr = valueBuffer->as<char>();

  auto values = AlignedBuffer::allocate<char>(sizeof(StringView) * length, pool);
  auto* rawValues = values->asMutable<StringView>();

  uint64_t offset = 0;
  for (int32_t i = 0; i < length; ++i) {
    rawValues[i] = StringView(valueBufferPtr + offset, rawLength[i]);
    offset += rawLength[i];
  }

  std::vector<BufferPtr> stringBuffers;
  stringBuffers.emplace_back(valueBuffer);

  return std::make_shared<FlatVector<StringView>>(
      pool, type, nulls, length, std::move(values), std::move(stringBuffers));
}

template <>
VectorPtr readFlatVector<TypeKind::VARCHAR>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, dictionary, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::VARBINARY>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, dictionary, pool);
}

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<BufferInputStream>(byteRanges);
  return byteStream;
}

RowVectorPtr readComplexType(BufferPtr buffer, RowTypePtr& rowType, memory::MemoryPool* pool) {
  RowVectorPtr result;
  auto byteStream = toByteStream(const_cast<uint8_t*>(buffer->as<uint8_t>()), buffer->size());
  auto serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
  serializer::presto::PrestoVectorSerde::PrestoOptions options;
  options.useLosslessTimestamp = true;
  serde->deserialize(byteStream.get(), pool, rowType, &result, &options);
  return result;
}

RowTypePtr getComplexWriteType(const std::vector<TypePtr>& types) {
  std::vector<std::string> complexTypeColNames;
  std::vector<TypePtr> complexTypeChildrens;
  for (int32_t i = 0; i < types.size(); ++i) {
    auto kind = types[i]->kind();
    switch (kind) {
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::ARRAY: {
        complexTypeColNames.emplace_back(types[i]->name());
        complexTypeChildrens.emplace_back(types[i]);
      } break;
      default:
        break;
    }
  }
  return std::make_shared<const RowType>(std::move(complexTypeColNames), std::move(complexTypeChildrens));
}

RowVectorPtr deserialize(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<BufferPtr>& buffers,
    const std::vector<int32_t>& dictionaryFields,
    const std::vector<VectorPtr>& dictionaries,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> children;
  auto types = type->as<TypeKind::ROW>().children();

  std::vector<VectorPtr> complexChildren;
  auto complexRowType = getComplexWriteType(types);
  if (complexRowType->children().size() > 0) {
    complexChildren = readComplexType(buffers[buffers.size() - 1], complexRowType, pool)->children();
  }

  int32_t bufferIdx = 0;
  int32_t complexIdx = 0;
  int32_t dictionaryIdx = 0;
  for (size_t i = 0; i < types.size(); ++i) {
    const auto kind = types[i]->kind();
    switch (kind) {
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::ARRAY: {
        children.emplace_back(std::move(complexChildren[complexIdx]));
        complexIdx++;
      } break;
      default: {
        VectorPtr dictionary{nullptr};
        if (!dictionaryFields.empty() && dictionaryIdx < dictionaryFields.size() &&
            dictionaryFields[dictionaryIdx] == i) {
          dictionary = dictionaries[dictionaryIdx++];
        }
        auto res = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            readFlatVector, kind, buffers, bufferIdx, numRows, types[i], dictionary, pool);
        children.emplace_back(std::move(res));
      } break;
    }
  }

  return std::make_shared<RowVector>(pool, type, BufferPtr(nullptr), numRows, children);
}

std::shared_ptr<VeloxColumnarBatch> makeColumnarBatch(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> arrowBuffers,
    const std::vector<int32_t>& dictionaryFields,
    const std::vector<VectorPtr>& dictionaries,
    memory::MemoryPool* pool,
    int64_t& deserializeTime) {
  ScopedTimer timer(&deserializeTime);
  std::vector<BufferPtr> veloxBuffers;
  veloxBuffers.reserve(arrowBuffers.size());
  for (auto& buffer : arrowBuffers) {
    veloxBuffers.push_back(convertToVeloxBuffer(std::move(buffer)));
  }
  auto rowVector = deserialize(type, numRows, veloxBuffers, dictionaryFields, dictionaries, pool);
  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

std::shared_ptr<VeloxColumnarBatch> makeColumnarBatch(
    RowTypePtr type,
    std::unique_ptr<InMemoryPayload> payload,
    memory::MemoryPool* pool,
    int64_t& deserializeTime) {
  ScopedTimer timer(&deserializeTime);
  std::vector<BufferPtr> veloxBuffers;
  auto numBuffers = payload->numBuffers();
  veloxBuffers.reserve(numBuffers);
  for (size_t i = 0; i < numBuffers; ++i) {
    GLUTEN_ASSIGN_OR_THROW(auto buffer, payload->readBufferAt(i));
    veloxBuffers.push_back(convertToVeloxBuffer(std::move(buffer)));
  }
  auto rowVector = deserialize(type, payload->numRows(), veloxBuffers, {}, {}, pool);
  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

arrow::Result<BufferPtr>
readDictionaryBuffer(arrow::io::InputStream* in, facebook::velox::memory::MemoryPool* pool, arrow::util::Codec* codec) {
  size_t bufferSize;

  ARROW_RETURN_NOT_OK(in->Read(sizeof(bufferSize), &bufferSize));
  auto buffer = facebook::velox::AlignedBuffer::allocate<char>(bufferSize, pool, std::nullopt, true);

  if (bufferSize == 0) {
    return buffer;
  }

  if (codec != nullptr) {
    size_t compressedSize;
    ARROW_RETURN_NOT_OK(in->Read(sizeof(compressedSize), &compressedSize));
    auto compressedBuffer = facebook::velox::AlignedBuffer::allocate<char>(compressedSize, pool, std::nullopt, true);
    ARROW_RETURN_NOT_OK(in->Read(compressedSize, compressedBuffer->asMutable<void>()));
    ARROW_ASSIGN_OR_RAISE(
        auto decompressedSize,
        codec->Decompress(compressedSize, compressedBuffer->as<uint8_t>(), bufferSize, buffer->asMutable<uint8_t>()));
    ARROW_RETURN_IF(
        decompressedSize != bufferSize,
        arrow::Status::IOError(
            fmt::format("Decompressed size doesn't equal to original size: ({} vs {})", decompressedSize, bufferSize)));
  } else {
    ARROW_RETURN_NOT_OK(in->Read(bufferSize, buffer->asMutable<void>()));
  }
  return buffer;
}

arrow::Result<VectorPtr> readDictionaryForBinary(
    arrow::io::InputStream* in,
    const TypePtr& type,
    facebook::velox::memory::MemoryPool* pool,
    arrow::util::Codec* codec) {
  // Read length buffer.
  ARROW_ASSIGN_OR_RAISE(auto lengthBuffer, readDictionaryBuffer(in, pool, codec));
  const auto* lengthBufferPtr = lengthBuffer->as<StringLengthType>();

  // Read value buffer.
  ARROW_ASSIGN_OR_RAISE(auto valueBuffer, readDictionaryBuffer(in, pool, codec));
  const auto* valueBufferPtr = valueBuffer->as<char>();

  // Build StringViews.
  const auto numElements = lengthBuffer->size() / sizeof(StringLengthType);
  auto values = AlignedBuffer::allocate<char>(sizeof(StringView) * numElements, pool, std::nullopt, true);
  auto* rawValues = values->asMutable<StringView>();

  uint64_t offset = 0;
  for (size_t i = 0; i < numElements; ++i) {
    rawValues[i] = StringView(valueBufferPtr + offset, lengthBufferPtr[i]);
    offset += lengthBufferPtr[i];
  }

  std::vector<BufferPtr> stringBuffers;
  stringBuffers.emplace_back(valueBuffer);

  return std::make_shared<FlatVector<StringView>>(
      pool, type, BufferPtr(nullptr), numElements, std::move(values), std::move(stringBuffers));
}

template <TypeKind Kind, typename NativeType = typename TypeTraits<Kind>::NativeType>
arrow::Result<VectorPtr> readDictionary(
    arrow::io::InputStream* in,
    const TypePtr& type,
    facebook::velox::memory::MemoryPool* pool,
    arrow::util::Codec* codec) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, readDictionaryBuffer(in, pool, codec));

  const auto numElements = buffer->size() / sizeof(NativeType);

  return std::make_shared<FlatVector<NativeType>>(
      pool, type, BufferPtr(nullptr), numElements, std::move(buffer), std::vector<BufferPtr>{});
}

template <>
arrow::Result<VectorPtr> readDictionary<TypeKind::VARCHAR>(
    arrow::io::InputStream* in,
    const TypePtr& type,
    facebook::velox::memory::MemoryPool* pool,
    arrow::util::Codec* codec) {
  return readDictionaryForBinary(in, type, pool, codec);
}

template <>
arrow::Result<VectorPtr> readDictionary<TypeKind::VARBINARY>(
    arrow::io::InputStream* in,
    const TypePtr& type,
    facebook::velox::memory::MemoryPool* pool,
    arrow::util::Codec* codec) {
  return readDictionaryForBinary(in, type, pool, codec);
}

} // namespace

class VeloxDictionaryReader {
 public:
  VeloxDictionaryReader(
      const facebook::velox::RowTypePtr& rowType,
      facebook::velox::memory::MemoryPool* veloxPool,
      arrow::util::Codec* codec)
      : rowType_(rowType), veloxPool_(veloxPool), codec_(codec) {}

  arrow::Result<std::vector<int32_t>> readFields(arrow::io::InputStream* in) const {
    // Read bitmap.
    auto bitMapSize = arrow::bit_util::RoundUpToMultipleOf8(rowType_->size());
    std::vector<uint8_t> bitMap(bitMapSize);

    RETURN_NOT_OK(in->Read(bitMapSize, bitMap.data()));

    std::vector<int32_t> fields;
    for (auto i = 0; i < rowType_->size(); ++i) {
      if (arrow::bit_util::GetBit(bitMap.data(), i)) {
        fields.push_back(i);
      }
    }

    return fields;
  }

  arrow::Result<std::vector<VectorPtr>> readDictionaries(arrow::io::InputStream* in, const std::vector<int32_t>& fields)
      const {
    // Read dictionary buffers.
    std::vector<VectorPtr> dictionaries;
    for (const auto i : fields) {
      auto dictionary = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          readDictionary, rowType_->childAt(i)->kind(), in, rowType_->childAt(i), veloxPool_, codec_);
      dictionaries.emplace_back();
      ARROW_ASSIGN_OR_RAISE(dictionaries.back(), dictionary);
    }

    return dictionaries;
  }

 private:
  facebook::velox::RowTypePtr rowType_;
  facebook::velox::memory::MemoryPool* veloxPool_;
  arrow::util::Codec* codec_;
};

VeloxHashShuffleReaderDeserializer::VeloxHashShuffleReaderDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const facebook::velox::RowTypePtr& rowType,
    int32_t batchSize,
    int64_t readerBufferSize,
    VeloxMemoryManager* memoryManager,
    std::vector<bool> isValidityBuffer,
    bool hasComplexType,
    bool enableStreamMerge,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : streamReader_(streamReader),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      batchSize_(validateHashShuffleReaderBatchSize(batchSize)),
      readerBufferSize_(readerBufferSize),
      memoryManager_(memoryManager),
      isValidityBuffer_(std::move(isValidityBuffer)),
      hasComplexType_(hasComplexType),
      enableStreamMerge_(enableStreamMerge),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime) {}

VeloxHashShuffleReaderDeserializer::~VeloxHashShuffleReaderDeserializer() {
  if (in_ != nullptr) {
    if (auto status = in_->Close(); !status.ok()) {
      LOG(WARNING) << "Input stream is not closed properly. Error: " << status.message();
    }
  }
}

bool VeloxHashShuffleReaderDeserializer::shouldSkipMerge() const {
  // Stream merge is a reader-side raw payload fast path: for plain payloads it
  // concatenates buffers before Velox vectors are materialized, avoiding the generic
  // RowVector append cost paid by VeloxResizeBatchesExec. Keep complex and dictionary
  // payloads on the existing per-payload path; VeloxResizeBatchesExec can be enabled
  // separately as the generic complement for those cases.
  return !enableStreamMerge_ || hasComplexType_ || !dictionaryFields_.empty();
}

bool VeloxHashShuffleReaderDeserializer::resolveNextBlockType() {
  if (blockTypeResolved_) {
    return true;
  }

  GLUTEN_ASSIGN_OR_THROW(auto blockType, readBlockType(in_.get()));
  switch (blockType) {
    case BlockType::kEndOfStream:
      GLUTEN_THROW_NOT_OK(in_->Close());
      in_ = nullptr;
      return false;
    case BlockType::kDictionary: {
      VeloxDictionaryReader reader(rowType_, memoryManager_->getLeafMemoryPool().get(), codec_.get());
      GLUTEN_ASSIGN_OR_THROW(dictionaryFields_, reader.readFields(in_.get()));
      GLUTEN_ASSIGN_OR_THROW(dictionaries_, reader.readDictionaries(in_.get(), dictionaryFields_));

      GLUTEN_ASSIGN_OR_THROW(blockType, readBlockType(in_.get()));
      GLUTEN_CHECK(blockType == BlockType::kDictionaryPayload, "Invalid block type for dictionary payload");
    } break;
    case BlockType::kDictionaryPayload: {
      GLUTEN_CHECK(
          !dictionaryFields_.empty() && !dictionaries_.empty(),
          "Dictionaries cannot be empty when reading dictionary payload");
    } break;
    case BlockType::kPlainPayload: {
      if (!dictionaryFields_.empty()) {
        // Clear previous dictionaries if the next block is a plain payload.
        dictionaryFields_.clear();
        dictionaries_.clear();
      }
    } break;
    default:
      throw GlutenException(fmt::format("Unsupported block type: {}", static_cast<int32_t>(blockType)));
  }
  blockTypeResolved_ = true;
  return true;
}

void VeloxHashShuffleReaderDeserializer::loadNextStream() {
  if (reachedEos_) {
    return;
  }

  auto in = streamReader_->readNextStream(memoryManager_->defaultArrowMemoryPool());
  if (in == nullptr) {
    reachedEos_ = true;
    return;
  }

  if (!dictionaryFields_.empty() || !dictionaries_.empty()) {
    dictionaryFields_.clear();
    dictionaries_.clear();
  }
  blockTypeResolved_ = false;

  if (readerBufferSize_ > 0) {
    GLUTEN_ASSIGN_OR_THROW(
        in_,
        arrow::io::BufferedInputStream::Create(
            readerBufferSize_, memoryManager_->defaultArrowMemoryPool(), std::move(in)));
  } else {
    in_ = std::move(in);
  }
}

std::shared_ptr<ColumnarBatch> VeloxHashShuffleReaderDeserializer::next() {
  while (true) {
    if (in_ == nullptr) {
      if (merged_) {
        return makeColumnarBatch(
            rowType_, std::move(merged_), memoryManager_->getLeafMemoryPool().get(), deserializeTime_);
      }

      loadNextStream();

      if (reachedEos_) {
        return nullptr;
      }
    }
    if (resolveNextBlockType()) {
      break;
    }
  }

  if (shouldSkipMerge()) {
    if (merged_) {
      return makeColumnarBatch(
          rowType_, std::move(merged_), memoryManager_->getLeafMemoryPool().get(), deserializeTime_);
    }

    uint32_t numRows = 0;
    GLUTEN_ASSIGN_OR_THROW(
        auto arrowBuffers,
        BlockPayload::deserialize(
            in_.get(), codec_, memoryManager_->defaultArrowMemoryPool(), numRows, deserializeTime_, decompressTime_));

    blockTypeResolved_ = false;

    return makeColumnarBatch(
        rowType_,
        numRows,
        std::move(arrowBuffers),
        dictionaryFields_,
        dictionaries_,
        memoryManager_->getLeafMemoryPool().get(),
        deserializeTime_);
  }

  std::vector<std::shared_ptr<arrow::Buffer>> arrowBuffers{};
  uint32_t numRows = 0;
  while (!merged_ || merged_->numRows() < batchSize_) {
    if (in_ == nullptr) {
      if (merged_) {
        break;
      }

      loadNextStream();
      if (reachedEos_) {
        break;
      }
    }
    if (!resolveNextBlockType()) {
      continue;
    }

    if (shouldSkipMerge()) {
      break;
    }

    GLUTEN_ASSIGN_OR_THROW(
        arrowBuffers,
        BlockPayload::deserialize(
            in_.get(), codec_, memoryManager_->defaultArrowMemoryPool(), numRows, deserializeTime_, decompressTime_));

    blockTypeResolved_ = false;

    if (!merged_) {
      merged_ = std::make_unique<InMemoryPayload>(numRows, &isValidityBuffer_, schema_, std::move(arrowBuffers));
      arrowBuffers.clear();
      continue;
    }

    auto mergedRows = merged_->numRows() + numRows;
    if (mergedRows > batchSize_) {
      break;
    }

    auto append = std::make_unique<InMemoryPayload>(numRows, &isValidityBuffer_, schema_, std::move(arrowBuffers));
    GLUTEN_ASSIGN_OR_THROW(
        merged_,
        InMemoryPayload::merge(std::move(merged_), std::move(append), memoryManager_->defaultArrowMemoryPool()));
    arrowBuffers.clear();
  }

  if (!merged_) {
    return nullptr;
  }

  auto columnarBatch =
      makeColumnarBatch(rowType_, std::move(merged_), memoryManager_->getLeafMemoryPool().get(), deserializeTime_);

  if (!arrowBuffers.empty()) {
    merged_ = std::make_unique<InMemoryPayload>(numRows, &isValidityBuffer_, schema_, std::move(arrowBuffers));
  }

  return columnarBatch;
}

std::unique_ptr<ColumnarBatchIterator> VeloxHashShuffleReaderDeserializer::deserializeStreams() {
  return std::make_unique<SyncShuffleReaderIterator<VeloxHashShuffleReaderDeserializer>>(this);
}

VeloxSortShuffleReaderDeserializer::VeloxSortShuffleReaderDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const RowTypePtr& rowType,
    int32_t batchSize,
    int64_t readerBufferSize,
    int64_t deserializerBufferSize,
    VeloxMemoryManager* memoryManager,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : streamReader_(streamReader),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      batchSize_(batchSize),
      readerBufferSize_(readerBufferSize),
      deserializerBufferSize_(deserializerBufferSize),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime),
      memoryManager_(memoryManager) {}

VeloxSortShuffleReaderDeserializer::~VeloxSortShuffleReaderDeserializer() {
  if (in_ != nullptr) {
    if (auto in = std::dynamic_pointer_cast<CompressedInputStream>(in_)) {
      decompressTime_ += in->decompressTime();
    }
    if (auto status = in_->Close(); !status.ok()) {
      LOG(WARNING) << "Input stream is not closed properly. Error: " << status.message();
    }
  }
}

std::unique_ptr<ColumnarBatchIterator> VeloxSortShuffleReaderDeserializer::deserializeStreams() {
  return std::make_unique<SyncShuffleReaderIterator<VeloxSortShuffleReaderDeserializer>>(this);
}

std::shared_ptr<ColumnarBatch> VeloxSortShuffleReaderDeserializer::next() {
  if (in_ == nullptr) {
    loadNextStream();
  }

  if (reachedEos_) {
    return nullptr;
  }

  if (rowBuffer_ == nullptr) {
    rowBuffer_ = AlignedBuffer::allocate<char>(
        deserializerBufferSize_, memoryManager_->getLeafMemoryPool().get(), std::nullopt, true /*allocateExact*/);
    rowBufferPtr_ = rowBuffer_->asMutable<char>();
    data_.reserve(batchSize_);
  }

  if (lastRowSize_ != 0) {
    if (lastRowSize_ > rowBuffer_->size()) {
      reallocateRowBuffer();
    }
    readNextRow();
  }

  while (cachedRows_ < batchSize_) {
    GLUTEN_ASSIGN_OR_THROW(auto bytes, in_->Read(sizeof(RowSizeType), &lastRowSize_));
    while (bytes == 0) {
      GLUTEN_THROW_NOT_OK(in_->Close());
      // Current stream has no more data. Try to load the next stream.
      loadNextStream();
      if (reachedEos_) {
        if (bytesRead_ > 0) {
          return deserializeToBatch();
        }
        // If we reached EOS and have no rows, return nullptr.
        return nullptr;
      }
      GLUTEN_ASSIGN_OR_THROW(bytes, in_->Read(sizeof(RowSizeType), &lastRowSize_));
    }

    if (lastRowSize_ + bytesRead_ > rowBuffer_->size()) {
      if (bytesRead_ > 0) {
        // If we have already read some rows, return the current batch.
        return deserializeToBatch();
      }
      reallocateRowBuffer();
    }

    readNextRow();
  }

  return deserializeToBatch();
}

std::shared_ptr<ColumnarBatch> VeloxSortShuffleReaderDeserializer::deserializeToBatch() {
  ScopedTimer timer(&deserializeTime_);

  auto rowVector =
      facebook::velox::row::CompactRow::deserialize(data_, rowType_, memoryManager_->getLeafMemoryPool().get());

  cachedRows_ = 0;
  bytesRead_ = 0;
  data_.resize(0);
  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

void VeloxSortShuffleReaderDeserializer::reallocateRowBuffer() {
  auto newSize = facebook::velox::bits::nextPowerOfTwo(lastRowSize_);
  LOG(WARNING) << "Row size " << lastRowSize_ << " exceeds current buffer size " << rowBuffer_->size()
               << ". Resizing buffer to " << newSize;
  rowBuffer_ = AlignedBuffer::allocate<char>(
      newSize, memoryManager_->getLeafMemoryPool().get(), std::nullopt, true /*allocateExact*/);
  rowBufferPtr_ = rowBuffer_->asMutable<char>();
}

void VeloxSortShuffleReaderDeserializer::loadNextStream() {
  if (reachedEos_) {
    return;
  }

  auto in = streamReader_->readNextStream(memoryManager_->defaultArrowMemoryPool());
  if (in == nullptr) {
    reachedEos_ = true;
    return;
  }

  if (codec_ != nullptr) {
    GLUTEN_ASSIGN_OR_THROW(
        in_, CompressedInputStream::Make(codec_.get(), std::move(in), memoryManager_->defaultArrowMemoryPool()));
  } else {
    if (readerBufferSize_ > 0) {
      GLUTEN_ASSIGN_OR_THROW(
          in_,
          arrow::io::BufferedInputStream::Create(
              readerBufferSize_, memoryManager_->defaultArrowMemoryPool(), std::move(in)));
    } else {
      in_ = std::move(in);
    }
  }
}

void VeloxSortShuffleReaderDeserializer::readNextRow() {
  GLUTEN_THROW_NOT_OK(in_->Read(lastRowSize_, rowBufferPtr_ + bytesRead_));
  data_.push_back(std::string_view(rowBufferPtr_ + bytesRead_, lastRowSize_));
  bytesRead_ += lastRowSize_;
  lastRowSize_ = 0;
  ++cachedRows_;
}

// A single-window refill stream: each next() overwrites the sole range with up
// to buffer_ capacity bytes from the underlying InputStream. Because earlier
// windows are physically discarded on refill, callers can always read forward
// and rewind within the current window, but cannot revisit bytes from prior
// windows — seekp() fails fast instead of reading overwritten data.
class VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream : public facebook::velox::ByteInputStream {
 public:
  RssSortShuffleReaderInputStream(std::shared_ptr<arrow::io::InputStream> input, facebook::velox::BufferPtr buffer);

  bool hasNext();

  /// Refills the window from the underlying InputStream. Throws if
  /// 'throwIfPastEnd' and the stream is already exhausted.
  void next(bool throwIfPastEnd = true);

  size_t remainingSize() const override;

  size_t size() const override {
    return atEnd_ ? totalBytesRead_ : std::numeric_limits<size_t>::max();
  }

  bool atEnd() const override {
    return atEnd_;
  }

  std::streampos tellp() const override;

  void seekp(std::streampos position) override;

  uint8_t readByte() override;

  void readBytes(uint8_t* bytes, int32_t size) override;

  std::string_view nextView(int64_t size) override;

  void skip(int32_t size) override;

  std::string toString() const override;

  int32_t remainingInWindow() const {
    if (ranges_.empty()) {
      return 0;
    }
    return ranges_[0].size - ranges_[0].position;
  }

  uint8_t* data() const {
    return ranges_.empty() ? nullptr : ranges_[0].buffer + ranges_[0].position;
  }

  void advance(int32_t n) {
    VELOX_CHECK(!ranges_.empty() && ranges_[0].position + n <= ranges_[0].size);
    ranges_[0].position += n;
  }

 private:
  void setRange(ByteRange range) {
    ranges_.resize(1);
    ranges_[0] = range;
    current_ = ranges_.data();
  }

  std::shared_ptr<arrow::io::InputStream> in_;
  const facebook::velox::BufferPtr buffer_;
  uint64_t offset_ = -1;
  uint64_t totalBytesRead_ = 0;
  bool atEnd_ = false;
  std::vector<ByteRange> ranges_;
};

VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::RssSortShuffleReaderInputStream(
    std::shared_ptr<arrow::io::InputStream> input,
    facebook::velox::BufferPtr buffer)
    : in_(std::move(input)), buffer_(std::move(buffer)) {
  next(false);
}

bool VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::hasNext() {
  if (offset_ == 0) {
    return false;
  }
  if (ranges_[0].position >= ranges_[0].size) {
    next(false);
    return offset_ != 0;
  }
  return true;
}

void VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::next(bool throwIfPastEnd) {
  const uint32_t readBytes = buffer_->capacity();
  offset_ = 0;
  GLUTEN_ASSIGN_OR_THROW(int64_t realBytes, in_->Read(readBytes, buffer_->asMutable<char>()));
  if (realBytes > 0) {
    offset_ = realBytes;
    totalBytesRead_ += realBytes;
    atEnd_ = false;
    setRange({buffer_->asMutable<uint8_t>(), static_cast<int32_t>(realBytes), 0});
  } else {
    atEnd_ = true;
    if (throwIfPastEnd) {
      VELOX_FAIL(
          "Reading past end of RssSortShuffleReaderInputStream, real bytes = {}, totalBytesRead = {}",
          realBytes,
          totalBytesRead_);
    }
  }
}

VeloxRssSortShuffleReaderDeserializer::VeloxRssSortShuffleReaderDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    VeloxMemoryManager* memoryManager,
    const RowTypePtr& rowType,
    int32_t batchSize,
    facebook::velox::common::CompressionKind veloxCompressionType,
    int64_t& deserializeTime)
    : streamReader_(streamReader),
      memoryManager_(memoryManager),
      rowType_(rowType),
      batchSize_(batchSize),
      veloxCompressionType_(veloxCompressionType),
      serde_(getNamedVectorSerde("Presto")),
      deserializeTime_(deserializeTime) {
  serdeOptions_ = {false, veloxCompressionType_};
}

VeloxRssSortShuffleReaderDeserializer::~VeloxRssSortShuffleReaderDeserializer() {
  if (arrowIn_ != nullptr) {
    if (auto status = arrowIn_->Close(); !status.ok()) {
      LOG(WARNING) << "Input stream is not closed properly. Error: " << status.message();
    }
  }
}

std::shared_ptr<ColumnarBatch> VeloxRssSortShuffleReaderDeserializer::next() {
  if (in_ == nullptr || !in_->hasNext()) {
    do {
      loadNextStream();
      if (reachedEos_) {
        return nullptr;
      }
    } while (!in_->hasNext());
  }

  ScopedTimer timer(&deserializeTime_);

  auto rowVector = readPage();

  if (rowVector->size() >= batchSize_) {
    return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
  }

  while (rowVector->size() < batchSize_ && in_->hasNext()) {
    rowVector->append(readPage().get());
  }

  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

std::unique_ptr<ColumnarBatchIterator> VeloxRssSortShuffleReaderDeserializer::deserializeStreams() {
  return std::make_unique<SyncShuffleReaderIterator<VeloxRssSortShuffleReaderDeserializer>>(this);
}

void VeloxRssSortShuffleReaderDeserializer::loadNextStream() {
  if (reachedEos_) {
    return;
  }

  if (arrowIn_ != nullptr) {
    GLUTEN_THROW_NOT_OK(arrowIn_->Close());
  }
  arrowIn_ = streamReader_->readNextStream(memoryManager_->defaultArrowMemoryPool());

  if (arrowIn_ == nullptr) {
    reachedEos_ = true;
    return;
  }

  constexpr uint64_t kMaxReadBufferSize = (1 << 20) - AlignedBuffer::kPaddedSize;
  auto buffer = AlignedBuffer::allocate<char>(kMaxReadBufferSize, memoryManager_->getLeafMemoryPool().get());
  in_ = std::make_unique<RssSortShuffleReaderInputStream>(std::move(arrowIn_), std::move(buffer));
}

size_t VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::remainingSize() const {
  return std::numeric_limits<unsigned long>::max();
}

uint8_t VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::readByte() {
  if (current_->position < current_->size) {
    return current_->buffer[current_->position++];
  }
  next();
  return readByte();
}

void VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::readBytes(uint8_t* bytes, int32_t size) {
  VELOX_CHECK_GE(size, 0, "Attempting to read negative number of bytes");
  int32_t offset = 0;
  for (;;) {
    int32_t available = current_->size - current_->position;
    int32_t numUsed = std::min(available, size);
    simd::memcpy(bytes + offset, current_->buffer + current_->position, numUsed);
    offset += numUsed;
    size -= numUsed;
    current_->position += numUsed;
    if (!size) {
      return;
    }
    next();
  }
}

void VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::skip(int32_t size) {
  VELOX_CHECK_GE(size, 0, "Attempting to skip negative number of bytes");
  for (;;) {
    int32_t available = current_->size - current_->position;
    int32_t numUsed = std::min(available, size);
    size -= numUsed;
    current_->position += numUsed;
    if (!size) {
      return;
    }
    next();
  }
}

std::string VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::toString() const {
  std::stringstream oss;
  oss << ranges_.size() << " ranges (position/size) [";
  for (const auto& range : ranges_) {
    oss << "(" << range.position << "/" << range.size << (&range == current_ ? " current" : "") << ")";
    if (&range != &ranges_.back()) {
      oss << ",";
    }
  }
  oss << "]";
  return oss.str();
}

std::string_view VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::nextView(int64_t size) {
  VELOX_CHECK_GE(size, 0, "Attempting to view negative number of bytes");
  if (ranges_.empty()) {
    return std::string_view(nullptr, 0);
  }
  if (ranges_[0].position == ranges_[0].size) {
    // Current window is exhausted. For single-window refill streams, next()
    // overwrites the window with fresh data, so attempt refill before
    // reporting end-of-stream.
    next(false);
    if (ranges_.empty() || ranges_[0].position == ranges_[0].size) {
      return std::string_view(nullptr, 0);
    }
  }
  VELOX_DCHECK(ranges_[0].size > 0);
  const int32_t position = ranges_[0].position;
  const int64_t viewSize = std::min<int64_t>(ranges_[0].size - ranges_[0].position, size);
  ranges_[0].position += static_cast<int32_t>(viewSize);
  return std::string_view(reinterpret_cast<char*>(ranges_[0].buffer) + position, viewSize);
}

std::streampos VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::tellp() const {
  if (ranges_.empty()) {
    return 0;
  }
  return static_cast<std::streampos>(static_cast<int64_t>(totalBytesRead_) - (ranges_[0].size - ranges_[0].position));
}

void VeloxRssSortShuffleReaderDeserializer::RssSortShuffleReaderInputStream::seekp(std::streampos position) {
  if (ranges_.empty() && position == 0) {
    return;
  }
  VELOX_CHECK(!ranges_.empty(), "Cannot seek an empty RssSortShuffleReaderInputStream");
  const int64_t windowStart = static_cast<int64_t>(totalBytesRead_) - ranges_[0].size;
  const int64_t windowEnd = static_cast<int64_t>(totalBytesRead_);
  const int64_t target = static_cast<int64_t>(position);
  VELOX_CHECK(
      target >= windowStart && target <= windowEnd,
      "RssSortShuffleReaderInputStream::seekp({}) is outside the resident window [{}, {}): bytes before the "
      "window were already consumed from the underlying stream (totalBytesRead={})",
      target,
      windowStart,
      windowEnd,
      totalBytesRead_);
  ranges_[0].position = static_cast<int32_t>(target - windowStart);
}

RowVectorPtr VeloxRssSortShuffleReaderDeserializer::readPage() {
  using facebook::velox::serializer::presto::detail::kCompressedBitMask;
  using facebook::velox::serializer::presto::detail::kHeaderSize;
  using facebook::velox::serializer::presto::detail::PrestoHeader;
  constexpr int32_t kPrestoHeaderSize = kHeaderSize;

  // Fast path: peek the header without consuming; if the whole page fits in
  // the current window, deserialize in-situ from in_ directly.
  if (in_->remainingInWindow() >= kPrestoHeaderSize) {
    std::string_view window(reinterpret_cast<const char*>(in_->data()), in_->remainingInWindow());
    auto peekedHeader = PrestoHeader::read(&window);
    if (peekedHeader.has_value()) {
      const int32_t payloadSize = (peekedHeader->pageCodecMarker & kCompressedBitMask) != 0
          ? peekedHeader->compressedSize
          : peekedHeader->uncompressedSize;
      const int64_t totalSize = kPrestoHeaderSize + static_cast<int64_t>(payloadSize);
      if (totalSize <= in_->remainingInWindow()) {
        RowVectorPtr rowVector;
        VectorStreamGroup::read(
            in_.get(), memoryManager_->getLeafMemoryPool().get(), rowType_, serde_, &rowVector, &serdeOptions_);
        return rowVector;
      }
    }
  }

  // Slow path: the page spans multiple read windows. Reassemble it into a
  // contiguous BufferInputStream so the serde's backward seek never touches
  // window data overwritten by a refill.
  std::array<uint8_t, kHeaderSize> headerStorage;
  in_->readBytes(headerStorage.data(), kHeaderSize);
  std::string_view headerBytes(reinterpret_cast<const char*>(headerStorage.data()), kHeaderSize);
  auto headerOpt = PrestoHeader::read(&headerBytes);
  VELOX_CHECK(headerOpt.has_value(), "Invalid Presto page header");
  const auto& header = *headerOpt;

  const int32_t payloadSize =
      (header.pageCodecMarker & kCompressedBitMask) != 0 ? header.compressedSize : header.uncompressedSize;

  // Payload is still in the window: stitch [copied header, in-situ payload].
  if (payloadSize <= in_->remainingInWindow()) {
    in_->advance(payloadSize);
    BufferInputStream pageStream(std::vector<ByteRange>{
        ByteRange{headerStorage.data(), kPrestoHeaderSize, 0}, ByteRange{in_->data() - payloadSize, payloadSize, 0}});
    RowVectorPtr rowVector;
    VectorStreamGroup::read(
        &pageStream, memoryManager_->getLeafMemoryPool().get(), rowType_, serde_, &rowVector, &serdeOptions_);
    return rowVector;
  }

  // Payload spans windows: copy it into a contiguous buffer.
  auto payloadBuffer = AlignedBuffer::allocate<char>(payloadSize, memoryManager_->getLeafMemoryPool().get());
  in_->readBytes(payloadBuffer->asMutable<uint8_t>(), payloadSize);
  BufferInputStream pageStream(std::vector<ByteRange>{
      ByteRange{headerStorage.data(), kPrestoHeaderSize, 0},
      ByteRange{payloadBuffer->asMutable<uint8_t>(), payloadSize, 0}});
  RowVectorPtr rowVector;
  VectorStreamGroup::read(
      &pageStream, memoryManager_->getLeafMemoryPool().get(), rowType_, serde_, &rowVector, &serdeOptions_);
  return rowVector;
}

VeloxShuffleReader::VeloxShuffleReader(
    const std::shared_ptr<arrow::Schema>& schema,
    VeloxMemoryManager* memoryManager,
    const std::shared_ptr<ShuffleReaderOptions>& options)
    : schema_(schema), memoryManager_(memoryManager), options_(options) {
  codec_ = gluten::createCompressionCodec(options->compressionType, options->codecBackend);
  veloxCompressionType_ = arrowCompressionTypeToVelox(options->compressionType);
  rowType_ = facebook::velox::asRowType(gluten::fromArrowSchema(schema));
  initFromSchema();
}

void VeloxShuffleReader::createDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    const OutputType& outputType) {
  switch (options_->shuffleWriterType) {
    case ShuffleWriterType::kHashShuffle: {
      if (outputType == OutputType::kCudfTable) {
#ifdef GLUTEN_ENABLE_GPU
        VELOX_CHECK(!hasComplexType_);
        if (options_->enableGpuAsyncReader) {
          deserializer_ = std::make_unique<VeloxGpuAsyncHashShuffleReaderDeserializer>(
              streamReader,
              schema_,
              codec_,
              rowType_,
              options_->readerBufferSize,
              options_->gpuAsyncReaderMaxPrefetchBytes,
              memoryManager_,
              deserializeTime_,
              decompressTime_);
        } else {
          deserializer_ = std::make_unique<VeloxGpuHashShuffleReaderDeserializer>(
              streamReader,
              schema_,
              codec_,
              rowType_,
              options_->readerBufferSize,
              memoryManager_,
              deserializeTime_,
              decompressTime_);
        }
#else
        throw GlutenException("GLUTEN_ENABLE_GPU is not set. GPU shuffle reader deserializer is not supported.");
#endif
      } else {
        deserializer_ = std::make_unique<VeloxHashShuffleReaderDeserializer>(
            streamReader,
            schema_,
            codec_,
            rowType_,
            options_->batchSize,
            options_->readerBufferSize,
            memoryManager_,
            isValidityBuffer_,
            hasComplexType_,
            options_->enableHashShuffleReaderStreamMerge,
            deserializeTime_,
            decompressTime_);
      }
    } break;
    case ShuffleWriterType::kSortShuffle:
      deserializer_ = std::make_unique<VeloxSortShuffleReaderDeserializer>(
          streamReader,
          schema_,
          codec_,
          rowType_,
          options_->batchSize,
          options_->readerBufferSize,
          options_->deserializerBufferSize,
          memoryManager_,
          deserializeTime_,
          decompressTime_);
      break;
    case ShuffleWriterType::kRssSortShuffle:
      deserializer_ = std::make_unique<VeloxRssSortShuffleReaderDeserializer>(
          streamReader, memoryManager_, rowType_, options_->batchSize, veloxCompressionType_, deserializeTime_);
      break;
    default:
      VELOX_UNREACHABLE();
  }
}

void VeloxShuffleReader::initFromSchema() {
  GLUTEN_ASSIGN_OR_THROW(auto arrowColumnTypes, toShuffleTypeId(schema_->fields()));
  isValidityBuffer_.reserve(arrowColumnTypes.size());
  for (size_t i = 0; i < arrowColumnTypes.size(); ++i) {
    switch (arrowColumnTypes[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
        isValidityBuffer_.push_back(false);
      } break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
        hasComplexType_ = true;
      } break;
      case arrow::BooleanType::type_id: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(true);
      } break;
      case arrow::NullType::type_id:
        break;
      default: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
      } break;
    }
  }
}

std::shared_ptr<ResultIterator> VeloxShuffleReader::read(
    const std::shared_ptr<StreamReader>& streamReader,
    const OutputType& outputType) {
  // TODO: Support reader priority for async reader.
  createDeserializer(streamReader, outputType);
  return std::make_shared<ResultIterator>(deserializer_->deserializeStreams());
}

int64_t VeloxShuffleReader::getDecompressTime() const {
  return decompressTime_;
}

int64_t VeloxShuffleReader::getDeserializeTime() const {
  return deserializeTime_;
}

void VeloxShuffleReader::stop() {
  if (deserializer_) {
    deserializer_->stop();
  }
}
} // namespace gluten

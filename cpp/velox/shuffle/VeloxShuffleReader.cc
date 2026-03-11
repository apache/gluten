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
#include <velox/common/caching/AsyncDataCache.h>

#include "memory/VeloxColumnarBatch.h"
#include "shuffle/GlutenByteStream.h"
#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/Common.h"
#include "utils/Macros.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/row/CompactRow.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <algorithm>

#ifdef GLUTEN_ENABLE_GPU
#include "VeloxGpuShuffleReader.h"
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
    int64_t readerBufferSize,
    VeloxMemoryManager* memoryManager,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : streamReader_(streamReader),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      readerBufferSize_(readerBufferSize),
      memoryManager_(memoryManager),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime) {}

bool VeloxHashShuffleReaderDeserializer::resolveNextBlockType() {
  GLUTEN_ASSIGN_OR_THROW(auto blockType, readBlockType(in_.get()));
  switch (blockType) {
    case BlockType::kEndOfStream:
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
  if (in_ == nullptr) {
    loadNextStream();

    if (reachedEos_) {
      return nullptr;
    }
  }

  while (!resolveNextBlockType()) {
    loadNextStream();

    if (reachedEos_) {
      return nullptr;
    }
  }

  uint32_t numRows = 0;
  GLUTEN_ASSIGN_OR_THROW(
      auto arrowBuffers,
      BlockPayload::deserialize(
          in_.get(), codec_, memoryManager_->defaultArrowMemoryPool(), numRows, deserializeTime_, decompressTime_));

  return makeColumnarBatch(
      rowType_,
      numRows,
      std::move(arrowBuffers),
      dictionaryFields_,
      dictionaries_,
      memoryManager_->getLeafMemoryPool().get(),
      deserializeTime_);
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
  if (auto in = std::dynamic_pointer_cast<CompressedInputStream>(in_)) {
    decompressTime_ += in->decompressTime();
  }
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

class VeloxRssSortShuffleReaderDeserializer::VeloxInputStream : public facebook::velox::GlutenByteInputStream {
 public:
  VeloxInputStream(std::shared_ptr<arrow::io::InputStream> input, facebook::velox::BufferPtr buffer);

  bool hasNext();

  void next(bool throwIfPastEnd) override;

  size_t remainingSize() const override;

  std::shared_ptr<arrow::io::InputStream> in_;
  const facebook::velox::BufferPtr buffer_;
  uint64_t offset_ = -1;
};

VeloxRssSortShuffleReaderDeserializer::VeloxInputStream::VeloxInputStream(
    std::shared_ptr<arrow::io::InputStream> input,
    facebook::velox::BufferPtr buffer)
    : in_(std::move(input)), buffer_(std::move(buffer)) {
  next(true);
}

bool VeloxRssSortShuffleReaderDeserializer::VeloxInputStream::hasNext() {
  if (offset_ == 0) {
    return false;
  }
  if (ranges()[0].position >= ranges()[0].size) {
    next(true);
    return offset_ != 0;
  }
  return true;
}

void VeloxRssSortShuffleReaderDeserializer::VeloxInputStream::next(bool throwIfPastEnd) {
  const uint32_t readBytes = buffer_->capacity();
  offset_ = in_->Read(readBytes, buffer_->asMutable<char>()).ValueOr(0);
  if (offset_ > 0) {
    int32_t realBytes = offset_;
    VELOX_CHECK_LT(0, realBytes, "Reading past end of file.");
    setRange({buffer_->asMutable<uint8_t>(), realBytes, 0});
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

  RowVectorPtr rowVector;
  VectorStreamGroup::read(
      in_.get(), memoryManager_->getLeafMemoryPool().get(), rowType_, serde_, &rowVector, &serdeOptions_);

  if (rowVector->size() >= batchSize_) {
    return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
  }

  while (rowVector->size() < batchSize_ && in_->hasNext()) {
    RowVectorPtr rowVectorTemp;
    VectorStreamGroup::read(
        in_.get(), memoryManager_->getLeafMemoryPool().get(), rowType_, serde_, &rowVectorTemp, &serdeOptions_);
    rowVector->append(rowVectorTemp.get());
  }

  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

void VeloxRssSortShuffleReaderDeserializer::loadNextStream() {
  if (reachedEos_) {
    return;
  }

  arrowIn_ = streamReader_->readNextStream(memoryManager_->defaultArrowMemoryPool());

  if (arrowIn_ == nullptr) {
    reachedEos_ = true;
    return;
  }

  constexpr uint64_t kMaxReadBufferSize = (1 << 20) - AlignedBuffer::kPaddedSize;
  auto buffer = AlignedBuffer::allocate<char>(kMaxReadBufferSize, memoryManager_->getLeafMemoryPool().get());
  in_ = std::make_unique<VeloxInputStream>(std::move(arrowIn_), std::move(buffer));
}

size_t VeloxRssSortShuffleReaderDeserializer::VeloxInputStream::remainingSize() const {
  return std::numeric_limits<unsigned long>::max();
}

VeloxShuffleReaderDeserializerFactory::VeloxShuffleReaderDeserializerFactory(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    facebook::velox::common::CompressionKind veloxCompressionType,
    const RowTypePtr& rowType,
    int32_t batchSize,
    int64_t readerBufferSize,
    int64_t deserializerBufferSize,
    VeloxMemoryManager* memoryManager,
    ShuffleWriterType shuffleWriterType)
    : schema_(schema),
      codec_(codec),
      veloxCompressionType_(veloxCompressionType),
      rowType_(rowType),
      batchSize_(batchSize),
      readerBufferSize_(readerBufferSize),
      deserializerBufferSize_(deserializerBufferSize),
      memoryManager_(memoryManager),
      shuffleWriterType_(shuffleWriterType) {
  initFromSchema();
}

std::unique_ptr<ColumnarBatchIterator> VeloxShuffleReaderDeserializerFactory::createDeserializer(
    const std::shared_ptr<StreamReader>& streamReader) {
  switch (shuffleWriterType_) {
    case ShuffleWriterType::kGpuHashShuffle:
#ifdef GLUTEN_ENABLE_GPU
      VELOX_CHECK(!hasComplexType_);
      return std::make_unique<VeloxGpuHashShuffleReaderDeserializer>(
          streamReader,
          schema_,
          codec_,
          rowType_,
          readerBufferSize_,
          memoryManager_,
          deserializeTime_,
          decompressTime_);
#endif
    case ShuffleWriterType::kHashShuffle:
      return std::make_unique<VeloxHashShuffleReaderDeserializer>(
          streamReader,
          schema_,
          codec_,
          rowType_,
          readerBufferSize_,
          memoryManager_,
          deserializeTime_,
          decompressTime_);
    case ShuffleWriterType::kSortShuffle:
      return std::make_unique<VeloxSortShuffleReaderDeserializer>(
          streamReader,
          schema_,
          codec_,
          rowType_,
          batchSize_,
          readerBufferSize_,
          deserializerBufferSize_,
          memoryManager_,
          deserializeTime_,
          decompressTime_);
    case ShuffleWriterType::kRssSortShuffle:
      return std::make_unique<VeloxRssSortShuffleReaderDeserializer>(
          streamReader, memoryManager_, rowType_, batchSize_, veloxCompressionType_, deserializeTime_);
  }
  GLUTEN_UNREACHABLE();
}

int64_t VeloxShuffleReaderDeserializerFactory::getDecompressTime() {
  return decompressTime_;
}

int64_t VeloxShuffleReaderDeserializerFactory::getDeserializeTime() {
  return deserializeTime_;
}

void VeloxShuffleReaderDeserializerFactory::initFromSchema() {
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

VeloxShuffleReader::VeloxShuffleReader(std::unique_ptr<VeloxShuffleReaderDeserializerFactory> factory)
    : factory_(std::move(factory)) {}

std::shared_ptr<ResultIterator> VeloxShuffleReader::read(const std::shared_ptr<StreamReader>& streamReader) {
  return std::make_shared<ResultIterator>(factory_->createDeserializer(streamReader));
}

int64_t VeloxShuffleReader::getDecompressTime() const {
  return factory_->getDecompressTime();
}

int64_t VeloxShuffleReader::getDeserializeTime() const {
  return factory_->getDeserializeTime();
}
} // namespace gluten

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
#include "shuffle/Payload.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/bitmap.h>
#include <iostream>
#include <numeric>

#include "shuffle/Options.h"
#include "shuffle/Utils.h"
#include "utils/Exception.h"
#include "utils/Timer.h"
#include "utils/tac/TypeAwareCompressCodec.h"

namespace gluten {
namespace {

static const Payload::Type kCompressedType = gluten::BlockPayload::kCompressed;
static const Payload::Type kUncompressedType = gluten::BlockPayload::kUncompressed;

static constexpr int64_t kZeroLengthBuffer = 0;
static constexpr int64_t kNullBuffer = -1;
static constexpr int64_t kUncompressedBuffer = -2;
static constexpr int64_t kTypeAwareBuffer = -3;

template <typename T>
void write(uint8_t** dst, T data) {
  memcpy(*dst, &data, sizeof(T));
  *dst += sizeof(T);
}

template <typename T>
T* advance(uint8_t** dst) {
  auto ptr = reinterpret_cast<T*>(*dst);
  *dst += sizeof(T);
  return ptr;
}

arrow::Result<uint8_t> readPayloadType(arrow::io::InputStream* is) {
  uint8_t type;
  ARROW_ASSIGN_OR_RAISE(auto bytes, is->Read(sizeof(Payload::Type), &type));
  ARROW_RETURN_IF(bytes == 0, arrow::Status::IOError("Failed to read bytes. Reached EOS."));
  return type;
}

arrow::Result<int64_t> compressBuffer(
    const std::shared_ptr<arrow::Buffer>& buffer,
    uint8_t* output,
    int64_t outputLength,
    arrow::util::Codec* codec) {
  auto outputPtr = &output;
  if (!buffer) {
    write<int64_t>(outputPtr, kNullBuffer);
    return sizeof(int64_t);
  }
  if (buffer->size() == 0) {
    write<int64_t>(outputPtr, kZeroLengthBuffer);
    return sizeof(int64_t);
  }
  static const int64_t kCompressedBufferHeaderLength = 2 * sizeof(int64_t);
  auto* compressedLengthPtr = advance<int64_t>(outputPtr);
  write(outputPtr, static_cast<int64_t>(buffer->size()));
  ARROW_ASSIGN_OR_RAISE(
      auto compressedLength, codec->Compress(buffer->size(), buffer->data(), outputLength, *outputPtr));
  if (compressedLength >= buffer->size()) {
    // Write uncompressed buffer.
    memcpy(*outputPtr, buffer->data(), buffer->size());
    *compressedLengthPtr = kUncompressedBuffer;
    return kCompressedBufferHeaderLength + buffer->size();
  }
  *compressedLengthPtr = static_cast<int64_t>(compressedLength);
  return kCompressedBufferHeaderLength + compressedLength;
}

// Type-aware buffer compression via TypeAwareCompressCodec.
// Same wire format as compressBuffer:
//   kTypeAwareBuffer (int64) | uncompressedLength (int64) | compressedLength (int64) | compressed data
// If compressed size >= uncompressed size, falls back to kUncompressedBuffer (same as standard codec).
arrow::Result<int64_t> compressTypeAwareBuffer(
    const std::shared_ptr<arrow::Buffer>& buffer,
    uint8_t* output,
    int64_t outputLength,
    int8_t typeKind) {
  auto outputPtr = &output;
  if (!buffer) {
    write<int64_t>(outputPtr, kNullBuffer);
    return sizeof(int64_t);
  }
  if (buffer->size() == 0) {
    write<int64_t>(outputPtr, kZeroLengthBuffer);
    return sizeof(int64_t);
  }

  static const int64_t kHeaderLength = 3 * sizeof(int64_t); // marker + uncompressedLen + compressedLen
  if (outputLength < kHeaderLength + buffer->size()) {
    return arrow::Status::Invalid("Output buffer too small for type-aware compression.");
  }
  auto* dataOutput = output + kHeaderLength;
  auto availableOutput = outputLength - kHeaderLength;

  ARROW_ASSIGN_OR_RAISE(
      auto compressedSize,
      TypeAwareCompressCodec::compress(buffer->data(), buffer->size(), dataOutput, availableOutput, typeKind));

  if (compressedSize >= buffer->size()) {
    // Compression didn't help. Fall back to uncompressed, same as compressBuffer.
    write<int64_t>(outputPtr, kUncompressedBuffer);
    write(outputPtr, static_cast<int64_t>(buffer->size()));
    memcpy(*outputPtr, buffer->data(), buffer->size());
    return 2 * sizeof(int64_t) + buffer->size();
  }

  write<int64_t>(outputPtr, kTypeAwareBuffer);
  write(outputPtr, static_cast<int64_t>(buffer->size()));
  write(outputPtr, static_cast<int64_t>(compressedSize));
  // compressed data already written at dataOutput by TypeAwareCompressCodec::compress.
  return kHeaderLength + compressedSize;
}

arrow::Status compressAndFlush(
    const std::shared_ptr<arrow::Buffer>& buffer,
    arrow::io::OutputStream* outputStream,
    arrow::util::Codec* codec,
    arrow::MemoryPool* pool,
    int64_t& compressTime,
    int64_t& writeTime) {
  if (!buffer) {
    ScopedTimer timer(&writeTime);
    RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
    return arrow::Status::OK();
  }
  if (buffer->size() == 0) {
    ScopedTimer timer(&writeTime);
    RETURN_NOT_OK(outputStream->Write(&kZeroLengthBuffer, sizeof(int64_t)));
    return arrow::Status::OK();
  }
  ScopedTimer timer(&compressTime);
  auto maxCompressedLength = codec->MaxCompressedLen(buffer->size(), buffer->data());
  ARROW_ASSIGN_OR_RAISE(
      auto compressed, arrow::AllocateResizableBuffer(sizeof(int64_t) * 2 + maxCompressedLength, pool));
  auto output = compressed->mutable_data();
  ARROW_ASSIGN_OR_RAISE(auto compressedSize, compressBuffer(buffer, output, maxCompressedLength, codec));

  timer.switchTo(&writeTime);
  RETURN_NOT_OK(outputStream->Write(compressed->data(), compressedSize));
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
readUncompressedBuffer(arrow::io::InputStream* inputStream, arrow::MemoryPool* pool, int64_t& deserializedTime) {
  ScopedTimer timer(&deserializedTime);
  int64_t bufferLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &bufferLength));
  if (bufferLength == kNullBuffer) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(bufferLength, pool));
  RETURN_NOT_OK(inputStream->Read(bufferLength, buffer->mutable_data()));
  return buffer;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> readCompressedBuffer(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    int64_t& deserializeTime,
    int64_t& decompressTime) {
  ScopedTimer timer(&deserializeTime);
  int64_t compressedLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &compressedLength));
  if (compressedLength == kNullBuffer) {
    return nullptr;
  }
  if (compressedLength == kZeroLengthBuffer) {
    return zeroLengthNullBuffer();
  }

  int64_t uncompressedLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &uncompressedLength));

  if (compressedLength == kTypeAwareBuffer) {
    // Type-aware compressed buffer. This marker only appears when compression helped.
    // Wire format: compressedLength (int64) already consumed above as kTypeAwareBuffer,
    // then uncompressedLength (already read), then actualCompressedLen, then data.
    int64_t actualCompressedLen;
    RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &actualCompressedLen));
    ARROW_ASSIGN_OR_RAISE(auto compressed, arrow::AllocateResizableBuffer(actualCompressedLen, pool));
    RETURN_NOT_OK(inputStream->Read(actualCompressedLen, compressed->mutable_data()));

    timer.switchTo(&decompressTime);
    ARROW_ASSIGN_OR_RAISE(auto output, arrow::AllocateResizableBuffer(uncompressedLength, pool));
    RETURN_NOT_OK(TypeAwareCompressCodec::decompress(
                      compressed->data(), actualCompressedLen, output->mutable_data(), uncompressedLength)
                      .status());
    return output;
  }

  if (compressedLength == kUncompressedBuffer) {
    ARROW_ASSIGN_OR_RAISE(auto uncompressed, arrow::AllocateResizableBuffer(uncompressedLength, pool));
    RETURN_NOT_OK(inputStream->Read(uncompressedLength, uncompressed->mutable_data()));
    return uncompressed;
  }
  ARROW_ASSIGN_OR_RAISE(auto compressed, arrow::AllocateResizableBuffer(compressedLength, pool));
  RETURN_NOT_OK(inputStream->Read(compressedLength, compressed->mutable_data()));

  timer.switchTo(&decompressTime);
  ARROW_ASSIGN_OR_RAISE(auto output, arrow::AllocateResizableBuffer(uncompressedLength, pool));
  RETURN_NOT_OK(codec->Decompress(compressedLength, compressed->data(), uncompressedLength, output->mutable_data()));
  return output;
}

} // namespace

Payload::Payload(Payload::Type type, uint32_t numRows, const std::vector<bool>* isValidityBuffer)
    : type_(type), numRows_(numRows), isValidityBuffer_(isValidityBuffer) {}

std::string Payload::toString() const {
  static std::string kUncompressedString = "Payload::kUncompressed";
  static std::string kCompressedString = "Payload::kCompressed";
  static std::string kToBeCompressedString = "Payload::kToBeCompressed";

  if (type_ == kUncompressed) {
    return kUncompressedString;
  }
  if (type_ == kCompressed) {
    return kCompressedString;
  }
  return kToBeCompressedString;
}

arrow::Result<std::unique_ptr<BlockPayload>> BlockPayload::fromBuffers(
    Payload::Type payloadType,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    const std::vector<bool>* isValidityBuffer,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    const std::vector<int8_t>* bufferTypes) {
  const uint32_t numBuffers = buffers.size();

  if (payloadType == Payload::Type::kCompressed) {
    Timer compressionTime;
    compressionTime.start();

    // Compute max compressed length, accounting for type-aware compression where applicable.
    auto maxLength = maxCompressedLength(buffers, codec, bufferTypes);

    std::shared_ptr<arrow::Buffer> compressedBuffer;
    ARROW_ASSIGN_OR_RAISE(compressedBuffer, arrow::AllocateResizableBuffer(maxLength, pool));
    auto* output = compressedBuffer->mutable_data();

    int64_t actualLength = 0;
    // Compress buffers one by one.
    for (size_t i = 0; i < buffers.size(); ++i) {
      auto availableLength = maxLength - actualLength;
      auto typeKind = (bufferTypes != nullptr && i < bufferTypes->size()) ? (*bufferTypes)[i] : tac::kUnsupported;

      int64_t compressedSize = 0;
      if (TypeAwareCompressCodec::support(typeKind)) {
        // Use type-aware compression for supported types.
        ARROW_ASSIGN_OR_RAISE(
            compressedSize, compressTypeAwareBuffer(std::move(buffers[i]), output, availableLength, typeKind));
      } else {
        // Use standard codec (LZ4/ZSTD) for unsupported types.
        ARROW_ASSIGN_OR_RAISE(compressedSize, compressBuffer(std::move(buffers[i]), output, availableLength, codec));
      }
      output += compressedSize;
      actualLength += compressedSize;
    }

    ARROW_RETURN_IF(actualLength < 0, arrow::Status::Invalid("Writing compressed buffer out of bound."));

    RETURN_NOT_OK(std::dynamic_pointer_cast<arrow::ResizableBuffer>(compressedBuffer)->Resize(actualLength));

    compressionTime.stop();
    auto payload = std::unique_ptr<BlockPayload>(
        new BlockPayload(Type::kCompressed, numRows, numBuffers, {compressedBuffer}, isValidityBuffer));
    payload->setCompressionTime(compressionTime.realTimeUsed());

    return payload;
  }
  return std::unique_ptr<BlockPayload>(
      new BlockPayload(payloadType, numRows, numBuffers, std::move(buffers), isValidityBuffer));
}

arrow::Status BlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  switch (type_) {
    case Type::kUncompressed: {
      ScopedTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(&kUncompressedType, sizeof(Type)));
      RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
      RETURN_NOT_OK(outputStream->Write(&numBuffers_, sizeof(uint32_t)));
      for (auto& buffer : buffers_) {
        if (!buffer) {
          RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
          continue;
        }
        int64_t bufferSize = buffer->size();
        RETURN_NOT_OK(outputStream->Write(&bufferSize, sizeof(int64_t)));
        if (bufferSize > 0) {
          RETURN_NOT_OK(outputStream->Write(std::move(buffer)));
        }
      }
    } break;
    case Type::kToBeCompressed: {
      ScopedTimer timer(&writeTime_);

      // No type and rows metadata for kToBeCompressed payload.
      RETURN_NOT_OK(outputStream->Write(&numBuffers_, sizeof(uint32_t)));

      for (auto& buffer : buffers_) {
        if (!buffer) {
          RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
          continue;
        }

        int64_t bufferSize = buffer->size();
        RETURN_NOT_OK(outputStream->Write(&bufferSize, sizeof(int64_t)));
        if (bufferSize > 0) {
          RETURN_NOT_OK(outputStream->Write(std::move(buffer)));
        }
      }
    } break;
    case Type::kCompressed: {
      ScopedTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(Type)));
      RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
      RETURN_NOT_OK(outputStream->Write(&numBuffers_, sizeof(uint32_t)));
      RETURN_NOT_OK(outputStream->Write(std::move(buffers_[0])));
    } break;
    case Type::kRaw: {
      ScopedTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(std::move(buffers_[0])));
    } break;
  }
  buffers_.clear();
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> BlockPayload::readBufferAt(uint32_t pos) {
  if (type_ == Type::kCompressed) {
    return arrow::Status::Invalid("Cannot read buffer from compressed BlockPayload.");
  }
  if (type_ == Type::kRaw && pos != 0) {
    return arrow::Status::Invalid("Read buffer pos from raw should only be 0, but got " + std::to_string(pos));
  }
  return std::move(buffers_[pos]);
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> BlockPayload::deserialize(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    uint32_t& numRows,
    int64_t& deserializeTime,
    int64_t& decompressTime) {
  auto timer = std::make_unique<ScopedTimer>(&deserializeTime);
  ARROW_ASSIGN_OR_RAISE(auto type, readPayloadType(inputStream));

  RETURN_NOT_OK(inputStream->Read(sizeof(uint32_t), &numRows));
  uint32_t numBuffers;
  RETURN_NOT_OK(inputStream->Read(sizeof(uint32_t), &numBuffers));
  timer.reset();

  bool isCompressionEnabled = type == Type::kCompressed;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  buffers.reserve(numBuffers);
  for (auto i = 0; i < numBuffers; ++i) {
    buffers.emplace_back();
    if (isCompressionEnabled) {
      ARROW_ASSIGN_OR_RAISE(
          buffers.back(), readCompressedBuffer(inputStream, codec, pool, deserializeTime, decompressTime));
    } else {
      ARROW_ASSIGN_OR_RAISE(buffers.back(), readUncompressedBuffer(inputStream, pool, deserializeTime));
    }
  }
  return buffers;
}

void BlockPayload::setCompressionTime(int64_t compressionTime) {
  compressTime_ = compressionTime;
}

int64_t BlockPayload::rawSize() {
  return getBufferSize(buffers_);
}

int64_t BlockPayload::maxCompressedLength(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::util::Codec* codec,
    const std::vector<int8_t>* bufferTypes) {
  // Compressed buffer layout: | buffer1 compressedLength | buffer1 uncompressedLength | buffer1 | ...
  int64_t metadataLength = sizeof(int64_t) * 2 * buffers.size();
  int64_t totalCompressedLength = 0;
  for (size_t i = 0; i < buffers.size(); ++i) {
    const auto& buffer = buffers[i];
    if (!buffer) {
      continue;
    }
    if (bufferTypes != nullptr && i < bufferTypes->size()) {
      auto typeKind = (*bufferTypes)[i];
      if (TypeAwareCompressCodec::support(typeKind)) {
        // Type-aware compressed buffer has an extra int64 marker to indicate type-aware compression.
        // buffer layout: | kTypeAwareBuffer (int64) | buffer 1 uncompressedLength | buffer 1 compressedLength | buffer
        // 1 | ...
        metadataLength += sizeof(int64_t);
        totalCompressedLength += TypeAwareCompressCodec::maxCompressedLen(buffer->size(), typeKind);
        continue;
      }
    }
    // Standard codec: compressed data.
    totalCompressedLength += codec->MaxCompressedLen(buffer->size(), buffer->data());
  }
  return metadataLength + totalCompressedLength;
}

arrow::Result<std::unique_ptr<InMemoryPayload>> InMemoryPayload::merge(
    std::unique_ptr<InMemoryPayload> source,
    std::unique_ptr<InMemoryPayload> append,
    arrow::MemoryPool* pool) {
  GLUTEN_DCHECK(source->mergeable() && append->mergeable(), "Cannot merge payloads.");
  auto mergedRows = source->numRows() + append->numRows();
  auto isValidityBuffer = source->isValidityBuffer();

  auto numBuffers = append->numBuffers();
  ARROW_RETURN_IF(
      numBuffers != source->numBuffers(), arrow::Status::Invalid("Number of merging buffers doesn't match."));
  std::vector<std::shared_ptr<arrow::Buffer>> merged;
  merged.resize(numBuffers);
  for (size_t i = 0; i < numBuffers; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto sourceBuffer, source->readBufferAt(i));
    ARROW_ASSIGN_OR_RAISE(auto appendBuffer, append->readBufferAt(i));
    if (isValidityBuffer->at(i)) {
      if (!sourceBuffer) {
        if (!appendBuffer) {
          merged[i] = nullptr;
        } else {
          ARROW_ASSIGN_OR_RAISE(
              auto buffer, arrow::AllocateResizableBuffer(arrow::bit_util::BytesForBits(mergedRows), pool));
          // Source is null, fill all true.
          arrow::bit_util::SetBitsTo(buffer->mutable_data(), 0, source->numRows(), true);
          // Write append bits.
          arrow::internal::CopyBitmap(
              appendBuffer->data(), 0, append->numRows(), buffer->mutable_data(), source->numRows());
          merged[i] = std::move(buffer);
        }
      } else {
        // Because sourceBuffer can be resized, need to save buffer size in advance.
        auto sourceBufferSize = sourceBuffer->size();
        auto resizable = std::dynamic_pointer_cast<arrow::ResizableBuffer>(sourceBuffer);
        auto mergedBytes = arrow::bit_util::BytesForBits(mergedRows);
        if (resizable) {
          // If source is resizable, resize and reuse source.
          RETURN_NOT_OK(resizable->Resize(mergedBytes));
        } else {
          // Otherwise copy source.
          ARROW_ASSIGN_OR_RAISE(resizable, arrow::AllocateResizableBuffer(mergedBytes, pool));
          memcpy(resizable->mutable_data(), sourceBuffer->data(), sourceBufferSize);
        }
        if (!appendBuffer) {
          arrow::bit_util::SetBitsTo(resizable->mutable_data(), source->numRows(), append->numRows(), true);
        } else {
          arrow::internal::CopyBitmap(
              appendBuffer->data(), 0, append->numRows(), resizable->mutable_data(), source->numRows());
        }
        merged[i] = std::move(resizable);
      }
    } else {
      if (appendBuffer->size() == 0) {
        merged[i] = std::move(sourceBuffer);
      } else {
        // Because sourceBuffer can be resized, need to save buffer size in advance.
        auto sourceBufferSize = sourceBuffer->size();
        auto mergedSize = sourceBufferSize + appendBuffer->size();
        auto resizable = std::dynamic_pointer_cast<arrow::ResizableBuffer>(sourceBuffer);
        if (resizable) {
          // If source is resizable, resize and reuse source.
          RETURN_NOT_OK(resizable->Resize(mergedSize));
        } else {
          // Otherwise copy source.
          ARROW_ASSIGN_OR_RAISE(resizable, arrow::AllocateResizableBuffer(mergedSize, pool));
          memcpy(resizable->mutable_data(), sourceBuffer->data(), sourceBufferSize);
        }
        // Copy append.
        memcpy(resizable->mutable_data() + sourceBufferSize, appendBuffer->data(), appendBuffer->size());
        merged[i] = std::move(resizable);
      }
    }
  }
  return std::make_unique<InMemoryPayload>(
      mergedRows, isValidityBuffer, source->schema(), std::move(merged), false, source->bufferTypes_);
}

arrow::Result<std::unique_ptr<BlockPayload>>
InMemoryPayload::toBlockPayload(Payload::Type payloadType, arrow::MemoryPool* pool, arrow::util::Codec* codec) {
  return BlockPayload::fromBuffers(
      payloadType, numRows_, std::move(buffers_), isValidityBuffer_, pool, codec, bufferTypes_);
}

arrow::Status InMemoryPayload::serialize(arrow::io::OutputStream* outputStream) {
  for (auto& buffer : buffers_) {
    RETURN_NOT_OK(outputStream->Write(buffer->data(), buffer->size()));
    buffer = nullptr;
  }
  buffers_.clear();
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> InMemoryPayload::readBufferAt(uint32_t index) {
  GLUTEN_CHECK(
      index < buffers_.size(),
      "buffer index out of range: index = " + std::to_string(index) +
          " vs buffer size = " + std::to_string(buffers_.size()));
  return std::move(buffers_[index]);
}

arrow::Status InMemoryPayload::copyBuffers(arrow::MemoryPool* pool) {
  for (auto& buffer : buffers_) {
    if (!buffer) {
      continue;
    }
    if (buffer->size() == 0) {
      buffer = zeroLengthNullBuffer();
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(auto copy, arrow::AllocateResizableBuffer(buffer->size(), pool));
    memcpy(copy->mutable_data(), buffer->data(), buffer->size());
    buffer = std::move(copy);
  }
  return arrow::Status::OK();
}

int64_t InMemoryPayload::rawSize() {
  return getBufferSize(buffers_);
}

uint32_t InMemoryPayload::numBuffers() const {
  return buffers_.size();
}

int64_t InMemoryPayload::rawCapacity() const {
  return getBufferCapacity(buffers_);
}

bool InMemoryPayload::mergeable() const {
  return !hasComplexType_;
}

std::shared_ptr<arrow::Schema> InMemoryPayload::schema() const {
  return schema_;
}

arrow::Status InMemoryPayload::createDictionaries(const std::shared_ptr<ShuffleDictionaryWriter>& dictionaryWriter) {
  ARROW_ASSIGN_OR_RAISE(buffers_, dictionaryWriter->updateAndGet(schema_, numRows_, buffers_));
  return arrow::Status::OK();
}

UncompressedDiskBlockPayload::UncompressedDiskBlockPayload(
    Type type,
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    arrow::io::InputStream*& inputStream,
    uint64_t rawSize,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec)
    : Payload(type, numRows, isValidityBuffer),
      inputStream_(inputStream),
      rawSize_(rawSize),
      pool_(pool),
      codec_(codec) {}

arrow::Status UncompressedDiskBlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  ARROW_RETURN_IF(
      inputStream_ == nullptr, arrow::Status::Invalid("inputStream_ is uninitialized before calling serialize()."));

  if (type_ == Payload::kUncompressed) {
    ARROW_ASSIGN_OR_RAISE(auto block, inputStream_->Read(rawSize_));
    RETURN_NOT_OK(outputStream->Write(block));
    return arrow::Status::OK();
  }

  GLUTEN_DCHECK(
      type_ == Payload::kToBeCompressed,
      "Invalid payload type: " + std::to_string(type_) +
          ", should be either Payload::kUncompressed or Payload::kToBeCompressed");

  GLUTEN_CHECK(codec_ != nullptr, "Codec is null when serializing Payload::kToBeCompressed.");

  uint8_t blockType;
  ARROW_ASSIGN_OR_RAISE(auto bytes, inputStream_->Read(sizeof(blockType), &blockType));
  ARROW_RETURN_IF(bytes == 0, arrow::Status::Invalid("Cannot serialize payload. Reached EOS."));

  RETURN_NOT_OK(outputStream->Write(&blockType, sizeof(blockType)));

  RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(kCompressedType)));
  RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));

  uint32_t numBuffers = 0;
  ARROW_ASSIGN_OR_RAISE(bytes, inputStream_->Read(sizeof(uint32_t), &numBuffers));
  ARROW_RETURN_IF(bytes == 0 || numBuffers == 0, arrow::Status::Invalid("Cannot serialize payload with 0 buffers."));

  RETURN_NOT_OK(outputStream->Write(&numBuffers, sizeof(uint32_t)));

  ARROW_ASSIGN_OR_RAISE(auto start, inputStream_->Tell());

  auto pos = start;
  auto rawBufferSize = rawSize_ - sizeof(blockType) - sizeof(numBuffers);

  while (pos - start < rawBufferSize) {
    ARROW_ASSIGN_OR_RAISE(auto uncompressed, readUncompressedBuffer());
    ARROW_ASSIGN_OR_RAISE(pos, inputStream_->Tell());
    RETURN_NOT_OK(compressAndFlush(std::move(uncompressed), outputStream, codec_, pool_, compressTime_, writeTime_));
  }

  GLUTEN_CHECK(pos - start == rawBufferSize, "Not all data is read from input stream.");

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> UncompressedDiskBlockPayload::readUncompressedBuffer() {
  ScopedTimer timer(&writeTime_);

  int64_t bufferLength;
  RETURN_NOT_OK(inputStream_->Read(sizeof(int64_t), &bufferLength));
  if (bufferLength == kNullBuffer) {
    return nullptr;
  }
  if (bufferLength == 0) {
    return zeroLengthNullBuffer();
  }
  ARROW_ASSIGN_OR_RAISE(auto buffer, inputStream_->Read(bufferLength));
  return buffer;
}

int64_t UncompressedDiskBlockPayload::rawSize() {
  return rawSize_;
}

CompressedDiskBlockPayload::CompressedDiskBlockPayload(
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    arrow::io::InputStream*& inputStream,
    int64_t rawSize,
    arrow::MemoryPool* /* pool */)
    : Payload(Type::kCompressed, numRows, isValidityBuffer), inputStream_(inputStream), rawSize_(rawSize) {}

arrow::Status CompressedDiskBlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  ARROW_RETURN_IF(
      inputStream_ == nullptr, arrow::Status::Invalid("inputStream_ is uninitialized before calling serialize()."));
  ScopedTimer timer(&writeTime_);
  ARROW_ASSIGN_OR_RAISE(auto block, inputStream_->Read(rawSize_));
  RETURN_NOT_OK(outputStream->Write(block));
  return arrow::Status::OK();
}

int64_t CompressedDiskBlockPayload::rawSize() {
  return rawSize_;
}
} // namespace gluten

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

#pragma once

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>

#include "shuffle/Dictionary.h"
#include "shuffle/Options.h"
#include "shuffle/Utils.h"

namespace gluten {

class Payload {
 public:
  enum Type : uint8_t { kCompressed = 1, kUncompressed = 2, kToBeCompressed = 3, kRaw = 4 };

  Payload(Type type, uint32_t numRows, const std::vector<bool>* isValidityBuffer);

  virtual ~Payload() = default;

  virtual arrow::Status serialize(arrow::io::OutputStream* outputStream) = 0;

  virtual int64_t rawSize() = 0;

  int64_t getCompressTime() const {
    return compressTime_;
  }

  int64_t getWriteTime() const {
    return writeTime_;
  }

  Type type() const {
    return type_;
  }

  uint32_t numRows() const {
    return numRows_;
  }

  const std::vector<bool>* isValidityBuffer() const {
    return isValidityBuffer_;
  }

  std::string toString() const;

 protected:
  Type type_;
  uint32_t numRows_;
  const std::vector<bool>* isValidityBuffer_;
  int64_t compressTime_{0};
  int64_t writeTime_{0};
};

// A block represents data to be cached in-memory.
// Can be compressed or uncompressed.
class BlockPayload final : public Payload {
 public:
  static arrow::Result<std::unique_ptr<BlockPayload>> fromBuffers(
      Payload::Type payloadType,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec,
      const std::vector<int8_t>* bufferTypes = nullptr);

  static arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> deserialize(
      arrow::io::InputStream* inputStream,
      const std::shared_ptr<arrow::util::Codec>& codec,
      arrow::MemoryPool* pool,
      uint32_t& numRows,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  static int64_t maxCompressedLength(
      const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
      arrow::util::Codec* codec,
      const std::vector<int8_t>* bufferTypes = nullptr);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(uint32_t pos);

  int64_t rawSize() override;

 private:
  BlockPayload(
      Type type,
      uint32_t numRows,
      uint32_t numBuffers,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer)
      : Payload(type, numRows, isValidityBuffer), numBuffers_(numBuffers), buffers_(std::move(buffers)) {}

  void setCompressionTime(int64_t compressionTime);

  uint32_t numBuffers_;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers_;
};

class InMemoryPayload final : public Payload {
 public:
  InMemoryPayload(
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      const std::shared_ptr<arrow::Schema>& schema,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      bool hasComplexType = false,
      const std::vector<int8_t>* bufferTypes = nullptr)
      : Payload(Type::kUncompressed, numRows, isValidityBuffer),
        schema_(schema),
        buffers_(std::move(buffers)),
        hasComplexType_(hasComplexType),
        bufferTypes_(bufferTypes) {}

  static arrow::Result<std::unique_ptr<InMemoryPayload>>
  merge(std::unique_ptr<InMemoryPayload> source, std::unique_ptr<InMemoryPayload> append, arrow::MemoryPool* pool);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(uint32_t index);

  arrow::Result<std::unique_ptr<BlockPayload>>
  toBlockPayload(Payload::Type payloadType, arrow::MemoryPool* pool, arrow::util::Codec* codec);

  arrow::Status copyBuffers(arrow::MemoryPool* pool);

  int64_t rawSize() override;

  uint32_t numBuffers() const;

  int64_t rawCapacity() const;

  bool mergeable() const;

  std::shared_ptr<arrow::Schema> schema() const;

  arrow::Status createDictionaries(const std::shared_ptr<ShuffleDictionaryWriter>& dictionaryWriter);

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers_;
  bool hasComplexType_;
  const std::vector<int8_t>* bufferTypes_;
};

class UncompressedDiskBlockPayload final : public Payload {
 public:
  UncompressedDiskBlockPayload(
      Type type,
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      arrow::io::InputStream*& inputStream,
      uint64_t rawSize,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  int64_t rawSize() override;

 private:
  arrow::io::InputStream*& inputStream_;
  int64_t rawSize_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readUncompressedBuffer();
};

class CompressedDiskBlockPayload final : public Payload {
 public:
  CompressedDiskBlockPayload(
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      arrow::io::InputStream*& inputStream,
      int64_t rawSize,
      arrow::MemoryPool* pool);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  int64_t rawSize() override;

 private:
  arrow::io::InputStream*& inputStream_;
  int64_t rawSize_;
};
} // namespace gluten

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

#include "shuffle/Payload.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/VeloxSortShuffleWriter.h"

#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class VeloxHashShuffleReaderDeserializer final : public ColumnarBatchIterator {
 public:
  VeloxHashShuffleReaderDeserializer(
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
      int64_t& decompressTime);

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  bool shouldSkipMerge() const;

  bool resolveNextBlockType();

  void loadNextStream();

  std::shared_ptr<StreamReader> streamReader_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  uint32_t batchSize_;
  int64_t readerBufferSize_;
  VeloxMemoryManager* memoryManager_;
  std::vector<bool> isValidityBuffer_;
  bool hasComplexType_;
  bool enableStreamMerge_;

  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  std::shared_ptr<arrow::io::InputStream> in_{nullptr};

  std::unique_ptr<InMemoryPayload> merged_{nullptr};
  bool reachedEos_{false};
  bool blockTypeResolved_{false};

  std::vector<int32_t> dictionaryFields_{};
  std::vector<facebook::velox::VectorPtr> dictionaries_{};
};

class VeloxSortShuffleReaderDeserializer final : public ColumnarBatchIterator {
 public:
  using RowSizeType = VeloxSortShuffleWriter::RowSizeType;

  VeloxSortShuffleReaderDeserializer(
      const std::shared_ptr<StreamReader>& streamReader,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      int64_t readerBufferSize,
      int64_t deserializerBufferSize,
      VeloxMemoryManager* memoryManager,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  ~VeloxSortShuffleReaderDeserializer() override;

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  std::shared_ptr<ColumnarBatch> deserializeToBatch();

  void readNextRow();

  void reallocateRowBuffer();

  void loadNextStream();

  std::shared_ptr<StreamReader> streamReader_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;

  uint32_t batchSize_;
  int64_t readerBufferSize_;
  int64_t deserializerBufferSize_;
  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  VeloxMemoryManager* memoryManager_;

  facebook::velox::BufferPtr rowBuffer_{nullptr};
  char* rowBufferPtr_{nullptr};
  uint32_t bytesRead_{0};
  uint32_t lastRowSize_{0};
  std::vector<std::string_view> data_;

  std::shared_ptr<arrow::io::InputStream> in_{nullptr};

  uint32_t cachedRows_{0};
  bool reachedEos_{false};
};

class VeloxRssSortShuffleReaderDeserializer : public ColumnarBatchIterator {
 public:
  VeloxRssSortShuffleReaderDeserializer(
      const std::shared_ptr<StreamReader>& streamReader,
      VeloxMemoryManager* memoryManager,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      facebook::velox::common::CompressionKind veloxCompressionType,
      int64_t& deserializeTime);

  std::shared_ptr<ColumnarBatch> next();

 private:
  class VeloxInputStream;

  void loadNextStream();

  std::shared_ptr<StreamReader> streamReader_;
  VeloxMemoryManager* memoryManager_;
  facebook::velox::RowTypePtr rowType_;
  std::vector<facebook::velox::RowVectorPtr> batches_;
  int32_t batchSize_;
  facebook::velox::common::CompressionKind veloxCompressionType_;
  facebook::velox::VectorSerde* const serde_;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions serdeOptions_;
  int64_t& deserializeTime_;
  std::shared_ptr<VeloxInputStream> in_{nullptr};
  std::shared_ptr<arrow::io::InputStream> arrowIn_{nullptr};

  bool reachedEos_{false};
};

class VeloxShuffleReaderDeserializerFactory {
 public:
  VeloxShuffleReaderDeserializerFactory(
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      facebook::velox::common::CompressionKind veloxCompressionType,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      int64_t readerBufferSize,
      int64_t deserializerBufferSize,
      VeloxMemoryManager* memoryManager,
      ShuffleWriterType shuffleWriterType,
      bool enableHashShuffleReaderStreamMerge = false);

  std::unique_ptr<ColumnarBatchIterator> createDeserializer(const std::shared_ptr<StreamReader>& streamReader);

  int64_t getDecompressTime();

  int64_t getDeserializeTime();

 private:
  void initFromSchema();

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::common::CompressionKind veloxCompressionType_;
  facebook::velox::RowTypePtr rowType_;
  int32_t batchSize_;
  int64_t readerBufferSize_;
  int64_t deserializerBufferSize_;
  VeloxMemoryManager* memoryManager_;

  std::vector<bool> isValidityBuffer_;
  bool hasComplexType_{false};

  ShuffleWriterType shuffleWriterType_;
  bool enableHashShuffleReaderStreamMerge_;

  int64_t deserializeTime_{0};
  int64_t decompressTime_{0};
};

class VeloxShuffleReader final : public ShuffleReader {
 public:
  VeloxShuffleReader(std::unique_ptr<VeloxShuffleReaderDeserializerFactory> factory);

  std::shared_ptr<ResultIterator> read(const std::shared_ptr<StreamReader>& streamReader) override;

  int64_t getDecompressTime() const override;

  int64_t getDeserializeTime() const override;

 private:
  std::unique_ptr<VeloxShuffleReaderDeserializerFactory> factory_;
};
} // namespace gluten

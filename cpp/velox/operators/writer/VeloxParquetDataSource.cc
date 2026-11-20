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

#include "VeloxParquetDataSource.h"

#include <arrow/buffer.h>
#include <arrow/util/key_value_metadata.h>
#include <folly/io/IOBufQueue.h>
#include <folly/lang/Bits.h>
#include <algorithm>
#include <cstring>
#include <optional>
#include <string>

#include "arrow/c/bridge.h"
#include "compute/VeloxRuntime.h"

#include "utils/Exception.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/VeloxWriterUtils.h"
#include "velox/dwio/parquet/thrift/ParquetThrift.h"

using namespace facebook;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common;
using namespace facebook::velox::filesystems;

namespace gluten {

namespace {
constexpr char kSparkVersion[] = "org.apache.spark.version";
constexpr std::string_view kParquetMagic = "PAR1";
} // namespace

// The pinned writer has no file key/value metadata option. Forward data pages unchanged, then
// add Spark's calendar marker to the footer before it reaches the sink (including object stores).
class SparkCalendarFileSink final : public FileSink {
 public:
  SparkCalendarFileSink(std::unique_ptr<FileSink> sink, velox::memory::MemoryPool* pool, std::string version)
      : FileSink(sink->name(), {.pool = pool}), sink_(std::move(sink)), version_(std::move(version)) {}

  void startFooter() {
    tail_.emplace(*pool_);
  }

  void write(std::vector<DataBuffer<char>>& buffers) override {
    if (!tail_) {
      sink_->write(buffers);
    } else {
      for (const auto& buffer : buffers) {
        tail_->extendAppend(tail_->size(), buffer.data(), buffer.size());
      }
      buffers.clear();
    }
    size_ = sink_->size() + (tail_ ? tail_->size() : 0);
  }

  bool isBuffered() const override {
    return sink_->isBuffered();
  }

 private:
  void doClose() override {
    if (tail_ && tail_->size() != 0) {
      auto& tail = *tail_;
      constexpr auto trailerSize = sizeof(uint32_t) + kParquetMagic.size();
      VELOX_CHECK_GE(tail.size(), trailerSize);
      const auto lengthOffset = tail.size() - trailerSize;
      VELOX_CHECK_EQ(
          std::string_view(tail.data() + lengthOffset + sizeof(uint32_t), kParquetMagic.size()), kParquetMagic);
      auto length = folly::Endian::little(folly::loadUnaligned<uint32_t>(tail.data() + lengthOffset));
      VELOX_CHECK_LE(length, lengthOffset);
      // Page indexes, if present, precede the Thrift footer. Their offsets must not move.
      const auto footerOffset = lengthOffset - length;
      velox::parquet::thrift::FileMetaData metadata;
      VELOX_CHECK_EQ(velox::parquet::thrift::deserialize(&metadata, {tail.data() + footerOffset, length}), length);
      auto& entries = metadata.key_value_metadata().ensure();
      auto it = std::find_if(entries.begin(), entries.end(), [](const auto& kv) { return *kv.key() == kSparkVersion; });
      auto& entry = it == entries.end() ? entries.emplace_back() : *it;
      entry.key() = kSparkVersion;
      entry.value() = version_;
      folly::IOBufQueue encoded;
      const auto footerSize = velox::parquet::thrift::serialize(metadata, &encoded);
      auto bytes = encoded.move();
      const auto footer = bytes->coalesce();
      VELOX_CHECK_EQ(footerSize, footer.size());
      length = folly::Endian::little(footerSize);
      tail.append(footerOffset, reinterpret_cast<const char*>(footer.data()), footer.size());
      tail.append(tail.size(), reinterpret_cast<const char*>(&length), sizeof(length));
      tail.append(tail.size(), kParquetMagic.data(), kParquetMagic.size());
      sink_->write(std::move(tail));
    }
    sink_->close();
    size_ = sink_->size();
  }

  std::unique_ptr<FileSink> sink_;
  const std::string version_;
  std::optional<DataBuffer<char>> tail_;
};

void VeloxParquetDataSource::initSink(const std::unordered_map<std::string, std::string>& /* sparkConfs */) {
  if (strncmp(filePath_.c_str(), "file:", 5) == 0) {
    sink_ = dwio::common::FileSink::create(filePath_, {.pool = sinkPool_.get()});
  } else {
    throw std::runtime_error("The file path is not local when writing data with parquet format in velox runtime!");
  }
}

void VeloxParquetDataSource::init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  initSink(sparkConfs);
  auto schema = gluten::makeParquetWriteSchema(schema_);
  const auto writeOption = gluten::makeParquetWriteOption(sparkConfs);
  if (schema_->metadata() && schema_->metadata()->Contains(kSparkVersion)) {
    GLUTEN_ASSIGN_OR_THROW(auto version, schema_->metadata()->Get(kSparkVersion));
    auto sink = std::make_unique<SparkCalendarFileSink>(std::move(sink_), sinkPool_.get(), std::move(version));
    calendarSink_ = sink.get();
    sink_ = std::move(sink);
  }
  parquetWriter_ = std::make_unique<velox::parquet::Writer>(std::move(sink_), *writeOption, pool_, asRowType(schema));
}

void VeloxParquetDataSource::inspectSchema(struct ArrowSchema* out) {
  velox::dwio::common::ReaderOptions readerOptions(pool_.get());
  auto format = velox::dwio::common::FileFormat::PARQUET;
  readerOptions.setFileFormat(format);

  // Creates a file system: local, hdfs or s3.
  auto fs = velox::filesystems::getFileSystem(filePath_, nullptr);
  std::shared_ptr<velox::ReadFile> readFile{fs->openFileForRead(filePath_)};

  std::unique_ptr<velox::dwio::common::Reader> reader =
      velox::dwio::common::getReaderFactory(readerOptions.fileFormat())
          ->createReader(
              std::make_unique<velox::dwio::common::BufferedInput>(
                  std::make_shared<velox::dwio::common::ReadFileInputStream>(readFile), *pool_.get()),
              readerOptions);
  toArrowSchema(reader->rowType(), pool_.get(), out);
}

void VeloxParquetDataSource::close() {
  if (parquetWriter_) {
    if (calendarSink_) {
      parquetWriter_->flush();
      calendarSink_->startFooter();
    }
    parquetWriter_->close();
    parquetWriter_.reset();
    calendarSink_ = nullptr;
  }
}

void VeloxParquetDataSource::write(const std::shared_ptr<ColumnarBatch>& cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  VELOX_DCHECK(veloxBatch != nullptr, "Write batch should be VeloxColumnarBatch");
  parquetWriter_->write(prepareParquetWriteInput(veloxBatch->getFlattenedRowVector()));
}

} // namespace gluten

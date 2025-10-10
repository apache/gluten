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

#include "BoltParquetDataSource.h"

#include <arrow/buffer.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <cstring>
#include <string>

#include "arrow/c/bridge.h"
#include "compute/BoltRuntime.h"

#include "utils/BoltArrowUtils.h"
#include "utils/BoltWriterUtils.h"
#include "config/BoltConfig.h"

using namespace bytedance;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::filesystems;

namespace gluten {

namespace {

const int32_t kGzipWindowBits4k = 12;

// Compression-related helper functions
CompressionKind parseCompressionCodec(const std::string& codecStr) {
  if (boost::iequals(codecStr, "snappy"))
    return CompressionKind::CompressionKind_SNAPPY;
  if (boost::iequals(codecStr, "gzip"))
    return CompressionKind::CompressionKind_GZIP;
  if (boost::iequals(codecStr, "lzo"))
    return CompressionKind::CompressionKind_LZO;
  if (boost::iequals(codecStr, "lz4"))
    return CompressionKind::CompressionKind_LZ4;
  if (boost::iequals(codecStr, "zstd"))
    return CompressionKind::CompressionKind_ZSTD;
  if (boost::iequals(codecStr, "uncompressed") || boost::iequals(codecStr, "none")) {
    return CompressionKind::CompressionKind_NONE;
  }
  throw GlutenException("Unsupported compression codec: " + codecStr);
}

void configureMultithreading(
    bolt::parquet::WriterOptions& options,
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  auto writerThreadingIt = sparkConfs.find(kParquetWriterMultithreadingEnabled);
  if (writerThreadingIt == sparkConfs.end()) {
    return;
  }

  if (!boost::iequals(writerThreadingIt->second, "true")) {
    return;
  }

  auto numOfCores = std::thread::hardware_concurrency();
  if (auto executorCoresIt = sparkConfs.find("spark.executor.cores"); executorCoresIt != sparkConfs.end()) {
    numOfCores = std::stoi(executorCoresIt->second);
  }

  if (numOfCores > 1) {
    options.threadPoolSize = numOfCores;
  }
}

// Writer options configuration
bolt::parquet::WriterOptions createWriterOptions(
    const std::unordered_map<std::string, std::string>& sparkConfs,
    int64_t maxRowGroupBytes,
    std::vector<int32_t> expectedRowsInEachBlock,
    bool enableRowGroupAlignedWrite,
    double parquetWriterBufferGrowRatio,
    double parquetWriterBufferReserveRatio) {
  bolt::parquet::WriterOptions options;
  // Set compression
  options.compression = CompressionKind::CompressionKind_SNAPPY; // default
  if (auto it = sparkConfs.find(kParquetCompressionCodec); it != sparkConfs.end()) {
    options.compression = parseCompressionCodec(it->second);
    // Special handling for GZIP window size
    if (options.compression == CompressionKind::CompressionKind_GZIP) {
      if (auto windowIt = sparkConfs.find(kParquetGzipWindowSize);
          windowIt != sparkConfs.end() && windowIt->second == kGzipWindowSize4k) {
        auto codecOptions = std::make_shared<bytedance::bolt::parquet::arrow::util::GZipCodecOptions>();
        codecOptions->window_bits = kGzipWindowBits4k;
        options.codecOptions = std::move(codecOptions);
      }
    }
  }
  options.expectedRowsInEachBlock = expectedRowsInEachBlock;
  options.enableRowGroupAlignedWrite = enableRowGroupAlignedWrite;
  options.bufferGrowRatio = parquetWriterBufferGrowRatio;
  options.bufferReserveRatio = parquetWriterBufferReserveRatio;
  // Configure timestamp handling
  options.writeInt96AsTimestamp = true;
  options.parquetWriteTimestampUnit = TimestampUnit::kMicro;
  options.parquetWriteTimestampTimeZone = "UTC";
  // Configure block size handling
  options.enableFlushBasedOnBlockSize = true;
  options.parquet_block_size = maxRowGroupBytes;
  // Configure Parquet version
  if (auto parquetVersion = sparkConfs.find(kNativeWriterParquetVersion); parquetVersion != sparkConfs.end()) {
    using bytedance::bolt::parquet::arrow::ParquetVersion; // This shows the correct namespace
    if (boost::iequals(parquetVersion->second, "V2")) {
      options.parquetVersion = ParquetVersion::PARQUET_2_6;
    } else if (boost::iequals(parquetVersion->second, "V1")) {
      options.parquetVersion = ParquetVersion::PARQUET_1_0;
    }
  }
  // Configure decimal storage format
  auto legacyFormatIt = sparkConfs.find(kParquetWriteLegacyFormat);
  options.storeDecimalAsInteger =
      !(legacyFormatIt != sparkConfs.end() && boost::iequals(legacyFormatIt->second, "true"));
  // split batch bytes
  if (auto splitBatchBytes = sparkConfs.find(kNativeWriterParquetSplitBatchBytes);
      splitBatchBytes != sparkConfs.end()) {
    options.writeBatchBytes = std::stoull(splitBatchBytes->second);
  }
  // split min batch size
  if (auto splitMinBatchSize = sparkConfs.find(kNativeWriterParquetSplitMinBatchSize);
      splitMinBatchSize != sparkConfs.end()) {
    options.minBatchSize = std::stoi(splitMinBatchSize->second);
  }
  // Set parquet writer thread count.
  configureMultithreading(options, sparkConfs);
  return options;
}

} // namespace

void BoltParquetDataSource::initSink(const std::unordered_map<std::string, std::string>& /* sparkConfs */) {
  if (strncmp(filePath_.c_str(), "file:", 5) == 0) {
    sink_ = dwio::common::FileSink::create(filePath_, {.pool = pool_.get()});
  } else {
    throw std::runtime_error("The file path is not local when writing data with parquet format in bolt runtime!");
  }
}

void BoltParquetDataSource::init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  initSink(sparkConfs);
  auto schema = gluten::fromArrowSchema(schema_);
  const auto writeOption = gluten::makeParquetWriteOption(sparkConfs);
  parquetWriter_ =
      std::make_unique<bolt::parquet::Writer>(std::move(sink_), *writeOption, /*pool_,*/ asRowType(schema));
}

void BoltParquetDataSource::inspectSchema(struct ArrowSchema* out) {
  bolt::dwio::common::ReaderOptions readerOptions(pool_.get());
  auto format = bolt::dwio::common::FileFormat::PARQUET;
  readerOptions.setFileFormat(format);

  // Creates a file system: local, hdfs or s3.
  auto fs = bolt::filesystems::getFileSystem(filePath_, nullptr);
  std::shared_ptr<bolt::ReadFile> readFile{fs->openFileForRead(filePath_)};

  std::unique_ptr<bolt::dwio::common::Reader> reader =
      bolt::dwio::common::getReaderFactory(readerOptions.getFileFormat())
          ->createReader(
              std::make_unique<bolt::dwio::common::BufferedInput>(
                  std::make_shared<bolt::dwio::common::ReadFileInputStream>(readFile), *pool_.get()),
              readerOptions);
  toArrowSchema(reader->rowType(), pool_.get(), out);
}

void BoltParquetDataSource::close() {
  if (parquetWriter_) {
    parquetWriter_->close();
  }
}

void BoltParquetDataSource::write(const std::shared_ptr<ColumnarBatch>& cb) {
  auto boltBatch = std::dynamic_pointer_cast<BoltColumnarBatch>(cb);
  BOLT_DCHECK(boltBatch != nullptr, "Write batch should be BoltColumnarBatch");

  auto rowVector = boltBatch->getFlattenedRowVector();
  if (rowVector->childrenSize() > schema_->num_fields()) {
    GLUTEN_DCHECK(
        rowVector->childrenSize() - schema_->num_fields() == 3, "There should be 3 extra schema in DR update");
    auto& rowType = rowVector->type()->asRow();
    auto names = rowType.names();
    auto types = rowType.children();
    auto children = rowVector->children();
    names.erase(names.end() - 3, names.end());
    types.erase(types.end() - 3, types.end());
    children.erase(children.end() - 3, children.end());
    rowVector = std::make_shared<bolt::RowVector>(
        rowVector->pool(),
        ROW(std::move(names), std::move(types)),
        rowVector->nulls(),
        rowVector->size(),
        std::move(children));
  }

  parquetWriter_->write(rowVector);
}

void BoltParquetDataSource::configureWriterOptions(const std::unordered_map<std::string, std::string>& sparkConfs) {
  // Parse configuration values
  if (auto it = sparkConfs.find(kParquetBlockSize); it != sparkConfs.end()) {
    maxRowGroupBytes_ = static_cast<int64_t>(stoi(it->second));
  }
  if (auto it = sparkConfs.find(kParquetBlockRows); it != sparkConfs.end()) {
    maxRowGroupRows_ = static_cast<int64_t>(stoi(it->second));
  }
  if (sparkConfs.find(kParquetRowNumInEachBlock) != sparkConfs.end()) {
    enableRowGroupAlignedWrite_ = true;
    boost::tokenizer<> tok(sparkConfs.find(kParquetRowNumInEachBlock)->second);
    std::transform(
        tok.begin(), tok.end(), std::back_inserter(expectedRowsInEachBlock_), &boost::lexical_cast<int, std::string>);
  }
  if (sparkConfs.find(kParquetWriterBufferGrowRatio) != sparkConfs.end()) {
    parquetWriterBufferGrowRatio_ = stod(sparkConfs.find(kParquetWriterBufferGrowRatio)->second);
  }
  if (sparkConfs.find(kParquetWriterBufferReserveRatio) != sparkConfs.end()) {
    parquetWriterBufferReserveRatio_ = stod(sparkConfs.find(kParquetWriterBufferReserveRatio)->second);
  }

  writerOptions_ = createWriterOptions(
      sparkConfs,
      maxRowGroupBytes_,
      expectedRowsInEachBlock_,
      enableRowGroupAlignedWrite_,
      parquetWriterBufferGrowRatio_,
      parquetWriterBufferReserveRatio_);
}

} // namespace gluten

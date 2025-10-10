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

#include <arrow/filesystem/filesystem.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/type_fwd.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "config/GlutenConfig.h"
#include "memory/ColumnarBatch.h"
#include "memory/BoltColumnarBatch.h"
#include "operators/writer/BoltDataSource.h"

#include "bolt/common/compression/Compression.h"
#include "bolt/common/file/FileSystems.h"
#ifdef ENABLE_S3
#include "bolt/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "bolt/connectors/hive/storage_adapters/s3fs/S3Util.h"
#endif
#ifdef ENABLE_GCS
#include "bolt/connectors/hive/storage_adapters/gcs/GcsFileSystem.h"
#endif
#ifdef ENABLE_HDFS
#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsUtil.h"
#endif
#ifdef ENABLE_ABFS
#include "bolt/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"
#endif
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/vector/ComplexVector.h"

namespace gluten {

inline bool isSupportedS3SdkPath(const std::string& filePath) {
  // support scheme
  const std::array<const char*, 5> supported_schemes = {"s3:", "s3a:", "oss:", "cos:", "cosn:"};

  for (const char* scheme : supported_schemes) {
    size_t scheme_length = std::strlen(scheme);
    if (filePath.length() >= scheme_length && std::strncmp(filePath.c_str(), scheme, scheme_length) == 0) {
      return true;
    }
  }
  return false;
}

inline bool isSupportedGCSPath(const std::string& filePath) {
  return strncmp(filePath.c_str(), "gs:", 3) == 0;
}

inline bool isSupportedHDFSPath(const std::string& filePath) {
  return strncmp(filePath.c_str(), "hdfs:", 5) == 0;
}

inline bool isSupportedABFSPath(const std::string& filePath) {
  return strncmp(filePath.c_str(), "abfs:", 5) == 0 || strncmp(filePath.c_str(), "abfss:", 6) == 0;
}

class BoltParquetDataSource : public BoltDataSource {
 public:
  BoltParquetDataSource(
      const std::string& filePath,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltPool,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> sinkPool,
      std::shared_ptr<arrow::Schema> schema)
      : BoltDataSource(filePath, schema), filePath_(filePath), schema_(schema), pool_(std::move(boltPool)) {}

  void init(const std::unordered_map<std::string, std::string>& sparkConfs) override;
  virtual void initSink(const std::unordered_map<std::string, std::string>& sparkConfs);
  void inspectSchema(struct ArrowSchema* out) override;
  void write(const std::shared_ptr<ColumnarBatch>& cb) override;
  void close() override;
  std::shared_ptr<arrow::Schema> getSchema() override {
    return schema_;
  }

 protected:
  std::string filePath_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> sinkPool_;
  std::unique_ptr<bytedance::bolt::dwio::common::FileSink> sink_;

 private:
  void configureWriterOptions(const std::unordered_map<std::string, std::string>& sparkConfs);

 private:
  int64_t maxRowGroupBytes_ = 128 * 1024 * 1024; // 128MB
  int64_t maxRowGroupRows_ = 100 * 1024 * 1024; // 100M

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<bytedance::bolt::parquet::Writer> parquetWriter_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;

  std::vector<int32_t> expectedRowsInEachBlock_;
  bool enableRowGroupAlignedWrite_ = false;
  double parquetWriterBufferGrowRatio_ = 1;
  double parquetWriterBufferReserveRatio_ = 0;

  // Configuration parameters
  bytedance::bolt::parquet::WriterOptions writerOptions_;
};

} // namespace gluten

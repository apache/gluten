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

#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/writer/VeloxDataSource.h"

#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#ifdef ENABLE_S3
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#endif
#ifdef ENABLE_GCS
#include "velox/connectors/hive/storage_adapters/gcs/GcsFileSystem.h"
#endif
#ifdef ENABLE_HDFS
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsUtil.h"
#endif
#ifdef ENABLE_ABFS
#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"
#endif
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/ComplexVector.h"

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

class VeloxParquetDataSource : public VeloxDataSource {
 public:
  VeloxParquetDataSource(
      const std::string& filePath,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> sinkPool,
      std::shared_ptr<arrow::Schema> schema)
      : VeloxDataSource(filePath, schema),
        filePath_(filePath),
        sinkPool_(std::move(sinkPool)),
        schema_(schema),
        pool_(std::move(veloxPool)) {}

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
  std::shared_ptr<facebook::velox::memory::MemoryPool> sinkPool_;
  std::unique_ptr<facebook::velox::dwio::common::FileSink> sink_;

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<facebook::velox::parquet::Writer> parquetWriter_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

} // namespace gluten

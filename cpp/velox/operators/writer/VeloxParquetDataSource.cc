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
#include <cstring>
#include <string>

#include "arrow/c/bridge.h"
#include "compute/VeloxRuntime.h"

#include "utils/VeloxArrowUtils.h"
#include "utils/VeloxWriterUtils.h"

using namespace facebook;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common;
using namespace facebook::velox::filesystems;

namespace gluten {

void VeloxParquetDataSource::initSink(const std::unordered_map<std::string, std::string>& /* sparkConfs */) {
  if (strncmp(filePath_.c_str(), "file:", 5) == 0) {
    sink_ = dwio::common::FileSink::create(filePath_, {.pool = sinkPool_.get()});
  } else {
    throw std::runtime_error("The file path is not local when writing data with parquet format in velox runtime!");
  }
}

void VeloxParquetDataSource::init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  initSink(sparkConfs);
  auto schema = gluten::fromArrowSchema(schema_);
  const auto writeOption = gluten::makeParquetWriteOption(sparkConfs);
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
    parquetWriter_->close();
  }
}

void VeloxParquetDataSource::write(const std::shared_ptr<ColumnarBatch>& cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  VELOX_DCHECK(veloxBatch != nullptr, "Write batch should be VeloxColumnarBatch");
  parquetWriter_->write(veloxBatch->getFlattenedRowVector());
}

} // namespace gluten

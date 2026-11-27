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

#include <arrow/io/file.h>
#include <gtest/gtest.h>

#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/vector/tests/utils/VectorMaker.h"
#include "memory/BoltColumnarBatch.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleReaderBase.h"

#include "shuffle/BoltShuffleReaderWrapper.h"

namespace gluten {
namespace {

using namespace bytedance::bolt::shuffle::sparksql;

TEST(BoltShuffleReaderTest, closesCurrentStreamBeforeFetchingNext) {
  bytedance::bolt::memory::MemoryManager::testingSetInstance({});
  auto boltPool = bytedance::bolt::memory::memoryManager()->addLeafPool();
  auto* pool = arrow::default_memory_pool();
  ShuffleReaderOptions options;
  options.compressionType = arrow::Compression::UNCOMPRESSED;
  options.partitionShortName = "single";
  options.numPartitions = 1;
  options.forceShuffleWriterType = 0;
  BoltShuffleReader reader(arrow::schema({arrow::field("id", arrow::int64())}), options, pool, boltPool.get());

  const auto tempDir = bytedance::bolt::exec::test::TempDirectoryPath::create();
  bytedance::bolt::test::VectorMaker vectors(boltPool.get());
  ShuffleWriterOptions writerOptions;
  writerOptions.partitioning = Partitioning::kSingle;
  auto& partitionOptions = writerOptions.partitionWriterOptions;
  partitionOptions.numPartitions = 1;
  partitionOptions.compressionType = options.compressionType;
  partitionOptions.configuredDirs = {tempDir->getPath()};

  std::vector<std::string> blockFiles;
  for (int64_t value : {1, 2}) {
    partitionOptions.dataFile = tempDir->getPath() + "/block-" + std::to_string(value);
    auto writer = BoltShuffleWriter::createDefault(writerOptions, boltPool.get(), pool);
    auto input = vectors.rowVector({"id"}, {vectors.flatVector<int64_t>({value})});
    auto status = writer->split(input, 64L << 20);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = writer->stop();
    ASSERT_TRUE(status.ok()) << status.ToString();
    blockFiles.push_back(partitionOptions.dataFile);
  }

  auto nextFile = blockFiles.begin();
  std::shared_ptr<arrow::io::ReadableFile> currentStream;
  BoltShuffleReaderIteratorWrapper batches([&]() -> std::unique_ptr<BoltColumnarBatchDeserializer> {
    if (currentStream) {
      EXPECT_TRUE(currentStream->closed()) << "Fetcher advanced before the current stream closed";
    }
    if (nextFile == blockFiles.end()) {
      return nullptr;
    }
    currentStream = arrow::io::ReadableFile::Open(*nextFile++).ValueOrDie();
    return reader.readStream(currentStream);
  });

  for (int64_t expected : {1, 2}) {
    auto batch = std::static_pointer_cast<BoltColumnarBatch>(batches.next());
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->numRows(), 1);
    EXPECT_EQ(batch->getRowVector()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), expected);
  }
  EXPECT_EQ(batches.next(), nullptr);
}

} // namespace
} // namespace gluten

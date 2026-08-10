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

#include "utils/VeloxWholeStageDumper.h"
#include "compute/VeloxBackend.h"
#include "config/GlutenConfig.h"
#include "operators/reader/ParquetReaderIterator.h"
#include "operators/writer/VeloxColumnarBatchWriter.h"
#include "utils/HashTableDumpFile.h"

namespace gluten {
namespace {

std::filesystem::path checkAndGetDumpPath(const std::string& saveDir, const std::string& fileName) {
  std::filesystem::path f{saveDir};
  if (std::filesystem::exists(f)) {
    if (!std::filesystem::is_directory(f)) {
      throw GlutenException("Invalid path for " + kGlutenSaveDir + ": " + saveDir);
    }
  } else {
    std::error_code ec;
    std::filesystem::create_directory(f, ec);
    if (ec) {
      throw GlutenException("Failed to create directory: " + saveDir + ", error message: " + ec.message());
    }
  }
  return f / fileName;
}

void dumpToStorage(const std::string& saveDir, const std::string& fileName, const std::string content) {
  auto dumpPath = checkAndGetDumpPath(saveDir, fileName);

  std::ofstream outFile{dumpPath};

  if (!outFile.is_open()) {
    throw GlutenException("Failed to open file for writing: " + dumpPath.string());
  }

  outFile << content;
  outFile.close();
}

// Stands in for an input iterator that turned out to be empty, for which no parquet file was
// written. Keeps the running task seeing exactly what the original iterator produced: nothing.
class EmptyColumnarBatchIterator final : public ColumnarBatchIterator {
 public:
  std::shared_ptr<ColumnarBatch> next() override {
    return nullptr;
  }
};
} // namespace

VeloxWholeStageDumper::VeloxWholeStageDumper(
    const SparkTaskInfo& taskInfo,
    const std::string& saveDir,
    int64_t batchSize,
    facebook::velox::memory::MemoryPool* aggregatePool)
    : taskInfo_(taskInfo), saveDir_(saveDir), batchSize_(batchSize), pool_(aggregatePool) {}

void VeloxWholeStageDumper::dumpConf(const std::unordered_map<std::string, std::string>& confMap) {
  const auto& backendConfMap = VeloxBackend::get()->getBackendConf()->rawConfigs();
  auto allConfMap = backendConfMap;

  for (const auto& pair : confMap) {
    allConfMap.insert_or_assign(pair.first, pair.second);
  }

  std::stringstream out;

  // Calculate the maximum key length for alignment.
  size_t maxKeyLength = 0;
  for (const auto& pair : allConfMap) {
    maxKeyLength = std::max(maxKeyLength, pair.first.length());
  }

  // Write each key-value pair to the file with adjusted spacing for alignment.

  // Dump backend conf.
  out << "[Backend Conf]" << std::endl;
  for (const auto& pair : backendConfMap) {
    out << std::left << std::setw(maxKeyLength + 1) << pair.first << ' ' << pair.second << std::endl;
  }

  // Dump session conf.
  out << std::endl << "[Session Conf]" << std::endl;
  for (const auto& pair : confMap) {
    out << std::left << std::setw(maxKeyLength + 1) << pair.first << ' ' << pair.second << std::endl;
  }

  const auto fileName = fmt::format("conf_{}_{}_{}.ini", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId);
  dumpToStorage(saveDir_, fileName, out.str());
}

void VeloxWholeStageDumper::dumpPlan(const std::string& planJson) {
  const auto fileName = fmt::format("plan_{}_{}_{}.json", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId);
  dumpToStorage(saveDir_, fileName, planJson);
}

void VeloxWholeStageDumper::dumpInputSplit(int32_t splitIndex, const std::string& splitJson) {
  const auto fileName =
      fmt::format("split_{}_{}_{}_{}.json", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId, splitIndex);
  dumpToStorage(saveDir_, fileName, splitJson);
}

void VeloxWholeStageDumper::dumpHashTable(
    const std::string& cacheKey,
    bool ignoreNullKeys,
    bool joinHasNullKeys,
    const uint8_t* data,
    size_t size) {
  HashTableDump dump;
  dump.cacheKey = cacheKey;
  dump.ignoreNullKeys = ignoreNullKeys;
  dump.joinHasNullKeys = joinHasNullKeys;
  dump.payload.assign(data, data + size);

  const auto fileName = hashTableDumpFileName(taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId, cacheKey);
  dumpToStorage(saveDir_, fileName, encodeHashTableDump(dump));
}

std::shared_ptr<ColumnarBatchIterator> VeloxWholeStageDumper::dumpInputIterator(
    int32_t iteratorIndex,
    const std::shared_ptr<ColumnarBatchIterator>& inputIterator) {
  const auto fileName =
      fmt::format("data_{}_{}_{}_{}.parquet", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId, iteratorIndex);
  const auto dumpPath = checkAndGetDumpPath(saveDir_, fileName);

  // Velox parquet writer requires aggregate memory pool.
  auto writer = std::make_shared<VeloxColumnarBatchWriter>(
      dumpPath, batchSize_, pool_->addAggregateChild(fmt::format("dump_iterator.{}", iteratorIndex)));

  bool wroteBatch = false;
  while (auto cb = inputIterator->next()) {
    GLUTEN_THROW_NOT_OK(writer->write(cb));
    wroteBatch = true;
  }
  GLUTEN_THROW_NOT_OK(writer->close());

  if (!wroteBatch) {
    // The writer takes its schema from the first batch, so an iterator that yielded nothing leaves
    // no parquet file behind. Record the gap explicitly: the benchmark binds the files passed to
    // --data to iterator indexes by position, so a silently absent file shifts every later
    // iterator onto the wrong input. The marker keeps the gap visible in the dump directory, which
    // is where whoever replays the stage is looking.
    //
    // The usual cause is the build side of a broadcast hash join, which is handed to the native
    // operator through a process local hash table cache rather than streamed. See
    // docs/developers/MicroBenchmarks.md.
    dumpToStorage(
        saveDir_,
        fileName + ".empty",
        fmt::format(
            "Input iterator {} of {} produced no batches, so {} was not written.\n"
            "If this is the build side of a broadcast hash join, re-dump with\n"
            "spark.gluten.velox.buildHashTableOncePerExecutor.enabled=false to capture it.\n",
            iteratorIndex,
            taskInfo_.toString(),
            fileName));
    LOG(WARNING) << "Input iterator " << iteratorIndex << " of " << taskInfo_ << " produced no batches, so no "
                 << fileName << " was written. Left a " << fileName << ".empty marker instead.";
    return std::make_shared<EmptyColumnarBatchIterator>();
  }

  // Velox parquet reader requires leaf memory pool.
  return std::make_shared<ParquetStreamReaderIterator>(
      dumpPath, batchSize_, pool_->addLeafChild(fmt::format("retrieve_iterator.{}", iteratorIndex)));
}

} // namespace gluten

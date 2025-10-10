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

#include "utils/BoltWholeStageDumper.h"
#include "compute/BoltBackend.h"
#include "config/GlutenConfig.h"
#include "operators/reader/ParquetReaderIterator.h"
#include "operators/writer/BoltColumnarBatchWriter.h"
#include <fstream>

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
} // namespace

BoltWholeStageDumper::BoltWholeStageDumper(
    const SparkTaskInfo& taskInfo,
    const std::string& saveDir,
    int64_t batchSize,
    bytedance::bolt::memory::MemoryPool* aggregatePool)
    : taskInfo_(taskInfo), saveDir_(saveDir), batchSize_(batchSize), pool_(aggregatePool) {}

void BoltWholeStageDumper::dumpConf(const std::unordered_map<std::string, std::string>& confMap) {
  const auto& backendConfMap = BoltBackend::get()->getBackendConf()->rawConfigs();
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

void BoltWholeStageDumper::dumpPlan(const std::string& planJson) {
  const auto fileName = fmt::format("plan_{}_{}_{}.json", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId);
  dumpToStorage(saveDir_, fileName, planJson);
}

void BoltWholeStageDumper::dumpInputSplit(int32_t splitIndex, const std::string& splitJson) {
  const auto fileName =
      fmt::format("split_{}_{}_{}_{}.json", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId, splitIndex);
  dumpToStorage(saveDir_, fileName, splitJson);
}

std::shared_ptr<ColumnarBatchIterator> BoltWholeStageDumper::dumpInputIterator(
    int32_t iteratorIndex,
    const std::shared_ptr<ColumnarBatchIterator>& inputIterator) {
  const auto fileName =
      fmt::format("data_{}_{}_{}_{}.parquet", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId, iteratorIndex);
  const auto dumpPath = checkAndGetDumpPath(saveDir_, fileName);

  // Bolt parquet writer requires aggregate memory pool.
  auto writer = std::make_shared<BoltColumnarBatchWriter>(
      dumpPath, batchSize_, pool_->addAggregateChild(fmt::format("dump_iterator.{}", iteratorIndex)));

  while (auto cb = inputIterator->next()) {
    GLUTEN_THROW_NOT_OK(writer->write(cb));
  }
  GLUTEN_THROW_NOT_OK(writer->close());

  // Bolt parquet reader requires leaf memory pool.
  return std::make_shared<ParquetStreamReaderIterator>(
      dumpPath, batchSize_, pool_->addLeafChild(fmt::format("retrieve_iterator.{}", iteratorIndex)));
}

} // namespace gluten

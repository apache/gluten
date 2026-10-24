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

#include "compute/NativeExecutor.h"

#include <arrow/io/api.h>
#include "shuffle/LocalPartitionWriter.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/VeloxBatchResizer.h"

namespace gluten {
namespace {
class BatchInput final : public ColumnarBatchIterator {
 public:
  explicit BatchInput(std::vector<std::shared_ptr<ColumnarBatch>> batches) : batches_(std::move(batches)) {}
  std::shared_ptr<ColumnarBatch> next() override {
    return index_ == batches_.size() ? nullptr : batches_[index_++];
  }

 private:
  std::vector<std::shared_ptr<ColumnarBatch>> batches_;
  size_t index_{0};
};

class FileStreams final : public StreamReader {
 public:
  std::vector<std::string> files;
  std::shared_ptr<arrow::io::InputStream> readNextStream(arrow::MemoryPool*) override {
    if (index_ == files.size())
      return nullptr;
    GLUTEN_ASSIGN_OR_THROW(auto input, arrow::io::ReadableFile::Open(files[index_++]));
    return input;
  }

 private:
  size_t index_{0};
};

class ExecutorOutput final : public ColumnarBatchIterator {
 public:
  ExecutorOutput(std::shared_ptr<ShuffleReader> reader, std::shared_ptr<ResultIterator> result)
      : reader_(std::move(reader)), result_(std::move(result)) {}
  ~ExecutorOutput() override {
    result_.reset();
    try {
      if (reader_) {
        reader_->stop();
      }
    } catch (const std::exception& error) {
      LOG(WARNING) << "Closing benchmark shuffle reader: " << error.what();
    }
  }
  std::shared_ptr<ColumnarBatch> next() override {
    return result_->hasNext() ? result_->next() : nullptr;
  }

 private:
  std::shared_ptr<ShuffleReader> reader_;
  std::shared_ptr<ResultIterator> result_;
};

} // namespace

NativeExecutor::NativeExecutor(
    VeloxRuntime* runtime,
    std::vector<std::shared_ptr<VeloxPlanEvaluator>> stages,
    int batchSize,
    bool hashPartitioned,
    bool resizeShuffleInput,
    bool resizeShuffleOutput,
    int minRows,
    int maxRows,
    int64_t preferredBytes,
    bool copyRanges,
    std::string shuffleDir)
    : runtime_(runtime),
      stages_(std::move(stages)),
      batchSize_(batchSize),
      hashPartitioned_(hashPartitioned),
      resizeShuffleInput_(resizeShuffleInput),
      resizeShuffleOutput_(resizeShuffleOutput),
      minRows_(minRows),
      maxRows_(maxRows),
      preferredBytes_(preferredBytes),
      copyRanges_(copyRanges),
      shuffleDir_(std::move(shuffleDir)) {
  GLUTEN_CHECK(runtime_ != nullptr && batchSize_ > 0, "Invalid benchmark runtime");
  GLUTEN_CHECK(stages_.size() == 1 || stages_.size() == 2, "Expected one or two benchmark stages");
  if (stages_.size() == 2) {
    stateSchema_ = toArrowSchema(stages_.front()->outputType(), runtime_->memoryManager()->getLeafMemoryPool().get());
    if (hashPartitioned_) {
      GLUTEN_ASSIGN_OR_THROW(stateSchema_, stateSchema_->RemoveField(0));
    }
  }
}

std::shared_ptr<ResultIterator> NativeExecutor::resize(std::shared_ptr<ResultIterator> input) const {
  return std::make_shared<ResultIterator>(std::make_unique<VeloxBatchResizer>(
      runtime_->memoryManager()->getLeafMemoryPool().get(),
      minRows_,
      maxRows_,
      preferredBytes_,
      std::make_unique<ExecutorOutput>(nullptr, std::move(input)),
      copyRanges_));
}

std::shared_ptr<ResultIterator> NativeExecutor::execute(
    std::vector<std::vector<std::shared_ptr<ColumnarBatch>>> inputs,
    int64_t& shuffleBytes) {
  shuffleBytes = 0;
  return stages_.size() == 1 ? runSingleStage(std::move(inputs)) : runWithShuffle(std::move(inputs), shuffleBytes);
}

std::shared_ptr<ResultIterator> NativeExecutor::runSingleStage(
    std::vector<std::vector<std::shared_ptr<ColumnarBatch>>> inputs) {
  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  for (auto& partition : inputs) {
    batches.insert(batches.end(), std::make_move_iterator(partition.begin()), std::make_move_iterator(partition.end()));
  }
  auto taskInfo = runtime_->getSparkTaskInfo().value_or(SparkTaskInfo{});
  taskInfo.partitionId = 0;
  auto input = std::make_shared<ResultIterator>(std::make_unique<BatchInput>(std::move(batches)));
  return stages_.front()->evaluate(std::move(input), taskInfo);
}

std::shared_ptr<ResultIterator> NativeExecutor::runWithShuffle(
    std::vector<std::vector<std::shared_ptr<ColumnarBatch>>> inputs,
    int64_t& shuffleBytes) {
  auto taskInfo = runtime_->getSparkTaskInfo().value_or(SparkTaskInfo{});
  auto streams = std::make_shared<FileStreams>();
  int64_t byteCount = 0;
  for (auto& partition : inputs) {
    auto input = std::make_shared<ResultIterator>(std::make_unique<BatchInput>(std::move(partition)));
    taskInfo.partitionId = streams->files.size();
    auto output = stages_.front()->evaluate(input, taskInfo);
    if (resizeShuffleInput_) {
      output = resize(std::move(output));
    }
    auto file = shuffleDir_ + "/native-" + std::to_string(streams->files.size());
    streams->files.push_back(file);
    auto partitionWriter = std::make_shared<LocalPartitionWriter>(
        1,
        nullptr,
        runtime_->memoryManager(),
        std::make_shared<LocalPartitionWriterOptions>(),
        file,
        std::vector<std::string>{shuffleDir_});
    auto options = std::make_shared<HashShuffleWriterOptions>();
    options->partitioning = hashPartitioned_ ? Partitioning::kHash : Partitioning::kSingle;
    options->splitBufferSize = batchSize_;
    auto writer = runtime_->createShuffleWriter(1, partitionWriter, options);
    while (output->hasNext()) {
      auto batch = output->next();
      if (batch->numRows() > 0) {
        GLUTEN_THROW_NOT_OK(writer->write(batch, ShuffleWriter::kMaxMemLimit));
      }
    }
    GLUTEN_THROW_NOT_OK(writer->stop());
    byteCount += writer->bytesWritten();
  }
  auto options = std::make_shared<ShuffleReaderOptions>();
  options->compressionType = arrow::Compression::UNCOMPRESSED;
  options->batchSize = batchSize_;
  auto reader = runtime_->createShuffleReader(stateSchema_, options);
  auto decoded = reader->read(streams, ShuffleReader::OutputType::kRowVector);
  if (resizeShuffleOutput_) {
    decoded = resize(std::move(decoded));
  }
  taskInfo.partitionId = 0;
  auto result = stages_.back()->evaluate(decoded, taskInfo);
  shuffleBytes = byteCount;
  return std::make_shared<ResultIterator>(
      std::make_unique<ExecutorOutput>(std::move(reader), std::move(result)), runtime_);
}
} // namespace gluten

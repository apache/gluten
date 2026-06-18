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

#include "shuffle/LocalPartitionWriter.h"

#include "shuffle/Dictionary.h"
#include "shuffle/Payload.h"
#include "shuffle/Spill.h"
#include "shuffle/Utils.h"
#include "utils/Timer.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>
#include <filesystem>
#include <random>
#include <thread>

namespace gluten {

namespace {
arrow::Result<std::shared_ptr<arrow::io::OutputStream>> openFile(const std::string& file, int64_t bufferSize) {
  std::shared_ptr<arrow::io::FileOutputStream> out;
  const auto fd = open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0000);
  // Set the shuffle file permissions to 0644 to keep it consistent with the permissions of
  // the built-in shuffler manager in Spark.
  fchmod(fd, 0644);
  ARROW_ASSIGN_OR_RAISE(out, arrow::io::FileOutputStream::Open(fd));

  // The `shuffleFileBufferSize` bytes is a temporary allocation and will be freed with file close.
  // Use default memory pool and count treat the memory as executor memory overhead to avoid unnecessary spill.
  return arrow::io::BufferedOutputStream::Create(bufferSize, arrow::default_memory_pool(), out);
}
} // namespace

class LocalPartitionWriter::LocalSpiller {
 public:
  LocalSpiller(
      bool isFinal,
      std::shared_ptr<arrow::io::OutputStream> os,
      std::string spillFile,
      int32_t compressionBufferSize,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : isFinal_(isFinal),
        os_(os),
        spillFile_(std::move(spillFile)),
        pool_(pool),
        codec_(codec),
        diskSpill_(std::make_unique<Spill>()) {
    if (codec_ != nullptr) {
      GLUTEN_ASSIGN_OR_THROW(
          compressedOs_,
          ShuffleCompressedOutputStream::Make(codec_, compressionBufferSize, os, arrow::default_memory_pool()));
    }
  }

  arrow::Status spill(uint32_t partitionId, std::unique_ptr<BlockPayload> payload) {
    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());

    static constexpr uint8_t kSpillBlockType = static_cast<uint8_t>(BlockType::kPlainPayload);

    RETURN_NOT_OK(os_->Write(&kSpillBlockType, sizeof(kSpillBlockType)));
    RETURN_NOT_OK(payload->serialize(os_.get()));

    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());

    DLOG(INFO) << "LocalSpiller: Spilled partition " << partitionId << " file start: " << start << ", file end: " << end
               << ", file: " << spillFile_;

    compressTime_ += payload->getCompressTime();
    spillTime_ += payload->getWriteTime();

    diskSpill_->insertPayload(
        partitionId, payload->type(), payload->numRows(), payload->isValidityBuffer(), end - start, pool_, codec_);

    return arrow::Status::OK();
  }

  arrow::Status spill(uint32_t partitionId, std::unique_ptr<InMemoryPayload> payload) {
    ScopedTimer timer(&spillTime_);

    if (curPid_ != partitionId) {
      // Record the write position of the new partition.
      ARROW_ASSIGN_OR_RAISE(writePos_, os_->Tell());
      curPid_ = partitionId;
    }
    flushed_ = false;

    auto* raw = compressedOs_ != nullptr ? compressedOs_.get() : os_.get();
    RETURN_NOT_OK(payload->serialize(raw));

    return arrow::Status::OK();
  }

  arrow::Status flush() {
    if (flushed_) {
      return arrow::Status::OK();
    }
    flushed_ = true;

    if (compressedOs_ != nullptr) {
      RETURN_NOT_OK(compressedOs_->Flush());
    }
    RETURN_NOT_OK(insertSpill());

    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<Spill>> finish() {
    ARROW_RETURN_IF(finished_, arrow::Status::Invalid("Calling finish() on a finished LocalSpiller."));
    ARROW_RETURN_IF(os_->closed(), arrow::Status::Invalid("Spill file os has been closed."));

    if (curPid_ != -1) {
      if (compressedOs_ != nullptr) {
        compressTime_ = compressedOs_->compressTime();
        spillTime_ -= compressTime_;
        RETURN_NOT_OK(compressedOs_->Close());
      }
      RETURN_NOT_OK(insertSpill());
    }

    if (!isFinal_) {
      RETURN_NOT_OK(os_->Close());
    }

    diskSpill_->setSpillFile(spillFile_);
    diskSpill_->setSpillTime(spillTime_);
    diskSpill_->setCompressTime(compressTime_);
    finished_ = true;

    return std::move(diskSpill_);
  }

  bool finished() const {
    return finished_;
  }

 private:
  arrow::Status insertSpill() {
    ARROW_ASSIGN_OR_RAISE(const auto pos, os_->Tell());
    GLUTEN_DCHECK(pos >= writePos_, "Current write position should not be less than the last write position.");
    if (pos > writePos_) {
      diskSpill_->insertPayload(curPid_, Payload::kRaw, 0, nullptr, pos - writePos_, pool_, nullptr);
      DLOG(INFO) << "LocalSpiller: Spilled partition " << curPid_ << " file start: " << writePos_
                 << ", file end: " << pos << ", file: " << spillFile_;
      writePos_ = pos;
    }
    return arrow::Status::OK();
  }

  bool isFinal_;

  std::shared_ptr<arrow::io::OutputStream> os_;
  std::shared_ptr<ShuffleCompressedOutputStream> compressedOs_{nullptr};
  int64_t writePos_{0};

  std::string spillFile_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  std::shared_ptr<Spill> diskSpill_{nullptr};

  bool flushed_{true};
  bool finished_{false};
  int64_t spillTime_{0};
  int64_t compressTime_{0};
  int32_t curPid_{-1};
};

class LocalPartitionWriter::PayloadMerger {
 public:
  PayloadMerger(
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec,
      int32_t compressionThreshold,
      int32_t mergeBufferSize,
      int32_t mergeBufferMinSize)
      : pool_(pool),
        codec_(codec),
        compressionThreshold_(compressionThreshold),
        mergeBufferSize_(mergeBufferSize),
        mergeBufferMinSize_(mergeBufferMinSize) {}

  arrow::Result<std::vector<std::unique_ptr<InMemoryPayload>>>
  merge(uint32_t partitionId, std::unique_ptr<InMemoryPayload> append, bool reuseBuffers) {
    PartitionScopeGuard mergeGuard(partitionInUse_, partitionId);

    std::vector<std::unique_ptr<InMemoryPayload>> merged{};
    if (!append->mergeable()) {
      // TODO: Merging complex type is currently not supported.
      bool shouldCompress = codec_ != nullptr && append->numRows() >= compressionThreshold_;
      if (reuseBuffers && !shouldCompress) {
        RETURN_NOT_OK(append->copyBuffers(pool_));
      }
      merged.emplace_back(std::move(append));
      return merged;
    }

    auto cacheOrFinish = [&]() {
      if (append->numRows() <= mergeBufferMinSize_) {
        // Save for merge.
        if (reuseBuffers) {
          // This is the first append, therefore need copy.
          RETURN_NOT_OK(append->copyBuffers(pool_));
        }
        partitionMergePayload_[partitionId] = std::move(append);
        return arrow::Status::OK();
      }
      // Commit if current buffer rows reaches merging threshold.
      bool shouldCompress = codec_ != nullptr && append->numRows() >= compressionThreshold_;
      if (reuseBuffers && !shouldCompress) {
        RETURN_NOT_OK(append->copyBuffers(pool_));
      }
      merged.emplace_back(std::move(append));
      return arrow::Status::OK();
    };

    if (!hasMerged(partitionId)) {
      RETURN_NOT_OK(cacheOrFinish());
      return merged;
    }

    auto lastPayload = std::move(partitionMergePayload_[partitionId]);
    auto mergedRows = append->numRows() + lastPayload->numRows();
    if (mergedRows > mergeBufferSize_ || append->numRows() > mergeBufferMinSize_) {
      merged.emplace_back(std::move(lastPayload));
      RETURN_NOT_OK(cacheOrFinish());
      return merged;
    }

    // Merge buffers.
    DLOG(INFO) << "Merged partition: " << partitionId << ", numRows before: " << lastPayload->numRows()
               << ", numRows appended: " << append->numRows() << ", numRows after: " << mergedRows;
    ARROW_ASSIGN_OR_RAISE(auto payload, InMemoryPayload::merge(std::move(lastPayload), std::move(append), pool_));
    if (mergedRows < mergeBufferSize_) {
      // Still not reach merging threshold, save for next merge.
      partitionMergePayload_[partitionId] = std::move(payload);
      return merged;
    }

    // mergedRows == mergeBufferSize_
    merged.emplace_back(std::move(payload));
    return merged;
  }

  arrow::Result<std::optional<std::unique_ptr<InMemoryPayload>>> finish(uint32_t partitionId, bool fromSpill) {
    // We need to check whether the spill source is from compressing/copying the merged buffers.
    if ((fromSpill && (partitionInUse_.has_value() && partitionInUse_.value() == partitionId)) ||
        !hasMerged(partitionId)) {
      return std::nullopt;
    }

    if (!fromSpill) {
      GLUTEN_DCHECK(
          !partitionInUse_.has_value(),
          "Invalid status: partitionInUse_ is set when not in spilling: " + std::to_string(partitionInUse_.value()));
    }

    return std::move(partitionMergePayload_[partitionId]);
  }

  bool hasMerged(uint32_t partitionId) {
    return partitionMergePayload_.find(partitionId) != partitionMergePayload_.end() &&
        partitionMergePayload_[partitionId] != nullptr;
  }

 private:
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;
  int32_t compressionThreshold_;
  int32_t mergeBufferSize_;
  int32_t mergeBufferMinSize_;
  std::unordered_map<uint32_t, std::unique_ptr<InMemoryPayload>> partitionMergePayload_;
  std::optional<uint32_t> partitionInUse_{std::nullopt};
};

class LocalPartitionWriter::PayloadCache {
 public:
  PayloadCache(
      uint32_t numPartitions,
      arrow::util::Codec* codec,
      int32_t compressionThreshold,
      bool enableDictionary,
      arrow::MemoryPool* pool,
      MemoryManager* memoryManager)
      : numPartitions_(numPartitions),
        codec_(codec),
        compressionThreshold_(compressionThreshold),
        enableDictionary_(enableDictionary),
        pool_(pool),
        memoryManager_(memoryManager) {}

  arrow::Status cache(uint32_t partitionId, std::unique_ptr<InMemoryPayload> payload) {
    PartitionScopeGuard cacheGuard(partitionInUse_, partitionId);

    if (partitionCachedPayload_.find(partitionId) == partitionCachedPayload_.end()) {
      partitionCachedPayload_[partitionId] = std::list<std::unique_ptr<BlockPayload>>{};
    }

    if (enableDictionary_) {
      if (partitionDictionaries_.find(partitionId) == partitionDictionaries_.end()) {
        partitionDictionaries_[partitionId] = createDictionaryWriter(memoryManager_, codec_);
      }
      RETURN_NOT_OK(payload->createDictionaries(partitionDictionaries_[partitionId]));
    }

    bool shouldCompress = codec_ != nullptr && payload->numRows() >= compressionThreshold_;
    ARROW_ASSIGN_OR_RAISE(
        auto block,
        payload->toBlockPayload(shouldCompress ? Payload::kCompressed : Payload::kUncompressed, pool_, codec_));

    partitionCachedPayload_[partitionId].push_back(std::move(block));

    return arrow::Status::OK();
  }

  arrow::Status write(uint32_t partitionId, arrow::io::OutputStream* os) {
    GLUTEN_DCHECK(
        !partitionInUse_.has_value(),
        "Invalid status: partitionInUse_ is set: " + std::to_string(partitionInUse_.value()));

    if (hasCachedPayloads(partitionId)) {
      ARROW_ASSIGN_OR_RAISE(const bool hasDictionaries, writeDictionaries(partitionId, os));

      auto& payloads = partitionCachedPayload_[partitionId];
      while (!payloads.empty()) {
        const auto payload = std::move(payloads.front());
        payloads.pop_front();

        // Write the cached payload to disk.
        uint8_t blockType =
            static_cast<uint8_t>(hasDictionaries ? BlockType::kDictionaryPayload : BlockType::kPlainPayload);
        RETURN_NOT_OK(os->Write(&blockType, sizeof(blockType)));
        RETURN_NOT_OK(payload->serialize(os));

        compressTime_ += payload->getCompressTime();
        writeTime_ += payload->getWriteTime();
      }
    }
    return arrow::Status::OK();
  }

  bool canSpill() {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (partitionInUse_.has_value() && partitionInUse_.value() == pid) {
        continue;
      }
      if (hasCachedPayloads(pid)) {
        return true;
      }
    }
    return false;
  }

  arrow::Result<std::shared_ptr<Spill>> spill(
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec,
      const int64_t bufferSize,
      int64_t& totalBytesToEvict) {
    ARROW_ASSIGN_OR_RAISE(const auto os, openFile(spillFile, bufferSize));

    int64_t start = 0;
    auto diskSpill = std::make_shared<Spill>();

    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (partitionInUse_.has_value() && partitionInUse_.value() == pid) {
        continue;
      }

      if (hasCachedPayloads(pid)) {
        ARROW_ASSIGN_OR_RAISE(const bool hasDictionaries, writeDictionaries(pid, os.get()));

        auto& payloads = partitionCachedPayload_[pid];
        while (!payloads.empty()) {
          auto payload = std::move(payloads.front());
          payloads.pop_front();
          totalBytesToEvict += payload->rawSize();

          // Spill the cached payload to disk.
          uint8_t blockType =
              static_cast<uint8_t>(hasDictionaries ? BlockType::kDictionaryPayload : BlockType::kPlainPayload);
          RETURN_NOT_OK(os->Write(&blockType, sizeof(blockType)));
          RETURN_NOT_OK(payload->serialize(os.get()));

          compressTime_ += payload->getCompressTime();
          spillTime_ += payload->getWriteTime();
        }

        ARROW_ASSIGN_OR_RAISE(auto end, os->Tell());

        diskSpill->insertPayload(pid, Payload::kRaw, 0, nullptr, end - start, pool, codec);

        DLOG(INFO) << "PayloadCache: Spilled partition " << pid << " file start: " << start << ", file end: " << end
                   << ", file: " << spillFile;

        start = end;
      }
    }

    RETURN_NOT_OK(os->Close());
    diskSpill->setSpillFile(spillFile);

    return diskSpill;
  }

  int64_t getCompressTime() const {
    return compressTime_;
  }

  int64_t getSpillTime() const {
    return spillTime_;
  }

  int64_t getWriteTime() const {
    return writeTime_;
  }

  double getAvgDictionaryFields() const {
    if (numDictionaryPayloads_ == 0 || dictionaryFieldCount_ == 0) {
      return 0.0;
    }
    return dictionaryFieldCount_ / static_cast<double>(numDictionaryPayloads_);
  }

  int64_t getDictionarySize() const {
    return dictionarySize_;
  }

 private:
  bool hasCachedPayloads(uint32_t partitionId) {
    return partitionCachedPayload_.find(partitionId) != partitionCachedPayload_.end() &&
        !partitionCachedPayload_[partitionId].empty();
  }

  arrow::Result<bool> writeDictionaries(uint32_t partitionId, arrow::io::OutputStream* os) {
    if (!enableDictionary_) {
      return false;
    }

    const auto& dict = partitionDictionaries_.find(partitionId);
    GLUTEN_DCHECK(
        dict != partitionDictionaries_.end(),
        "Dictionary for partition " + std::to_string(partitionId) + " not found.");

    const auto numDictionaryFields = dict->second->numDictionaryFields();
    dictionaryFieldCount_ += numDictionaryFields;
    ++numDictionaryPayloads_;

    if (numDictionaryFields == 0) {
      // No dictionary fields, no need to write.
      return false;
    }

    dictionarySize_ += partitionDictionaries_[partitionId]->getDictionarySize();

    static constexpr uint8_t kDictionaryBlock = static_cast<uint8_t>(BlockType::kDictionary);
    RETURN_NOT_OK(os->Write(&kDictionaryBlock, sizeof(kDictionaryBlock)));
    RETURN_NOT_OK(partitionDictionaries_[partitionId]->serialize(os));

    partitionDictionaries_.erase(partitionId);

    return true;
  }

  uint32_t numPartitions_;
  arrow::util::Codec* codec_;
  int32_t compressionThreshold_;
  bool enableDictionary_;
  arrow::MemoryPool* pool_;
  MemoryManager* memoryManager_;

  int64_t compressTime_{0};
  int64_t spillTime_{0};
  int64_t writeTime_{0};
  std::unordered_map<uint32_t, std::list<std::unique_ptr<BlockPayload>>> partitionCachedPayload_;

  std::unordered_map<uint32_t, std::shared_ptr<ShuffleDictionaryWriter>> partitionDictionaries_;

  int64_t dictionaryFieldCount_{0};
  int64_t numDictionaryPayloads_{0};
  int64_t dictionarySize_{0};

  std::optional<uint32_t> partitionInUse_{std::nullopt};
};

LocalPartitionWriter::LocalPartitionWriter(
    uint32_t numPartitions,
    std::unique_ptr<arrow::util::Codec> codec,
    MemoryManager* memoryManager,
    const std::shared_ptr<LocalPartitionWriterOptions>& options,
    const std::string& dataFile,
    std::vector<std::string> localDirs)
    : PartitionWriter(numPartitions, std::move(codec), memoryManager),
      options_(options),
      dataFile_(dataFile),
      localDirs_(std::move(localDirs)) {
  init();
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getShuffleSpillDir(localDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] = (subDirSelection_[dirSelection_] + 1) % options_->numSubDirs;
  dirSelection_ = (dirSelection_ + 1) % localDirs_.size();
  return spilledFileDir;
}

arrow::Status LocalPartitionWriter::clearResource() {
  if (dataFileOs_ != nullptr) {
    RETURN_NOT_OK(dataFileOs_->Close());
    // When bufferedWrite = true, dataFileOs_->Close doesn't release underlying buffer.
    dataFileOs_.reset();
  }

  // Check all spills are merged.
  size_t spillId = 0;
  for (const auto& spill : spills_) {
    compressTime_ += spill->compressTime();
    spillTime_ += spill->spillTime();
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (spill->hasNextPayload(pid)) {
        return arrow::Status::Invalid(
            "Merging from spill " + std::to_string(spillId) + " is not exhausted. pid: " + std::to_string(pid));
      }
    }
    if (std::filesystem::exists(spill->spillFile()) && !std::filesystem::remove(spill->spillFile())) {
      LOG(WARNING) << "Error while deleting spill file " << spill->spillFile();
    }
    ++spillId;
  }
  spills_.clear();

  return arrow::Status::OK();
}

void LocalPartitionWriter::init() {
  GLUTEN_CHECK(!dataFile_.empty(), "Shuffle data file path is not configured.");
  GLUTEN_CHECK(!localDirs_.empty(), "Shuffle local directories can not be empty.");

  partitionLengths_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);

  // Shuffle the configured local directories. This prevents each task from using the same directory for spilled
  // files.
  std::random_device rd;
  std::default_random_engine engine(rd());
  std::shuffle(localDirs_.begin(), localDirs_.end(), engine);
  subDirSelection_.assign(localDirs_.size(), 0);
}

arrow::Result<int64_t> LocalPartitionWriter::mergeSpills(uint32_t partitionId, arrow::io::OutputStream* os) {
  int64_t bytesEvicted = 0;
  int32_t spillIndex = 0;

  for (const auto& spill : spills_) {
    ARROW_ASSIGN_OR_RAISE(auto startPos, os->Tell());

    spill->openForRead(options_->shuffleFileBufferSize);

    // Read if partition exists in the spilled file. Then write to the final data file.
    while (auto payload = spill->nextPayload(partitionId)) {
      RETURN_NOT_OK(payload->serialize(os));
      compressTime_ += payload->getCompressTime();
      writeTime_ += payload->getWriteTime();
    }

    ARROW_ASSIGN_OR_RAISE(auto endPos, os->Tell());
    auto bytesWritten = endPos - startPos;

    DLOG(INFO) << "Partition " << partitionId << " spilled from spillResult " << spillIndex++ << " of bytes "
               << bytesWritten;

    bytesEvicted += bytesWritten;
  }

  totalBytesEvicted_ += bytesEvicted;
  return bytesEvicted;
}

arrow::Status LocalPartitionWriter::writeCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) const {
  if (payloadCache_ != nullptr) {
    RETURN_NOT_OK(payloadCache_->write(partitionId, os));
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop(ShuffleWriterMetrics* metrics, int64_t& evictBytes) {
  if (stopped_) {
    return arrow::Status::OK();
  }
  stopped_ = true;

  if (useSpillFileAsDataFile_) {
    ARROW_ASSIGN_OR_RAISE(auto spill, spiller_->finish());

    // Merge the remaining partitions from spills.
    if (!spills_.empty()) {
      for (auto pid = lastEvictPid_ + 1; pid < numPartitions_; ++pid) {
        ARROW_ASSIGN_OR_RAISE(partitionLengths_[pid], mergeSpills(pid, dataFileOs_.get()));
      }
    }

    for (auto pid = 0; pid < numPartitions_; ++pid) {
      while (auto payload = spill->nextPayload(pid)) {
        partitionLengths_[pid] += payload->rawSize();
      }
    }
    writeTime_ = spill->spillTime();
    compressTime_ += spill->compressTime();
  } else {
    RETURN_NOT_OK(finishSpill());
    RETURN_NOT_OK(finishMerger());

    ARROW_ASSIGN_OR_RAISE(dataFileOs_, openFile(dataFile_, options_->shuffleFileBufferSize));

    int64_t endInFinalFile = 0;
    DLOG(INFO) << "LocalPartitionWriter stopped. Total spills: " << spills_.size();
    // Iterator over pid.
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      // Record start offset.
      auto startInFinalFile = endInFinalFile;
      // Iterator over all spilled files.
      // May trigger spill during compression.
      RETURN_NOT_OK(mergeSpills(pid, dataFileOs_.get()));
      RETURN_NOT_OK(writeCachedPayloads(pid, dataFileOs_.get()));

      ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
      partitionLengths_[pid] = endInFinalFile - startInFinalFile;
    }
  }
  ARROW_ASSIGN_OR_RAISE(totalBytesWritten_, dataFileOs_->Tell());
  evictBytes += totalBytesWritten_;

  // Close Final file. Clear buffered resources.
  RETURN_NOT_OK(clearResource());

  // Populate shuffle writer metrics.
  RETURN_NOT_OK(populateMetrics(metrics));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::requestSpill(bool isFinal) {
  if (!spiller_ || spiller_->finished()) {
    std::string spillFile;
    std::shared_ptr<arrow::io::OutputStream> os;
    if (isFinal) {
      // If `spill()` is requested after `stop()`, open the final data file for writing.
      ARROW_ASSIGN_OR_RAISE(dataFileOs_, openFile(dataFile_, options_->shuffleFileBufferSize));
      spillFile = dataFile_;
      os = dataFileOs_;
      useSpillFileAsDataFile_ = true;
    } else {
      ARROW_ASSIGN_OR_RAISE(spillFile, createTempShuffleFile(nextSpilledFileDir()));
      ARROW_ASSIGN_OR_RAISE(os, openFile(spillFile, options_->shuffleFileBufferSize));
    }
    spiller_ = std::make_unique<LocalSpiller>(
        isFinal, os, std::move(spillFile), options_->compressionBufferSize, payloadPool_.get(), codec_.get());
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::finishSpill() {
  if (spiller_ && !spiller_->finished()) {
    auto spiller = std::move(spiller_);
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(spills_.back(), spiller->finish());
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::finishMerger() {
  if (merger_ != nullptr) {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      ARROW_ASSIGN_OR_RAISE(auto maybeMerged, merger_->finish(pid, false));
      if (maybeMerged.has_value()) {
        if (payloadCache_ == nullptr) {
          payloadCache_ = std::make_shared<PayloadCache>(
              numPartitions_,
              codec_.get(),
              options_->compressionThreshold,
              options_->enableDictionary,
              payloadPool_.get(),
              memoryManager_);
        }
        // Spill can be triggered by compressing or building dictionaries.
        RETURN_NOT_OK(payloadCache_->cache(pid, std::move(maybeMerged.value())));
      }
    }
    merger_.reset();
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::hashEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    Evict::type evictType,
    bool reuseBuffers,
    int64_t& evictBytes) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->rawSize();

  if (evictType == Evict::kSpill) {
    RETURN_NOT_OK(requestSpill(false));

    auto shouldCompress = codec_ != nullptr && inMemoryPayload->numRows() >= options_->compressionThreshold;
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        inMemoryPayload->toBlockPayload(
            shouldCompress ? Payload::kToBeCompressed : Payload::kUncompressed, payloadPool_.get(), codec_.get()));

    RETURN_NOT_OK(spiller_->spill(partitionId, std::move(payload)));
    return arrow::Status::OK();
  }

  if (!merger_) {
    merger_ = std::make_shared<PayloadMerger>(
        payloadPool_.get(),
        codec_.get(),
        options_->compressionThreshold,
        options_->mergeBufferSize,
        options_->mergeBufferSize * options_->mergeThreshold);
  }
  ARROW_ASSIGN_OR_RAISE(auto merged, merger_->merge(partitionId, std::move(inMemoryPayload), reuseBuffers));
  if (!merged.empty()) {
    if (UNLIKELY(!payloadCache_)) {
      payloadCache_ = std::make_shared<PayloadCache>(
          numPartitions_,
          codec_.get(),
          options_->compressionThreshold,
          options_->enableDictionary,
          payloadPool_.get(),
          memoryManager_);
    }
    for (auto& payload : merged) {
      RETURN_NOT_OK(payloadCache_->cache(partitionId, std::move(payload)));
    }
    merged.clear();
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::sortEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    bool isFinal,
    int64_t& evictBytes) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->rawSize();

  if (lastEvictPid_ != -1 && (partitionId < lastEvictPid_ || (isFinal && !dataFileOs_))) {
    lastEvictPid_ = -1;
    RETURN_NOT_OK(finishSpill());
  }
  RETURN_NOT_OK(requestSpill(isFinal));

  if (lastEvictPid_ != partitionId) {
    // Flush the remaining data for lastEvictPid_.
    RETURN_NOT_OK(spiller_->flush());

    // For final data file, merge all spills for partitions in [lastEvictPid_ + 1, partitionId]. Note in this function,
    // only the spilled partitions before partitionId are merged. Therefore, the remaining partitions after partitionId
    // are not merged here and will be merged in `stop()`.
    if (isFinal && !spills_.empty()) {
      for (auto pid = lastEvictPid_ + 1; pid <= partitionId; ++pid) {
        ARROW_ASSIGN_OR_RAISE(partitionLengths_[pid], mergeSpills(pid, dataFileOs_.get()));
      }
    }
  }

  RETURN_NOT_OK(spiller_->spill(partitionId, std::move(inMemoryPayload)));
  lastEvictPid_ = partitionId;
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  // Finish last spiller.
  RETURN_NOT_OK(finishSpill());

  int64_t reclaimed = 0;
  // Reclaim memory from payloadCache.
  if (payloadCache_ && payloadCache_->canSpill()) {
    auto beforeSpill = payloadPool_->bytes_allocated();

    ARROW_ASSIGN_OR_RAISE(auto spillFile, createTempShuffleFile(nextSpilledFileDir()));

    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        spills_.back(),
        payloadCache_->spill(
            spillFile, payloadPool_.get(), codec_.get(), options_->shuffleFileBufferSize, totalBytesToEvict_));

    reclaimed += beforeSpill - payloadPool_->bytes_allocated();

    if (reclaimed >= size) {
      *actual = reclaimed;
      return arrow::Status::OK();
    }
  }

  // Then spill payloads from merger. Create uncompressed payloads.
  if (merger_) {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      ARROW_ASSIGN_OR_RAISE(auto maybeMerged, merger_->finish(pid, true));

      if (maybeMerged.has_value()) {
        const auto& merged = maybeMerged.value();

        totalBytesToEvict_ += merged->rawSize();
        reclaimed += merged->rawCapacity();

        RETURN_NOT_OK(requestSpill(false));

        bool shouldCompress = codec_ != nullptr && merged->numRows() >= options_->compressionThreshold;
        ARROW_ASSIGN_OR_RAISE(
            auto payload,
            merged->toBlockPayload(
                shouldCompress ? Payload::kToBeCompressed : Payload::kUncompressed, payloadPool_.get(), codec_.get()));

        RETURN_NOT_OK(spiller_->spill(pid, std::move(payload)));
      }
    }
    RETURN_NOT_OK(finishSpill());
  }

  *actual = reclaimed;
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::populateMetrics(ShuffleWriterMetrics* metrics) {
  if (payloadCache_) {
    spillTime_ += payloadCache_->getSpillTime();
    writeTime_ += payloadCache_->getWriteTime();
    compressTime_ += payloadCache_->getCompressTime();
    metrics->avgDictionaryFields = payloadCache_->getAvgDictionaryFields();
    metrics->dictionarySize = payloadCache_->getDictionarySize();
  }

  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += spillTime_;
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesToEvict += totalBytesToEvict_;
  metrics->totalBytesEvicted += totalBytesEvicted_;
  metrics->totalBytesWritten += totalBytesWritten_;
  metrics->partitionLengths = std::move(partitionLengths_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}
} // namespace gluten

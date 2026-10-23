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

#include <arrow/array.h>
#include <arrow/ipc/writer.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>

#include <chrono>
#include <filesystem>

#include "utils/Compression.h"

#include <utils/Exception.h>
#include <utils/Timer.h>

namespace gluten {

using StringLengthType = uint32_t;
using IpcOffsetBufferType = arrow::LargeStringType::offset_type;

static const size_t kSizeOfStringLength = sizeof(StringLengthType);
static const size_t kSizeOfIpcOffsetBuffer = sizeof(IpcOffsetBufferType);
static const std::string kGlutenSparkLocalDirs = "GLUTEN_SPARK_LOCAL_DIRS";

class PartitionScopeGuard {
 public:
  PartitionScopeGuard(std::optional<uint32_t>& partitionInUse, uint32_t partitionId) : partitionInUse_(partitionInUse) {
    GLUTEN_DCHECK(!partitionInUse_.has_value(), "Partition id is already set.");
    partitionInUse_ = partitionId;
  }

  ~PartitionScopeGuard() {
    partitionInUse_ = std::nullopt;
  }

 private:
  std::optional<uint32_t>& partitionInUse_;
};

std::string getShuffleSpillDir(const std::string& configuredDir, int32_t subDirId);

arrow::Result<std::string> createTempShuffleFile(const std::string& dir);

arrow::Result<std::vector<std::shared_ptr<arrow::DataType>>> toShuffleTypeId(
    const std::vector<std::shared_ptr<arrow::Field>>& fields);

int64_t getBufferSize(const std::shared_ptr<arrow::Array>& array);

int64_t getBufferSize(const std::vector<std::shared_ptr<arrow::Buffer>>& buffers);

int64_t getBufferCapacity(const std::vector<std::shared_ptr<arrow::Buffer>>& buffers);

int64_t getMaxCompressedBufferSize(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::util::Codec* codec);

std::shared_ptr<arrow::Buffer> zeroLengthNullBuffer();

// Velox may read past the end of a buffer with SIMD loads, up to simd::kPadding bytes,
// which its AlignedBuffer keeps addressable. The shuffle reader hands deserialized buffers
// to Velox without a copy, so they reserve the same spare capacity. 64 covers AVX-512.
constexpr int64_t kVeloxBufferPadding = 64;

// Allocates a buffer of 'size' bytes with at least kVeloxBufferPadding bytes of spare
// capacity after it.
arrow::Result<std::shared_ptr<arrow::ResizableBuffer>> allocatePaddedBuffer(int64_t size, arrow::MemoryPool* pool);

// Resizes 'buffer' to 'size' bytes, keeping at least kVeloxBufferPadding bytes of spare
// capacity after it.
arrow::Status resizeWithPadding(arrow::ResizableBuffer& buffer, int64_t size);

// MmapFileStream is used to optimize sequential file reading. It uses madvise
// to prefetch and release memory timely.
class MmapFileStream : public arrow::io::InputStream {
 public:
  MmapFileStream(arrow::internal::FileDescriptor fd, uint8_t* data, int64_t size, uint64_t prefetchSize);

  static arrow::Result<std::shared_ptr<MmapFileStream>> open(const std::string& path, uint64_t prefetchSize = 0);

  arrow::Result<int64_t> Tell() const override;

  arrow::Status Close() override;

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

  bool closed() const override;

 private:
  arrow::Result<int64_t> actualReadSize(int64_t nbytes);

  void advance(int64_t length);

  void willNeed(int64_t length);

  // Page-aligned prefetch size
  const int64_t prefetchSize_;
  arrow::internal::FileDescriptor fd_;
  uint8_t* data_ = nullptr;
  int64_t size_;
  int64_t pos_ = 0;
  int64_t posFetch_ = 0;
  int64_t posRetain_ = 0;
};

// Adopted from arrow::io::CompressedOutputStream. Rebuild compressor after each `Flush()`.
class ShuffleCompressedOutputStream : public arrow::io::OutputStream {
 public:
  /// \brief Create a compressed output stream wrapping the given output stream.
  static arrow::Result<std::shared_ptr<ShuffleCompressedOutputStream>> Make(
      arrow::util::Codec* codec,
      int32_t compressionBufferSize,
      const std::shared_ptr<OutputStream>& raw,
      arrow::MemoryPool* pool) {
    auto res = std::shared_ptr<ShuffleCompressedOutputStream>(
        new ShuffleCompressedOutputStream(codec, compressionBufferSize, raw, pool));
    RETURN_NOT_OK(res->Init(codec));
    return res;
  }

  arrow::Result<int64_t> Tell() const override {
    return totalPos_;
  }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    ARROW_RETURN_IF(!isOpen_, arrow::Status::Invalid("Stream is closed"));

    if (nbytes == 0) {
      return arrow::Status::OK();
    }

    freshCompressor_ = false;

    int64_t flushTime = 0;
    {
      ScopedTimer timer(&compressTime_);
      auto input = static_cast<const uint8_t*>(data);
      while (nbytes > 0) {
        int64_t input_len = nbytes;
        int64_t output_len = compressed_->size() - compressedPos_;
        uint8_t* output = compressed_->mutable_data() + compressedPos_;
        ARROW_ASSIGN_OR_RAISE(auto result, compressor_->Compress(input_len, input, output_len, output));
        compressedPos_ += result.bytes_written;

        if (result.bytes_read == 0) {
          // Not enough output, try to flush it and retry
          if (compressedPos_ > 0) {
            RETURN_NOT_OK(FlushCompressed(flushTime));
            output_len = compressed_->size() - compressedPos_;
            output = compressed_->mutable_data() + compressedPos_;
            ARROW_ASSIGN_OR_RAISE(result, compressor_->Compress(input_len, input, output_len, output));
            compressedPos_ += result.bytes_written;
          }
        }
        input += result.bytes_read;
        nbytes -= result.bytes_read;
        totalPos_ += result.bytes_read;
        if (compressedPos_ == compressed_->size()) {
          // Output buffer full, flush it
          RETURN_NOT_OK(FlushCompressed(flushTime));
        }
        if (result.bytes_read == 0) {
          // Need to enlarge output buffer
          RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
        }
      }
    }
    compressTime_ -= flushTime;
    flushTime_ += flushTime;
    return arrow::Status::OK();
  }

  arrow::Status Flush() override {
    ARROW_RETURN_IF(!isOpen_, arrow::Status::Invalid("Stream is closed"));

    if (freshCompressor_) {
      // No data written, no need to flush
      return arrow::Status::OK();
    }

    RETURN_NOT_OK(FinalizeCompression());
    ARROW_ASSIGN_OR_RAISE(compressor_, codec_->MakeCompressor());
    freshCompressor_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Close() override {
    if (isOpen_) {
      isOpen_ = false;
      if (!freshCompressor_) {
        RETURN_NOT_OK(FinalizeCompression());
      }
      // Do not close the underlying stream, it is the caller's responsibility.
    }
    return arrow::Status::OK();
  }

  arrow::Status Abort() override {
    if (isOpen_) {
      isOpen_ = false;
      return raw_->Abort();
    }
    return arrow::Status::OK();
  }

  bool closed() const override {
    return !isOpen_;
  }

  int64_t compressTime() const {
    return compressTime_;
  }

  int64_t flushTime() const {
    return flushTime_;
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ShuffleCompressedOutputStream);

  ShuffleCompressedOutputStream(
      arrow::util::Codec* codec,
      int32_t compressionBufferSize,
      const std::shared_ptr<OutputStream>& raw,
      arrow::MemoryPool* pool)
      : codec_(codec), compressionBufferSize_(compressionBufferSize), raw_(raw), pool_(pool) {}

  arrow::Status Init(arrow::util::Codec* codec) {
    ARROW_ASSIGN_OR_RAISE(compressor_, codec->MakeCompressor());
    ARROW_ASSIGN_OR_RAISE(compressed_, AllocateResizableBuffer(compressionBufferSize_, pool_));
    compressedPos_ = 0;
    isOpen_ = true;
    return arrow::Status::OK();
  }

  arrow::Status FlushCompressed(int64_t& flushTime) {
    if (compressedPos_ > 0) {
      ScopedTimer timer(&flushTime);
      RETURN_NOT_OK(raw_->Write(compressed_->data(), compressedPos_));
      compressedPos_ = 0;
    }
    return arrow::Status::OK();
  }

  arrow::Status FinalizeCompression() {
    int64_t flushTime = 0;
    {
      ScopedTimer timer(&compressTime_);
      while (true) {
        // Try to end compressor
        int64_t output_len = compressed_->size() - compressedPos_;
        uint8_t* output = compressed_->mutable_data() + compressedPos_;
        ARROW_ASSIGN_OR_RAISE(auto result, compressor_->End(output_len, output));
        compressedPos_ += result.bytes_written;

        // Flush compressed output
        RETURN_NOT_OK(FlushCompressed(flushTime));

        if (result.should_retry) {
          // Need to enlarge output buffer
          RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
        } else {
          // Done
          break;
        }
      }
    }
    compressTime_ -= flushTime;
    flushTime_ += flushTime;
    return arrow::Status::OK();
  }

  arrow::util::Codec* codec_;
  int32_t compressionBufferSize_;
  std::shared_ptr<OutputStream> raw_;
  arrow::MemoryPool* pool_;

  bool freshCompressor_{true};
  std::shared_ptr<arrow::util::Compressor> compressor_;
  std::shared_ptr<arrow::ResizableBuffer> compressed_;

  bool isOpen_{false};
  int64_t compressedPos_{0};
  // Total number of bytes compressed
  int64_t totalPos_{0};

  // Time spent on compressing data. Flushing the compressed data into raw_ stream is not included.
  int64_t compressTime_{0};
  int64_t flushTime_{0};
};

class CompressedInputStream : public arrow::io::InputStream {
 public:
  static arrow::Result<std::shared_ptr<CompressedInputStream>>
  Make(arrow::util::Codec* codec, const std::shared_ptr<InputStream>& raw, arrow::MemoryPool* pool) {
    std::shared_ptr<CompressedInputStream> res(new CompressedInputStream(raw, pool));
    RETURN_NOT_OK(res->Init(codec));
    return res;
  }

  arrow::Status Init(arrow::util::Codec* codec) {
    ARROW_ASSIGN_OR_RAISE(decompressor_, codec->MakeDecompressor());
    fresh_decompressor_ = true;
    return arrow::Status::OK();
  }

  arrow::Status Close() override {
    if (is_open_) {
      is_open_ = false;
      return raw_->Close();
    } else {
      return arrow::Status::OK();
    }
  }

  arrow::Status Abort() override {
    if (is_open_) {
      is_open_ = false;
      return raw_->Abort();
    } else {
      return arrow::Status::OK();
    }
  }

  bool closed() const override {
    return !is_open_;
  }

  arrow::Result<int64_t> Tell() const override {
    return total_pos_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    ScopedTimer timer(&decompressWallTime_);
    auto out_data = reinterpret_cast<uint8_t*>(out);

    int64_t total_read = 0;
    bool decompressor_has_data = true;

    while (nbytes - total_read > 0 && decompressor_has_data) {
      total_read += ReadFromDecompressed(nbytes - total_read, out_data + total_read);

      if (nbytes == total_read) {
        break;
      }

      // At this point, no more decompressed data remains, so we need to
      // decompress more
      RETURN_NOT_OK(RefillDecompressed(&decompressor_has_data));
    }

    total_pos_ += total_read;
    return total_read;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes, pool_));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buf->mutable_data()));
    RETURN_NOT_OK(buf->Resize(bytes_read));
    return std::move(buf);
  }

  int64_t decompressTime() const {
    return decompressWallTime_ - blockingTime_;
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(CompressedInputStream);

  CompressedInputStream() = default;

  CompressedInputStream(const std::shared_ptr<InputStream>& raw, arrow::MemoryPool* pool)
      : raw_(raw), pool_(pool), is_open_(true), compressed_pos_(0), decompressed_pos_(0), total_pos_(0) {}

  // Read compressed data if necessary
  arrow::Status EnsureCompressedData() {
    int64_t compressed_avail = compressed_ ? compressed_->size() - compressed_pos_ : 0;
    if (compressed_avail == 0) {
      ScopedTimer timer(&blockingTime_);
      // No compressed data available, read a full chunk
      ARROW_ASSIGN_OR_RAISE(compressed_, raw_->Read(kChunkSize));
      compressed_pos_ = 0;
    }
    return arrow::Status::OK();
  }

  // Decompress some data from the compressed_ buffer.
  // Call this function only if the decompressed_ buffer is empty.
  arrow::Status DecompressData() {
    GLUTEN_CHECK(compressed_->data() != nullptr, "Compressed data is null");

    int64_t decompress_size = kDecompressSize;

    while (true) {
      ARROW_ASSIGN_OR_RAISE(decompressed_, AllocateResizableBuffer(decompress_size, pool_));
      decompressed_pos_ = 0;

      int64_t input_len = compressed_->size() - compressed_pos_;
      const uint8_t* input = compressed_->data() + compressed_pos_;
      int64_t output_len = decompressed_->size();
      uint8_t* output = decompressed_->mutable_data();

      ARROW_ASSIGN_OR_RAISE(auto result, decompressor_->Decompress(input_len, input, output_len, output));
      compressed_pos_ += result.bytes_read;
      if (result.bytes_read > 0) {
        fresh_decompressor_ = false;
      }
      if (result.bytes_written > 0 || !result.need_more_output || input_len == 0) {
        // StdMemoryAllocator does not allow resize to 0.
        if (result.bytes_written > 0) {
          RETURN_NOT_OK(decompressed_->Resize(result.bytes_written));
        } else {
          decompressed_.reset();
        }
        break;
      }
      GLUTEN_CHECK(result.bytes_written == 0, "Decompressor should return 0 bytes written");
      // Need to enlarge output buffer
      decompress_size *= 2;
    }
    return arrow::Status::OK();
  }

  // Read a given number of bytes from the decompressed_ buffer.
  int64_t ReadFromDecompressed(int64_t nbytes, uint8_t* out) {
    int64_t readable = decompressed_ ? (decompressed_->size() - decompressed_pos_) : 0;
    int64_t read_bytes = std::min(readable, nbytes);

    if (read_bytes > 0) {
      memcpy(out, decompressed_->data() + decompressed_pos_, read_bytes);
      decompressed_pos_ += read_bytes;

      if (decompressed_pos_ == decompressed_->size()) {
        // Decompressed data is exhausted, release buffer
        decompressed_.reset();
      }
    }

    return read_bytes;
  }

  // Try to feed more data into the decompressed_ buffer.
  arrow::Status RefillDecompressed(bool* has_data) {
    // First try to read data from the decompressor
    if (compressed_ && compressed_->size() != 0) {
      if (decompressor_->IsFinished()) {
        // We just went over the end of a previous compressed stream.
        RETURN_NOT_OK(decompressor_->Reset());
        fresh_decompressor_ = true;
      }
      RETURN_NOT_OK(DecompressData());
    }
    if (!decompressed_ || decompressed_->size() == 0) {
      // Got nothing, need to read more compressed data
      RETURN_NOT_OK(EnsureCompressedData());
      if (compressed_pos_ == compressed_->size()) {
        // No more data to decompress
        if (!fresh_decompressor_ && !decompressor_->IsFinished()) {
          return arrow::Status::IOError("Truncated compressed stream");
        }
        *has_data = false;
        return arrow::Status::OK();
      }
      RETURN_NOT_OK(DecompressData());
    }
    *has_data = true;
    return arrow::Status::OK();
  }

  std::shared_ptr<InputStream> raw() const {
    return raw_;
  }

  // Read 64 KB compressed data at a time
  static const int64_t kChunkSize = 64 * 1024;
  // Decompress 1 MB at a time
  static const int64_t kDecompressSize = 1024 * 1024;

  std::shared_ptr<InputStream> raw_;
  arrow::MemoryPool* pool_;

  std::shared_ptr<arrow::util::Decompressor> decompressor_;
  std::shared_ptr<arrow::Buffer> compressed_;

  bool is_open_;
  // Position in compressed buffer
  int64_t compressed_pos_;
  std::shared_ptr<arrow::ResizableBuffer> decompressed_;
  // Position in decompressed buffer
  int64_t decompressed_pos_;
  // True if the decompressor hasn't read any data yet.
  bool fresh_decompressor_;
  // Total number of bytes decompressed
  int64_t total_pos_;

  int64_t blockingTime_{0};
  int64_t decompressWallTime_{0};
};

} // namespace gluten

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

#include "shuffle/Utils.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <string>
#include <vector>

using namespace gluten;

namespace {

struct MadviseCall {
  uint8_t* addr;
  size_t length;
  int advice;
};

std::mutex madviseMutex;
std::vector<MadviseCall> madviseCalls;
std::atomic<bool> madviseRecording{false};

int realMadvise(void* addr, size_t length, int advice) {
  return static_cast<int>(::syscall(SYS_madvise, addr, length, advice));
}

} // namespace

// Interpose madvise(2) so the test can observe the ranges advised by
// MmapFileStream. The recording flag ensures only calls issued while a
// MadviseRecorder is alive are captured.
extern "C" int madvise(void* addr, size_t length, int advice) {
  if (madviseRecording.load(std::memory_order_relaxed)) {
    std::lock_guard<std::mutex> lock(madviseMutex);
    madviseCalls.push_back({static_cast<uint8_t*>(addr), length, advice});
  }
  return realMadvise(addr, length, advice);
}

namespace {

class MadviseRecorder {
 public:
  MadviseRecorder() {
    std::lock_guard<std::mutex> lock(madviseMutex);
    madviseCalls.clear();
    madviseRecording.store(true, std::memory_order_relaxed);
  }

  ~MadviseRecorder() {
    madviseRecording.store(false, std::memory_order_relaxed);
  }

  std::vector<MadviseCall> callsWithAdvice(int advice) const {
    std::lock_guard<std::mutex> lock(madviseMutex);
    std::vector<MadviseCall> result;
    for (const auto& call : madviseCalls) {
      if (call.advice == advice) {
        result.push_back(call);
      }
    }
    return result;
  }

  std::vector<MadviseCall> allCalls() const {
    std::lock_guard<std::mutex> lock(madviseMutex);
    return madviseCalls;
  }
};

class MmapFileStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = (std::filesystem::temp_directory_path() / ("mmap-file-stream-test-" + std::to_string(::getpid()) + ".bin"))
                .string();
  }

  void TearDown() override {
    std::filesystem::remove(path_);
  }

  static std::vector<uint8_t> makePattern(int64_t size) {
    std::vector<uint8_t> data(size);
    std::mt19937 gen(42);
    for (auto& byte : data) {
      byte = static_cast<uint8_t>(gen());
    }
    return data;
  }

  void writePattern(const std::vector<uint8_t>& data) {
    std::ofstream file(path_, std::ios::binary | std::ios::trunc);
    file.write(reinterpret_cast<const char*>(data.data()), data.size());
    ASSERT_TRUE(file.good());
  }

  std::shared_ptr<MmapFileStream> openStream(uint64_t prefetchSize) {
    auto result = MmapFileStream::open(path_, prefetchSize);
    if (!result.ok()) {
      ADD_FAILURE() << "Failed to open " << path_ << ": " << result.status().ToString();
      return nullptr;
    }
    return result.ValueOrDie();
  }

  std::string path_;
};

// Regression test for the fetch length calculation in willNeed(). The read
// position can lag behind the prefetch frontier (posFetch_) after lengths are
// rounded up to the prefetch size. When that happens, computing the fetch
// length from the read position instead of the frontier makes the advised
// range [posFetch_, posFetch_ + fetchLen) overrun the end of the mapping.
TEST_F(MmapFileStreamTest, willNeedStaysWithinMapping) {
  constexpr int64_t kFileSize = 10 << 20;
  constexpr uint64_t kPrefetchSize = 4 << 20;
  constexpr int64_t kFirstRead = 1 << 20;

  auto pattern = makePattern(kFileSize);
  writePattern(pattern);
  auto stream = openStream(kPrefetchSize);
  ASSERT_NE(stream, nullptr);

  MadviseRecorder recorder;
  // The first read rounds the fetch length up to the prefetch size, so the
  // prefetch frontier ends up 3MB ahead of the read position.
  auto first = stream->Read(kFirstRead);
  ASSERT_TRUE(first.ok());
  ASSERT_EQ(kFirstRead, first.ValueOrDie()->size());
  // The second read needs the rest of the file, starting from the frontier.
  auto second = stream->Read(kFileSize - kFirstRead);
  ASSERT_TRUE(second.ok());
  ASSERT_EQ(kFileSize - kFirstRead, second.ValueOrDie()->size());

  auto willNeed = recorder.callsWithAdvice(MADV_WILLNEED);
  ASSERT_FALSE(willNeed.empty());

  // The mapping starts at the address of the first advised range.
  const uint8_t* base = willNeed.front().addr;
  for (const auto& call : willNeed) {
    // Advising beyond data_ + size_ relies on the kernel clamping the range at
    // the end of the mapping and may touch whatever mapping follows it.
    ASSERT_GE(call.addr, base);
    ASSERT_LE(call.addr + call.length, base + kFileSize)
        << "madvise range [" << (call.addr - base) << ", " << (call.addr - base) + call.length
        << ") overruns the mapping of size " << kFileSize;
  }

  // The first read of 1MB is rounded up to the prefetch size.
  ASSERT_EQ(2, willNeed.size());
  EXPECT_EQ(base, willNeed[0].addr);
  EXPECT_EQ(kPrefetchSize, willNeed[0].length);
  // The second read needs 6MB from the frontier (4MB): rounded up to the
  // prefetch size would be 8MB, but it is clamped to the end of the file.
  EXPECT_EQ(base + kPrefetchSize, willNeed[1].addr);
  EXPECT_EQ(kFileSize - kPrefetchSize, willNeed[1].length);

  EXPECT_EQ(0, memcmp(pattern.data(), first.ValueOrDie()->data(), kFirstRead));
  EXPECT_EQ(0, memcmp(pattern.data() + kFirstRead, second.ValueOrDie()->data(), kFileSize - kFirstRead));
  ASSERT_TRUE(stream->Close().ok());
}

// Read a file whose size is not a multiple of the prefetch size in fixed-size
// chunks until EOF, and make sure every madvise range stays within the mapping.
TEST_F(MmapFileStreamTest, chunkedReadsStaysWithinMapping) {
  constexpr int64_t kFileSize = (5 << 20) + 12345;
  constexpr uint64_t kPrefetchSize = 2 << 20;
  constexpr int64_t kChunkSize = 300 << 10;

  auto pattern = makePattern(kFileSize);
  writePattern(pattern);
  auto stream = openStream(kPrefetchSize);
  ASSERT_NE(stream, nullptr);

  MadviseRecorder recorder;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  int64_t totalRead = 0;
  while (totalRead < kFileSize) {
    auto buffer = stream->Read(kChunkSize);
    ASSERT_TRUE(buffer.ok());
    ASSERT_GT(buffer.ValueOrDie()->size(), 0);
    totalRead += buffer.ValueOrDie()->size();
    buffers.push_back(buffer.ValueOrDie());
  }
  ASSERT_EQ(kFileSize, totalRead);
  // A read at EOF returns an empty buffer.
  auto eof = stream->Read(kChunkSize);
  ASSERT_TRUE(eof.ok());
  ASSERT_EQ(0, eof.ValueOrDie()->size());

  // Every advised range, WILLNEED or DONTNEED, must lie within the mapping.
  auto willNeed = recorder.callsWithAdvice(MADV_WILLNEED);
  ASSERT_FALSE(willNeed.empty());
  const uint8_t* base = willNeed.front().addr;
  for (const auto& call : recorder.allCalls()) {
    ASSERT_GE(call.addr, base);
    ASSERT_LE(call.addr + call.length, base + kFileSize);
  }
  for (const auto& call : willNeed) {
    ASSERT_GE(call.addr, base);
    ASSERT_LE(call.addr + call.length, base + kFileSize)
        << "madvise range [" << (call.addr - base) << ", " << (call.addr - base) + call.length
        << ") overruns the mapping of size " << kFileSize;
  }

  // The prefetched ranges must be contiguous and cover everything that was
  // read, i.e. the frontier never skips over data still to be consumed.
  for (size_t i = 1; i < willNeed.size(); ++i) {
    ASSERT_EQ(willNeed[i - 1].addr + willNeed[i - 1].length, willNeed[i].addr);
  }
  ASSERT_LE(base + totalRead, willNeed.back().addr + willNeed.back().length);

  int64_t offset = 0;
  for (const auto& buffer : buffers) {
    EXPECT_EQ(0, memcmp(pattern.data() + offset, buffer->data(), buffer->size()));
    offset += buffer->size();
  }
  ASSERT_TRUE(stream->Close().ok());
}

// The memcpy-style Read() advances the read position without prefetching, so
// the position can jump ahead of the prefetch frontier. The fetch length must
// still be clamped against the frontier (not the read position), otherwise
// the frontier can land on a non-page-aligned offset inside the file and
// later madvise() calls receive an unaligned address, which the kernel
// rejects with EINVAL, silently dropping the prefetch.
TEST_F(MmapFileStreamTest, positionJumpKeepsAddressesAligned) {
  // File size and jump offset are chosen so that clamping against pos_
  // produces a non-page-aligned fetch length.
  constexpr int64_t kFileSize = (15 << 20) + 123;
  constexpr uint64_t kPrefetchSize = 4 << 20;
  constexpr int64_t kJumpSize = 12 << 20;

  auto pattern = makePattern(kFileSize);
  writePattern(pattern);
  auto stream = openStream(kPrefetchSize);
  ASSERT_NE(stream, nullptr);

  MadviseRecorder recorder;
  // Advance the read position without prefetching.
  std::vector<uint8_t> jumped(kJumpSize);
  auto jumpedRead = stream->Read(kJumpSize, jumped.data());
  ASSERT_TRUE(jumpedRead.ok());
  ASSERT_EQ(kJumpSize, jumpedRead.ValueOrDie());
  EXPECT_EQ(0, memcmp(pattern.data(), jumped.data(), kJumpSize));

  // Continue with buffer reads to the end of the file.
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  int64_t totalRead = kJumpSize;
  while (totalRead < kFileSize) {
    auto buffer = stream->Read(1 << 20);
    ASSERT_TRUE(buffer.ok());
    ASSERT_GT(buffer.ValueOrDie()->size(), 0);
    totalRead += buffer.ValueOrDie()->size();
    buffers.push_back(buffer.ValueOrDie());
  }
  ASSERT_EQ(kFileSize, totalRead);

  auto willNeed = recorder.callsWithAdvice(MADV_WILLNEED);
  ASSERT_FALSE(willNeed.empty());
  const uint8_t* base = willNeed.front().addr;
  const int64_t pageSize = ::sysconf(_SC_PAGESIZE);
  for (const auto& call : willNeed) {
    ASSERT_GE(call.addr, base);
    ASSERT_LE(call.addr + call.length, base + kFileSize)
        << "madvise range [" << (call.addr - base) << ", " << (call.addr - base) + call.length
        << ") overruns the mapping of size " << kFileSize;
    ASSERT_EQ(0, reinterpret_cast<intptr_t>(call.addr) % pageSize)
        << "madvise address is not page-aligned: offset " << (call.addr - base);
  }

  int64_t offset = kJumpSize;
  for (const auto& buffer : buffers) {
    EXPECT_EQ(0, memcmp(pattern.data() + offset, buffer->data(), buffer->size()));
    offset += buffer->size();
  }
  ASSERT_TRUE(stream->Close().ok());
}

// Randomized stress test: for random combinations of file size, prefetch size
// and read size, every madvise range must stay within the mapping and the
// prefetched ranges must contiguously cover all data that was read.
TEST_F(MmapFileStreamTest, randomizedReadsStaysWithinMapping) {
  constexpr int32_t kIterations = 25;
  for (int32_t iter = 0; iter < kIterations; ++iter) {
    std::mt19937 gen(iter);
    const int64_t fileSize = 1 + gen() % (16 << 20);
    const uint64_t prefetchSize = 4096 * (1 + gen() % 1024);
    const int64_t readSize = 1 + gen() % (1 << 20);

    auto pattern = makePattern(fileSize);
    writePattern(pattern);
    auto stream = openStream(prefetchSize);
    ASSERT_NE(stream, nullptr);

    MadviseRecorder recorder;
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    int64_t totalRead = 0;
    while (totalRead < fileSize) {
      auto buffer = stream->Read(readSize);
      ASSERT_TRUE(buffer.ok());
      ASSERT_GT(buffer.ValueOrDie()->size(), 0);
      totalRead += buffer.ValueOrDie()->size();
      buffers.push_back(buffer.ValueOrDie());
    }
    ASSERT_EQ(fileSize, totalRead);

    auto willNeed = recorder.callsWithAdvice(MADV_WILLNEED);
    ASSERT_FALSE(willNeed.empty());
    const uint8_t* base = willNeed.front().addr;
    const int64_t pageSize = ::sysconf(_SC_PAGESIZE);
    for (const auto& call : recorder.allCalls()) {
      ASSERT_GE(call.addr, base);
      ASSERT_LE(call.addr + call.length, base + fileSize)
          << "iteration " << iter << ": madvise range [" << (call.addr - base) << ", "
          << (call.addr - base) + call.length << ") overruns the mapping of size " << fileSize;
      ASSERT_EQ(0, reinterpret_cast<intptr_t>(call.addr) % pageSize)
          << "iteration " << iter << ": madvise address is not page-aligned: offset " << (call.addr - base);
    }
    // Prefetched ranges are contiguous and cover all read data.
    ASSERT_EQ(base, willNeed.front().addr);
    for (size_t i = 1; i < willNeed.size(); ++i) {
      ASSERT_EQ(willNeed[i - 1].addr + willNeed[i - 1].length, willNeed[i].addr) << "iteration " << iter;
    }
    ASSERT_GE(willNeed.back().addr + willNeed.back().length, base + totalRead) << "iteration " << iter;

    int64_t offset = 0;
    for (const auto& buffer : buffers) {
      EXPECT_EQ(0, memcmp(pattern.data() + offset, buffer->data(), buffer->size())) << "iteration " << iter;
      offset += buffer->size();
    }
    ASSERT_TRUE(stream->Close().ok());
  }
}

} // namespace

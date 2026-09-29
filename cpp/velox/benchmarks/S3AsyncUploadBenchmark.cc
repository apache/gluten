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

#include <benchmark/benchmark.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <fmt/core.h>

#include "filesystem/GlutenS3FileSystem.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"

namespace gluten {
namespace {

namespace velox = facebook::velox;
namespace filesystems = facebook::velox::filesystems;
namespace memory = facebook::velox::memory;

constexpr std::string_view kDefaultLogLocation{"/tmp/gluten-s3-benchmark"};
constexpr std::string_view kDefaultMinPartSize{"5MB"};

std::optional<std::string> getEnv(std::string_view name) {
  const auto* value = std::getenv(std::string(name).c_str());
  if (value == nullptr || value[0] == '\0') {
    return std::nullopt;
  }
  return value;
}

std::string getEnvOrDefault(std::string_view name, std::string_view fallback) {
  return getEnv(name).value_or(std::string(fallback));
}

std::optional<std::string> firstEnv(std::initializer_list<std::string_view> names) {
  for (const auto name : names) {
    auto value = getEnv(name);
    if (value.has_value()) {
      return value;
    }
  }
  return std::nullopt;
}

bool startsWith(std::string_view value, std::string_view prefix) {
  return value.substr(0, prefix.size()) == prefix;
}

bool contains(std::string_view value, std::string_view pattern) {
  return value.find(pattern) != std::string_view::npos;
}

std::string boolString(bool value) {
  return value ? "true" : "false";
}

bool defaultPathStyleAccess(const std::optional<std::string>& endpoint) {
  if (!endpoint.has_value()) {
    return false;
  }
  return contains(endpoint.value(), "localhost") || contains(endpoint.value(), "127.0.0.1") ||
      !contains(endpoint.value(), "amazonaws.com");
}

std::optional<std::string> inferAwsRegion(const std::optional<std::string>& endpoint) {
  if (!endpoint.has_value()) {
    return std::nullopt;
  }

  const auto& value = endpoint.value();
  const auto s3Prefix = value.find("s3.");
  const auto awsSuffix = value.find(".amazonaws.com");
  if (s3Prefix == std::string::npos || awsSuffix == std::string::npos || awsSuffix <= s3Prefix + 3) {
    return std::nullopt;
  }
  return value.substr(s3Prefix + 3, awsSuffix - s3Prefix - 3);
}

std::string s3ConfigKey(filesystems::S3Config::Keys key) {
  return filesystems::S3Config::baseConfigKey(key);
}

class S3AsyncUploadBenchmark {
 public:
  S3AsyncUploadBenchmark() {
    bucket_ = firstEnv({"GLUTEN_S3_BENCH_BUCKET", "S3_BUCKET"}).value_or("");
    VELOX_USER_CHECK(!bucket_.empty(), "Set GLUTEN_S3_BENCH_BUCKET or S3_BUCKET before running this benchmark.");

    objectPrefix_ = getEnvOrDefault("GLUTEN_S3_BENCH_PREFIX", "gluten-s3-benchmark");
    minPartSize_ = getEnvOrDefault("GLUTEN_S3_BENCH_MIN_PART_SIZE", kDefaultMinPartSize);
    maxConcurrentUploadNum_ = getEnvOrDefault("GLUTEN_S3_BENCH_MAX_CONCURRENCY", "4");
    uploadThreads_ = getEnvOrDefault("GLUTEN_S3_BENCH_UPLOAD_THREADS", "16");
    runId_ = fmt::format("{}", std::chrono::steady_clock::now().time_since_epoch().count());

    filesystems::initializeS3(
        getEnvOrDefault("GLUTEN_S3_BENCH_LOG_LEVEL", "FATAL"),
        getEnv("GLUTEN_S3_BENCH_LOG_LOCATION").value_or(std::string(kDefaultLogLocation)));
  }

  ~S3AsyncUploadBenchmark() {
    filesystems::finalizeS3();
  }

  void run(::benchmark::State& state, std::string_view name, bool enableUploadPartAsync, int32_t sizeMiB) {
    state.PauseTiming();
    const auto sequence = sequence_.fetch_add(1);
    const auto key = fmt::format("{}/{}_{}MiB_{}_{}.bin", objectPrefix_, name, sizeMiB, runId_, sequence);
    const auto s3File = fmt::format("s3://{}/{}", bucket_, key);
    auto config = createConfig(enableUploadPartAsync);
    GlutenS3FileSystem s3fs(bucket_, config);
    std::string data(1024 * 1024, 'a');

    state.ResumeTiming();
    auto pool = memory::memoryManager()->addLeafPool(fmt::format("S3AsyncUploadBenchmark-{}", sequence));
    filesystems::FileOptions options;
    options.pool = pool.get();
    auto writeFile = s3fs.openFileForWrite(s3File, options);
    for (int32_t i = 0; i < sizeMiB; ++i) {
      writeFile->append(data);
    }
    writeFile->close();
    ::benchmark::DoNotOptimize(writeFile->size());
    VELOX_CHECK_EQ(writeFile->size(), static_cast<uint64_t>(sizeMiB) * 1024 * 1024);
  }

 private:
  std::shared_ptr<const velox::config::ConfigBase> createConfig(bool enableUploadPartAsync) const {
    std::unordered_map<std::string, std::string> values;
    const auto endpoint = firstEnv({"GLUTEN_S3_BENCH_ENDPOINT", "AWS_ENDPOINT"});
    if (endpoint.has_value()) {
      values[s3ConfigKey(filesystems::S3Config::Keys::kEndpoint)] = endpoint.value();
    }

    if (auto accessKey = firstEnv({"GLUTEN_S3_BENCH_ACCESS_KEY", "AWS_ACCESS_KEY_ID"})) {
      values[s3ConfigKey(filesystems::S3Config::Keys::kAccessKey)] = accessKey.value();
    }
    if (auto secretKey = firstEnv({"GLUTEN_S3_BENCH_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"})) {
      values[s3ConfigKey(filesystems::S3Config::Keys::kSecretKey)] = secretKey.value();
    }

    auto region = firstEnv({"GLUTEN_S3_BENCH_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"});
    if (!region.has_value()) {
      region = inferAwsRegion(endpoint);
    }
    if (region.has_value()) {
      values[s3ConfigKey(filesystems::S3Config::Keys::kEndpointRegion)] = region.value();
    }

    values[s3ConfigKey(filesystems::S3Config::Keys::kPathStyleAccess)] =
        getEnv("GLUTEN_S3_BENCH_PATH_STYLE_ACCESS").value_or(boolString(defaultPathStyleAccess(endpoint)));
    values[s3ConfigKey(filesystems::S3Config::Keys::kSSLEnabled)] =
        getEnv("GLUTEN_S3_BENCH_SSL_ENABLED")
            .value_or(boolString(!endpoint.has_value() || !startsWith(endpoint.value(), "http://")));
    values[s3ConfigKey(filesystems::S3Config::Keys::kMultipartMinPartSize)] = minPartSize_;
    values["hive.s3.part-upload-async"] = enableUploadPartAsync ? "true" : "false";
    values["hive.s3.max-concurrent-upload-num"] = maxConcurrentUploadNum_;
    values["hive.s3.upload-threads"] = uploadThreads_;

    return std::make_shared<velox::config::ConfigBase>(std::move(values));
  }

  std::string bucket_;
  std::string objectPrefix_;
  std::string minPartSize_;
  std::string maxConcurrentUploadNum_;
  std::string uploadThreads_;
  std::string runId_;
  std::atomic<uint64_t> sequence_{0};
};

std::unique_ptr<S3AsyncUploadBenchmark> s3Benchmark;

void runS3AsyncUploadBenchmark(::benchmark::State& state, bool enableUploadPartAsync, int32_t sizeMiB) {
  const auto name = enableUploadPartAsync ? "async_upload" : "sync_upload";
  for (auto _ : state) {
    s3Benchmark->run(state, name, enableUploadPartAsync, sizeMiB);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * sizeMiB * 1024 * 1024);
}

#define REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(size)                                                                    \
  BENCHMARK_CAPTURE(runS3AsyncUploadBenchmark, sync_upload_##size##M, false, size)->Unit(::benchmark::kMillisecond); \
  BENCHMARK_CAPTURE(runS3AsyncUploadBenchmark, async_upload_##size##M, true, size)->Unit(::benchmark::kMillisecond)

REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(4);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(8);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(16);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(32);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(64);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(128);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(256);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(512);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(1024);
REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS(2048);

#undef REGISTER_S3_ASYNC_UPLOAD_BENCHMARKS

} // namespace

void initializeS3AsyncUploadBenchmark() {
  s3Benchmark = std::make_unique<S3AsyncUploadBenchmark>();
}

void finalizeS3AsyncUploadBenchmark() {
  s3Benchmark.reset();
}

} // namespace gluten

int main(int argc, char** argv) {
  facebook::velox::memory::MemoryManager::initialize(facebook::velox::memory::MemoryManager::Options{});
  gluten::initializeS3AsyncUploadBenchmark();
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  gluten::finalizeS3AsyncUploadBenchmark();
  return 0;
}

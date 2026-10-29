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

#include <aws/core/Aws.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <chrono>
#include <iostream>
#include <thread>

#include "filesystem/GlutenS3FileSystem.h"
#include "utils/ConfigExtractor.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"

namespace velox = facebook::velox;
namespace fs = velox::filesystems;
using Properties = std::unordered_map<std::string, std::string>;

// Complete the prefetch on another thread before the consumer runs. This
// deterministically exercises the valid scheduling order where a background
// read has already failed when the foreground asks for its buffer.
class DrainingPrefetchExecutor final : public folly::Executor {
 public:
  void add(folly::Func func) override {
    std::thread worker([func = std::move(func)]() mutable {
      try {
        func();
      } catch (const std::exception& e) {
        std::cerr << "prefetch_error=" << e.what() << std::endl;
      }
    });
    worker.join();
  }
};

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "--schedule") {
    auto spark = std::make_shared<velox::config::ConfigBase>(Properties{{"spark.hadoop.fs.s3a.retry.limit", "20"}});
    const auto hive = gluten::createHiveConnectorConfig(spark, gluten::FileSystemType::kS3);
    const fs::S3Config config("lloc", hive);
    Aws::Client::DefaultRetryStrategy retry(config.maxAttempts().value());
    Aws::Client::AWSError<Aws::Client::CoreErrors> error(Aws::Client::CoreErrors::NETWORK_CONNECTION, true);
    int64_t delay = 0;
    for (int attempt = 0; retry.ShouldRetry(error, attempt); ++attempt) {
      const auto ms = retry.CalculateDelayBeforeNextRetry(error, attempt);
      delay += ms;
      std::cout << "retry=" << attempt + 1 << " delay_ms=" << ms << '\n';
    }
    const auto connectMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                               velox::config::toDuration(config.connectTimeout().value()))
                               .count();
    std::cout << "mode=" << config.retryMode().value() << " requests=" << retry.GetMaxAttempts()
              << " backoff_ms=" << delay << " connect_timeout_ms=" << connectMs
              << " total_ms=" << delay + retry.GetMaxAttempts() * connectMs << std::endl;
    return 0;
  }
  if (argc != 5) {
    std::cerr << "Usage: reproduce ENDPOINT FILE_SIZE MODE ATTEMPTS | --schedule\n";
    return 2;
  }

  Aws::SDKOptions sdk;
  Aws::InitAPI(sdk);
  fs::initializeS3();
  velox::memory::MemoryManager::initialize({});
  {
    const std::string endpoint(argv[1]);
    const int64_t size = std::stoll(argv[2]);
    const std::string mode(argv[3]);
    const int attempts = std::stoi(argv[4]);
    auto config = std::make_shared<velox::config::ConfigBase>(Properties{
        {"s3.endpoint", endpoint},
        {"s3.endpoint.region", "us-east-1"},
        {"s3.aws-access-key", "local-test"},
        {"s3.aws-secret-key", "local-test"},
        {"s3.path-style-access", "true"},
        {"s3.ssl.enabled", "false"},
        {"s3.use-proxy-from-env", "false"},
        {"s3.aws-imds-enabled", "false"},
        {"s3.connect-timeout", "100ms"},
        {"s3.socket-timeout", "1s"},
        {"s3.retry-mode", "legacy"},
        {"s3.max-attempts", "2"}});
    gluten::GlutenS3FileSystem filesystem("lloc", config);
    auto pool = velox::memory::memoryManager()->addLeafPool();
    fs::FileOptions options;
    // Isolate GETs using a known file length, as when FileOptions supplies it
    // or a previously opened file handle retains the length from HEAD.
    options.fileSize = size;
    if (mode != "ordinary" && mode != "direct") {
      options.tokenProvider = gluten::GlutenS3TokenProvider::create({Properties{
          {"location", "s3://lloc/wopdadjwkoq/21"},
          {"s3.access-key-id", "local-test"},
          {"s3.secret-access-key", "local-test"},
          {"s3.session-token", "local-session-token"},
          {"s3.endpoint", endpoint},
          {"client.region", "us-east-1"},
          {"s3.path-style-access", "true"}}});
    }
    velox::memory::MmapAllocator::Options cacheOptions;
    cacheOptions.capacity = 64 << 20;
    velox::memory::MmapAllocator allocator(cacheOptions);
    auto cache = mode == "cached" ? velox::cache::AsyncDataCache::create(&allocator) : nullptr;
    const velox::StringIdLease fileId(velox::fileIds(), "s3://lloc/wopdadjwkoq/21/1mb_valid.parquet");
    const velox::StringIdLease groupId(velox::fileIds(), "s3://lloc/wopdadjwkoq/21");
    DrainingPrefetchExecutor prefetchExecutor;
    for (int attempt = 0; attempt < attempts; ++attempt) {
      const auto start = std::chrono::steady_clock::now();
      try {
        std::shared_ptr<velox::ReadFile> file =
            filesystem.openFileForRead("s3://lloc/wopdadjwkoq/21/1mb_valid.parquet", options);
        if (mode == "raw") {
          const auto bytes = file->pread(0, size);
          const bool magic = bytes.substr(bytes.size() - 4) == "PAR1";
          std::cout << "attempt=" << attempt << " returned_bytes=" << bytes.size() << " footer_magic=" << magic
                    << std::endl;
        } else {
          velox::dwio::common::ReaderOptions readerOptions(pool.get());
          readerOptions.setCacheable(true);
          std::unique_ptr<velox::dwio::common::BufferedInput> input;
          if (mode == "direct") {
            // Attempt zero wins the foreground/prefetch race. Later attempts
            // let prefetch finish first. No data is carried between attempts.
            input = std::make_unique<velox::dwio::common::DirectBufferedInput>(
                file,
                velox::dwio::common::MetricsLog::voidLog(),
                fileId,
                nullptr,
                groupId,
                std::make_shared<velox::dwio::common::IoStatistics>(),
                std::make_shared<velox::IoStats>(),
                attempt == 0 ? nullptr : &prefetchExecutor,
                readerOptions);
          } else if (cache) {
            input = std::make_unique<velox::dwio::common::CachedBufferedInput>(
                file,
                velox::dwio::common::MetricsLog::voidLog(),
                fileId,
                cache.get(),
                nullptr,
                groupId,
                std::make_shared<velox::dwio::common::IoStatistics>(),
                std::make_shared<velox::IoStats>(),
                nullptr,
                readerOptions);
          } else {
            input = std::make_unique<velox::dwio::common::BufferedInput>(file, *pool);
          }
          velox::parquet::ParquetReader reader(std::move(input), readerOptions);
          std::cout << "attempt=" << attempt << " rows=" << reader.numberOfRows().value() << std::endl;
        }
      } catch (const std::exception& e) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        std::cout << "attempt=" << attempt << " elapsed_ms=" << ms.count() << " error=" << e.what() << std::endl;
      }
    }
    if (cache) {
      cache->shutdown();
    }
  }
  fs::finalizeS3();
  Aws::ShutdownAPI(sdk);
  return 0;
}

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
#include <gflags/gflags.h>
#include <filesystem>

#include "BoltBackend.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <bolt/common/memory/sparksql/ConfigurationResolver.h>

#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/plannodes/RowVectorStream.h"
#include "utils/ConfigExtractor.h"
#include "bolt/dwio/orc/reader/RegisterOrcReader.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_GPU
#include "bolt/experimental/cudf/CudfConfig.h"
#include "bolt/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "bolt/experimental/cudf/exec/ToCudf.h"
#endif

#include "compute/BoltRuntime.h"
#include "config/BoltConfig.h"
#include "jni/JniFileSystem.h"
#include "memory/BoltGlutenMemoryManager.h"
#include "operators/functions/SparkExprToSubfieldFilterParser.h"
#include "udf/UdfLoader.h"
#include "utils/Exception.h"
#include "bolt/common/caching/SsdCache.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveDataSource.h"
#include "bolt/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h" // @manual
#include "bolt/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h" // @manual
#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h" // @manual
#include "bolt/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h" // @manual
#include "bolt/dwio/orc/reader/OrcReader.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/RegisterParquetWriter.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include "bolt/shuffle/sparksql/ShuffleReaderNode.h"

DECLARE_bool(bolt_exception_user_stacktrace_enabled);
DECLARE_int32(bolt_memory_num_shared_leaf_pools);
DECLARE_bool(bolt_memory_use_hugepages);
DECLARE_bool(bolt_ssd_odirect);
DECLARE_bool(bolt_memory_pool_capacity_transfer_across_tasks);
DECLARE_int32(cache_prefetch_min_pct);

DECLARE_int32(gluten_bolt_async_timeout_on_task_stopping);
DEFINE_int32(gluten_bolt_async_timeout_on_task_stopping, 30000, "Async timout when task is being stopped");

using namespace bytedance;

namespace gluten {

namespace {
MemoryManager* boltMemoryManagerFactory(
    const std::string& kind,
    std::unique_ptr<AllocationListener> listener,
    const std::string& name) {
  return new BoltMemoryManager(kind, std::move(listener), *BoltBackend::get()->getBackendConf(), name);
}

void boltMemoryManagerReleaser(MemoryManager* memoryManager) {
  delete memoryManager;
}

class BoltThreadManager : public ThreadManager {
 public:
  BoltThreadManager(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer)
      : ThreadManager(kind), initializer_(std::shared_ptr<ThreadInitializer>(std::move(initializer))) {}

  ThreadInitializer* getThreadInitializer() override {
    return initializer_.get();
  }

 private:
  std::shared_ptr<ThreadInitializer> initializer_;
};

ThreadManager* boltThreadManagerFactory(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer) {
  return new BoltThreadManager(kind, std::move(initializer));
}

void boltThreadManagerReleaser(ThreadManager* threadManager) {
  delete threadManager;
}

Runtime* boltRuntimeFactory(
    const std::string& kind,
    MemoryManager* memoryManager,
    ThreadManager* threadManager,
    const std::unordered_map<std::string, std::string>& sessionConf,
    int64_t taskId) {
  auto* vmm = dynamic_cast<BoltMemoryManager*>(memoryManager);
  GLUTEN_CHECK(vmm != nullptr, "Not a Bolt memory manager");
  // new object every time
  return new BoltRuntime(kind, vmm, threadManager, sessionConf, taskId);
}

void boltRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}
} // namespace

void BoltBackend::init(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  backendConf_ =
      std::make_shared<bytedance::bolt::config::ConfigBase>(std::unordered_map<std::string, std::string>(conf));

  globalMemoryManager_ =
      std::make_unique<BoltMemoryManager>(kBoltBackendKind, std::move(listener), *backendConf_, "global");

  // Register factories.
  MemoryManager::registerFactory(kBoltBackendKind, boltMemoryManagerFactory, boltMemoryManagerReleaser);
  ThreadManager::registerFactory(kBoltBackendKind, boltThreadManagerFactory, boltThreadManagerReleaser);
  Runtime::registerFactory(kBoltBackendKind, boltRuntimeFactory, boltRuntimeReleaser);

  if (backendConf_->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "BoltBackend config:" << printConfig(backendConf_->rawConfigs());
  }

  // Init glog and log level.
  if (!backendConf_->get<bool>(kDebugModeEnabled, false)) {
    FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    FLAGS_minloglevel = backendConf_->get<uint32_t>(kGlogSeverityLevel, kGlogSeverityLevelDefault);
  } else {
    if (backendConf_->valueExists(kGlogVerboseLevel)) {
      FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    } else {
      FLAGS_v = kGlogVerboseLevelMaximum;
    }
  }
  FLAGS_logtostderr = true;
  google::InitGoogleLogging("gluten");

  // Allow growing buffer in another task through its memory pool.
  FLAGS_bolt_memory_pool_capacity_transfer_across_tasks =
      backendConf_->get<bool>(kMemoryPoolCapacityTransferAcrossTasks, true);

  // Avoid creating too many shared leaf pools.
  FLAGS_bolt_memory_num_shared_leaf_pools = 0;

  // Set bolt_exception_user_stacktrace_enabled.
  FLAGS_bolt_exception_user_stacktrace_enabled =
      backendConf_->get<bool>(kEnableUserExceptionStacktrace, kEnableUserExceptionStacktraceDefault);

  // Set bolt_exception_system_stacktrace_enabled.
  FLAGS_bolt_exception_system_stacktrace_enabled =
      backendConf_->get<bool>(kEnableSystemExceptionStacktrace, kEnableSystemExceptionStacktraceDefault);

  // Set bolt_memory_use_hugepages.
  FLAGS_bolt_memory_use_hugepages = backendConf_->get<bool>(kMemoryUseHugePages, kMemoryUseHugePagesDefault);

  // Async timeout.
  FLAGS_gluten_bolt_async_timeout_on_task_stopping =
      backendConf_->get<int32_t>(kBoltAsyncTimeoutOnTaskStopping, kBoltAsyncTimeoutOnTaskStoppingDefault);

  // Set cache_prefetch_min_pct default as 0 to force all loads are prefetched in DirectBufferInput.
  FLAGS_cache_prefetch_min_pct = backendConf_->get<int>(kCachePrefetchMinPct, 0);

  auto hiveConf = getHiveConfig(backendConf_);

  // Setup and register.
  bolt::filesystems::registerLocalFileSystem();

#ifdef ENABLE_HDFS
  bolt::filesystems::registerHdfsFileSystem();
#endif
#ifdef ENABLE_S3
  bolt::filesystems::registerS3FileSystem();
#endif
#ifdef ENABLE_GCS
  bolt::filesystems::registerGcsFileSystem();
#endif
#ifdef ENABLE_ABFS
  bolt::filesystems::registerAbfsFileSystem();
  bolt::filesystems::registerAzureClientProvider(*hiveConf);
#endif

#ifdef GLUTEN_ENABLE_GPU
  if (backendConf_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    std::unordered_map<std::string, std::string> options = {
        {bolt::cudf_bolt::CudfConfig::kCudfEnabled, "true"},
        {bolt::cudf_bolt::CudfConfig::kCudfDebugEnabled, backendConf_->get(kDebugCudf, kDebugCudfDefault)},
        {bolt::cudf_bolt::CudfConfig::kCudfMemoryResource,
         backendConf_->get(kCudfMemoryResource, kCudfMemoryResourceDefault)},
        {bolt::cudf_bolt::CudfConfig::kCudfMemoryPercent,
         backendConf_->get(kCudfMemoryPercent, kCudfMemoryPercentDefault)}};
    auto& cudfConfig = bolt::cudf_bolt::CudfConfig::getInstance();
    cudfConfig.initialize(std::move(options));
    bolt::cudf_bolt::registerCudf();
  }
#endif

  initJolFilesystem();
  initConnector(hiveConf);

  bolt::dwio::common::registerFileSinks();
  bolt::parquet::registerParquetReaderFactory();
  bolt::parquet::registerParquetWriterFactory();
  bolt::orc::registerOrcReaderFactory();
  bolt::exec::ExprToSubfieldFilterParser::registerParser(std::make_unique<SparkExprToSubfieldFilterParser>());

  // Register Bolt functions
  registerAllFunctions();
  if (!bytedance::bolt::isRegisteredVectorSerde()) {
    // serde, for spill
    bytedance::bolt::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(bytedance::bolt::VectorSerde::Kind::kPresto)) {
    // RSS shuffle serde.
    bytedance::bolt::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  bolt::exec::Operator::registerOperator(std::make_unique<RowVectorStreamOperatorTranslator>());
  bolt::exec::Operator::registerOperator(
      std::make_unique<bytedance::bolt::shuffle::sparksql::SparkShuffleWriterTranslator>());
  bolt::exec::Operator::registerOperator(
      std::make_unique<bytedance::bolt::shuffle::sparksql::SparkShuffleReaderTranslator>());

  initUdf();

  // Initialize Bolt-side memory manager for current process. The memory manager
  // will be used during spill calls so we don't track it with Spark off-heap memory instead
  // we rely on overhead memory. If we track it with off-heap memory, recursive reservations from
  // Spark off-heap memory pool will be conducted to cause unexpected OOMs.
  auto sparkOverhead = backendConf_->get<int64_t>(kSparkOverheadMemory);
  int64_t memoryManagerCapacity;
  if (sparkOverhead.has_value()) {
    // 0.75 * total overhead memory is used for Bolt global memory manager.
    // FIXME: Make this configurable.
    memoryManagerCapacity = sparkOverhead.value() * 0.75;
  } else {
    memoryManagerCapacity = bytedance::bolt::memory::kMaxMemory;
  }
  LOG(INFO) << "Setting global Bolt memory manager with capacity: " << memoryManagerCapacity;
  bytedance::bolt::memory::MemoryManager::Options options;
  options.allocatorCapacity = memoryManagerCapacity;
  bytedance::bolt::memory::initializeMemoryManager(options);

  // local cache persistent relies on the cache pool from root memory pool so we need to init this
  // after the memory manager instanced
  initCache();

  bool isParallelEnabled = backendConf_->get<bool>(kGlutenEnableParallel, false);
  LOG(INFO) << "BoltBackend::init isParallelEnabled=" << isParallelEnabled;
  auto executorCores = getSparkExecutorCores(conf);
  auto boostRatio = getSparkVcoreBoostRatio(conf);

  if (isParallelEnabled) {
    // multi-thread spark
    // tentative: spark.executor.vcore * spark.vcore.boost.ratio * (maxDrivers + 1)
    auto numDriverCpuThreads = executorCores * boostRatio * 3;
    BOLT_CHECK_GE(numDriverCpuThreads, 1, "numDriverCpuThreads can not be < 1");
    driverExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
        numDriverCpuThreads, std::make_shared<folly::NamedThreadFactory>("Driver"));
    LOG(ERROR) << "[multi-thread spark] Set up driver thread pool of size=" << numDriverCpuThreads;
  }

  if (backendConf_->get<bool>(kDynamicConcurrencyAdjustmentEnabled, kDynamicConcurrencyAdjustmentEnabledDefault)) {
    int32_t defaultValue;
    bool boltScheduleEnabled = backendConf_->get<bool>(kBoltTaskSchedulingEnabled, false);
    if (boltScheduleEnabled) {
      defaultValue = backendConf_->get<int32_t>(kDynamicConcurrencyDefaultValue, 4);
      BOLT_CHECK(boostRatio >= 2 && defaultValue <= executorCores * boostRatio);
    } else {
      defaultValue = executorCores * boostRatio;
    }

    int32_t milliCores = getSparkExecutorMillicores(conf, executorCores * 1000);

    LOG(INFO) << __FUNCTION__ << ": kDynamicConcurrencyAdjustmentEnabled is true, executor.core = " << executorCores
              << ", boostRatio = " << boostRatio << ", kDynamicConcurrencyDefaultValue = " << defaultValue
              << ", milliCores = " << milliCores << ", kBoltTaskSchedulingEnabled is " << boltScheduleEnabled;
    bytedance::bolt::exec::ExecutorTaskScheduler::instance().setDefaultTaskParallelism(defaultValue);
    bytedance::bolt::exec::ExecutorTaskScheduler::instance().setTargetMemoryUsage(1.3 * milliCores / 1000);
  }
}

bytedance::bolt::cache::AsyncDataCache* BoltBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void BoltBackend::initJolFilesystem() {
  int64_t maxSpillFileSize = backendConf_->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  registerJolFileSystem(maxSpillFileSize);
}

std::unique_ptr<bytedance::bolt::cache::SsdCache> BoltBackend::initSsdCache(uint64_t ssdCacheSize) {
  FLAGS_bolt_ssd_odirect = backendConf_->get<bool>(kBoltSsdODirectEnabled, false);
  uint64_t memCacheSize = backendConf_->get<uint64_t>(kBoltMemCacheSize, kBoltMemCacheSizeDefault);
  int32_t ssdCacheShards = backendConf_->get<int32_t>(kBoltSsdCacheShards, kBoltSsdCacheShardsDefault);
  int32_t ssdCacheIOThreads = backendConf_->get<int32_t>(kBoltSsdCacheIOThreads, kBoltSsdCacheIOThreadsDefault);
  std::string ssdCachePathPrefix = backendConf_->get<std::string>(kBoltSsdCachePath, kBoltSsdCachePathDefault);
  [[maybe_unused]] uint64_t ssdCheckpointIntervalSize =
      backendConf_->get<uint64_t>(kBoltSsdCheckpointIntervalBytes, 0);
  [[maybe_unused]] bool disableFileCow = backendConf_->get<bool>(kBoltSsdDisableFileCow, false);
  [[maybe_unused]] bool checksumEnabled = backendConf_->get<bool>(kBoltSsdCheckSumEnabled, false);
  [[maybe_unused]] bool checksumReadVerificationEnabled =
      backendConf_->get<bool>(kBoltSsdCheckSumReadVerificationEnabled, false);

  cachePathPrefix_ = ssdCachePathPrefix;
  cacheFilePrefix_ = getCacheFilePrefix();
  std::string ssdCachePath = ssdCachePathPrefix + "/" + cacheFilePrefix_;
  ssdCacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ssdCacheIOThreads);
  // TODO sync bolt and uncomment it(https://github.com/apache/incubator-gluten/pull/9228)
  // const cache::SsdCache::Config config(
  //     ssdCachePath,
  //     ssdCacheSize,
  //     ssdCacheShards,
  //     ssdCacheExecutor_.get(),
  //     ssdCheckpointIntervalSize,
  //     disableFileCow,
  //     checksumEnabled,
  //     checksumReadVerificationEnabled);
  // auto ssd = std::make_unique<bolt::cache::SsdCache>(config);
  auto ssd =
      std::make_unique<bolt::cache::SsdCache>(ssdCachePath, ssdCacheSize, ssdCacheShards, ssdCacheExecutor_.get());
  std::error_code ec;
  const std::filesystem::space_info si = std::filesystem::space(ssdCachePathPrefix, ec);
  if (si.available < ssdCacheSize) {
    BOLT_FAIL(
        "not enough space for ssd cache in " + ssdCachePath + " cache size: " + std::to_string(ssdCacheSize) +
        "free space: " + std::to_string(si.available));
  }
  LOG(INFO) << "Initializing SSD cache with: "
            << "memory cache size : " << memCacheSize << ", ssdCache prefix: " << ssdCachePath
            << ", ssdCache size: " << ssdCacheSize << ", ssdCache shards: " << ssdCacheShards
            << ", ssdCache IO threads: " << ssdCacheIOThreads;
  return ssd;
}

void BoltBackend::initCache() {
  if (backendConf_->get<bool>(kBoltCacheEnabled, false)) {
    uint64_t memCacheSize = backendConf_->get<uint64_t>(kBoltMemCacheSize, kBoltMemCacheSizeDefault);
    uint64_t ssdCacheSize = backendConf_->get<uint64_t>(kBoltSsdCacheSize, kBoltSsdCacheSizeDefault);

    bolt::memory::MmapAllocator::Options options;
    options.capacity = memCacheSize;
    cacheAllocator_ = std::make_shared<bolt::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = bolt::cache::AsyncDataCache::create(cacheAllocator_.get());
    } else {
      // TODO: this is not tracked by Spark.
      auto ssd = initSsdCache(ssdCacheSize);
      asyncDataCache_ = bolt::cache::AsyncDataCache::create(cacheAllocator_.get(), std::move(ssd));
    }

    BOLT_CHECK_NOT_NULL(dynamic_cast<bolt::cache::AsyncDataCache*>(asyncDataCache_.get()));
    LOG(INFO) << "AsyncDataCache is ready";
  }
}

std::tuple<int32_t, int32_t, int32_t, int64_t, int32_t> BoltBackend::getScanPreloadAdaptiveParam(
    const std::shared_ptr<bytedance::bolt::config::ConfigBase>& conf,
    bool log) {
  auto maxTaskNumber = BoltGlutenMemoryManager::getMaxTaskNumber(conf->rawConfigs());
  auto taskMemoryCapacity = BoltGlutenMemoryManager::getTaskMemoryCapacity(conf->rawConfigs());
  auto avgTaskMemory = taskMemoryCapacity / maxTaskNumber;
  // if avgTaskMemory < 128M, set ioThreads to 0
  // if avgTaskMemory in [128M, 512M), set ioThreads to 1, prefetch rowgroup to 1
  // if avgTaskMemory in [512M, 1G), set ioThreads to 2, prefetch rowgroup to 2
  // if avgTaskMemory in [1G, 2G), set ioThreads to maxTaskNumber, prefetch rowgroup to 4
  // else set to default values.
  int32_t ioThreads = conf->get<int32_t>(kBoltIOThreads, kBoltIOThreadsDefault);
  int32_t prefetchRowGroups = conf->get<int32_t>(bytedance::bolt::connector::hive::HiveConfig::kPrefetchRowGroups, 16);
  int32_t preloadSplitPerDriver = conf->get<int32_t>(bytedance::bolt::core::QueryConfig::kMaxSplitPreloadPerDriver, 4);
  // 0 for disable, 1 for adaptive enabled, -1 for force enabled
  int32_t preloadEnabled = conf->get<int32_t>(kPreloadEnabled, kPreloadEnabledDefault);
  if (ioThreads == 0 || preloadEnabled == 0) {
    LOG(WARNING) << "preload.enabled " << preloadEnabled << ", ioThreads is " << ioThreads << ", so disable preload";
    prefetchRowGroups = 0;
    preloadSplitPerDriver = 0;
    ioThreads = 0;
  } else if (preloadEnabled == 1) {
    if (avgTaskMemory < (1ULL << 27)) { // 128M, disable prefetch
      ioThreads = 0;
      prefetchRowGroups = 0;
      preloadSplitPerDriver = 0;
    } else if (avgTaskMemory < (1ULL << 29)) { // 512M
      ioThreads = 1;
      prefetchRowGroups = 1;
      preloadSplitPerDriver = 1;
    } else if (avgTaskMemory < (1ULL << 30)) { // 1G
      ioThreads = std::max(maxTaskNumber / 2, 2);
      prefetchRowGroups = 2;
      preloadSplitPerDriver = 1;
    } else if (avgTaskMemory < (2ULL << 30)) { // 2G
      ioThreads = maxTaskNumber;
      prefetchRowGroups = 4;
      preloadSplitPerDriver = 2;
    }
  }
  if (log) {
    LOG(INFO) << "init scan prefetch: preload.enabled: " << preloadEnabled << ", ioThreads: " << ioThreads
              << ", prefetchRowGroups: " << prefetchRowGroups << ", preloadSplitPerDriver: " << preloadSplitPerDriver
              << ", avgTaskMemory: " << avgTaskMemory << ", maxTaskNumber: " << maxTaskNumber
              << ", totalMemory: " << taskMemoryCapacity;
  }
  return std::make_tuple(ioThreads, prefetchRowGroups, preloadSplitPerDriver, avgTaskMemory, preloadEnabled);
}

void BoltBackend::initConnector(const std::shared_ptr<bolt::config::ConfigBase>& hiveConf) {
  auto ioThreads = backendConf_->get<int32_t>(kBoltIOThreads, kBoltIOThreadsDefault);
  GLUTEN_CHECK(
      ioThreads >= 0,
      kBoltIOThreads + " was set to negative number " + std::to_string(ioThreads) + ", this should not happen.");
  auto [_ioThreads, _prefetchRowGroups, _preloadSplitPerDriver, _, __] = getScanPreloadAdaptiveParam(backendConf_, true);
  ioThreads = _ioThreads;
  auto mutableConf = std::make_shared<bytedance::bolt::config::ConfigBase>(hiveConf->rawConfigsCopy(), true);
  mutableConf->set(bolt::connector::hive::HiveConfig::kPrefetchRowGroups, std::to_string(_prefetchRowGroups));
  if (ioThreads > 0) {
    LOG(INFO) << "Init ioExecutor with threads=" << ioThreads << " name:" << kAsyncPreloadThreadName;
    std::shared_ptr<folly::ThreadFactory> threadFactory =
        std::make_shared<folly::NamedThreadFactory>(kAsyncPreloadThreadName);
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads, threadFactory);

    if (backendConf_->get<bool>(kDynamicConcurrencyAdjustmentEnabled, kDynamicConcurrencyAdjustmentEnabledDefault)) {
      auto confMap = backendConf_->rawConfigs();
      auto maxTasks = getSparkExecutorCores(confMap) * getSparkVcoreBoostRatio(confMap);
      //(ioThreadsPerTask, defaultIoThreads, maxIoThreads, ioExecutor)
      bytedance::bolt::exec::ExecutorTaskScheduler::instance().setIOThreadInfo(
          1.0 * ioThreads / maxTasks, ioThreads, getSparkExecutorCores(confMap) * 16, ioExecutor_.get());
    }
  }
  bolt::connector::registerConnector(
      std::make_shared<bolt::connector::hive::HiveConnector>(kHiveConnectorId, mutableConf, ioExecutor_.get()));
#ifdef GLUTEN_ENABLE_GPU
  if (backendConf_->get<bool>(kCudfEnableTableScan, kCudfEnableTableScanDefault) &&
      backendConf_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    bytedance::bolt::cudf_bolt::connector::hive::CudfHiveConnectorFactory factory;
    auto hiveConnector = factory.newConnector(kCudfHiveConnectorId, mutableConf, ioExecutor_.get());
    bytedance::bolt::connector::registerConnector(hiveConnector);
  }
#endif
}

void BoltBackend::initUdf() {
  auto got = backendConf_->get<std::string>(kBoltUdfLibraryPaths, "");
  if (!got.empty()) {
    auto udfLoader = UdfLoader::getInstance();
    udfLoader->loadUdfLibraries(got);
    udfLoader->registerUdf();
  }
}

std::unique_ptr<BoltBackend> BoltBackend::instance_ = nullptr;

void BoltBackend::create(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  instance_ = std::unique_ptr<BoltBackend>(new BoltBackend(std::move(listener), conf));
}

BoltBackend* BoltBackend::get() {
  if (!instance_) {
    LOG(WARNING) << "BoltBackend instance is null, please invoke BoltBackend#create before use.";
    throw GlutenException("BoltBackend instance is null.");
  }
  return instance_.get();
}

void BoltBackend::tearDown() {
#ifdef ENABLE_HDFS
  for (const auto& [_, filesystem] : bytedance::bolt::filesystems::registeredFilesystems) {
    filesystem->close();
  }
#endif

  // Destruct IOThreadPoolExecutor will join all threads.
  // On threads exit, thread local variables can be constructed with referencing global variables.
  // So, we need to destruct IOThreadPoolExecutor and stop the threads before global variables get destructed.
  ioExecutor_.reset();
  globalMemoryManager_.reset();

  // dump cache stats on exit if enabled
  if (dynamic_cast<bytedance::bolt::cache::AsyncDataCache*>(asyncDataCache_.get())) {
    LOG(INFO) << asyncDataCache_->toString();
    for (const auto& entry : std::filesystem::directory_iterator(cachePathPrefix_)) {
      if (entry.path().filename().string().find(cacheFilePrefix_) != std::string::npos) {
        LOG(INFO) << "Removing cache file " << entry.path().filename().string();
        std::filesystem::remove(cachePathPrefix_ + "/" + entry.path().filename().string());
      }
    }
    asyncDataCache_->shutdown();
  }
}

} // namespace gluten

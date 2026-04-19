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
#include <filesystem>

#include "VeloxBackend.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>

#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/plannodes/RowVectorStream.h"
#include "utils/ConfigExtractor.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_GPU
#include "operators/plannodes/CudfVectorStream.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/exec/SparkAggregateFunctions.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/expression/SparkFunctions.h"
#endif

#include "compute/VeloxRuntime.h"
#include "config/VeloxConfig.h"
#include "jni/JniFileSystem.h"
#include "memory/GlutenBufferedInputBuilder.h"
#include "operators/functions/SparkExprToSubfieldFilterParser.h"
#include "operators/plannodes/RowVectorStream.h"
#include "shuffle/ArrowShuffleDictionaryWriter.h"
#include "udf/UdfLoader.h"
#include "utils/Exception.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/BufferedInputBuilder.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h" // @manual
#include "velox/dwio/orc/reader/OrcReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_bool(velox_ssd_odirect);
DECLARE_bool(velox_memory_pool_capacity_transfer_across_tasks);
DECLARE_int32(cache_prefetch_min_pct);

DECLARE_int32(gluten_velox_async_timeout_on_task_stopping);
DEFINE_int32(gluten_velox_async_timeout_on_task_stopping, 30000, "Async timout when task is being stopped");

using namespace facebook;

namespace gluten {

namespace {
MemoryManager* veloxMemoryManagerFactory(const std::string& kind, std::unique_ptr<AllocationListener> listener) {
  return new VeloxMemoryManager(kind, std::move(listener), *VeloxBackend::get()->getBackendConf());
}

void veloxMemoryManagerReleaser(MemoryManager* memoryManager) {
  delete memoryManager;
}

Runtime* veloxRuntimeFactory(
    const std::string& kind,
    MemoryManager* memoryManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  auto* vmm = dynamic_cast<VeloxMemoryManager*>(memoryManager);
  GLUTEN_CHECK(vmm != nullptr, "Not a Velox memory manager");
  return new VeloxRuntime(kind, vmm, sessionConf);
}

void veloxRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}
} // namespace

void VeloxBackend::init(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  backendConf_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(conf));

  globalMemoryManager_ = std::make_unique<VeloxMemoryManager>(kVeloxBackendKind, std::move(listener), *backendConf_);

  // Register factories.
  MemoryManager::registerFactory(kVeloxBackendKind, veloxMemoryManagerFactory, veloxMemoryManagerReleaser);
  Runtime::registerFactory(kVeloxBackendKind, veloxRuntimeFactory, veloxRuntimeReleaser);

  if (backendConf_->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "VeloxBackend config:" << printConfig(backendConf_->rawConfigs());
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
  FLAGS_velox_memory_pool_capacity_transfer_across_tasks =
      backendConf_->get<bool>(kMemoryPoolCapacityTransferAcrossTasks, true);

  // Avoid creating too many shared leaf pools.
  FLAGS_velox_memory_num_shared_leaf_pools = 0;

  // Set velox_exception_user_stacktrace_enabled.
  FLAGS_velox_exception_user_stacktrace_enabled =
      backendConf_->get<bool>(kEnableUserExceptionStacktrace, kEnableUserExceptionStacktraceDefault);

  // Set velox_exception_system_stacktrace_enabled.
  FLAGS_velox_exception_system_stacktrace_enabled =
      backendConf_->get<bool>(kEnableSystemExceptionStacktrace, kEnableSystemExceptionStacktraceDefault);

  // Set velox_memory_use_hugepages.
  FLAGS_velox_memory_use_hugepages = backendConf_->get<bool>(kMemoryUseHugePages, kMemoryUseHugePagesDefault);

  // Async timeout.
  FLAGS_gluten_velox_async_timeout_on_task_stopping =
      backendConf_->get<int32_t>(kVeloxAsyncTimeoutOnTaskStopping, kVeloxAsyncTimeoutOnTaskStoppingDefault);

  // Set cache_prefetch_min_pct default as 0 to force all loads are prefetched in DirectBufferInput.
  FLAGS_cache_prefetch_min_pct = backendConf_->get<int>(kCachePrefetchMinPct, 0);

  auto hiveConf = createHiveConnectorConfig(backendConf_);

  // Setup and register.
  velox::filesystems::registerLocalFileSystem();

#ifdef ENABLE_HDFS
  velox::filesystems::registerHdfsFileSystem();
#endif
#ifdef ENABLE_S3
  velox::filesystems::registerS3FileSystem();
#endif
#ifdef ENABLE_GCS
  velox::filesystems::registerGcsFileSystem();
#endif
#ifdef ENABLE_ABFS
  velox::filesystems::registerAbfsFileSystem();
  velox::filesystems::registerAzureClientProvider(*hiveConf);
#endif

#ifdef GLUTEN_ENABLE_GPU
  if (backendConf_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    std::unordered_map<std::string, std::string> options = {
        {velox::cudf_velox::CudfConfig::kCudfEnabled, "true"},
        {velox::cudf_velox::CudfConfig::kCudfDebugEnabled, backendConf_->get(kDebugCudf, kDebugCudfDefault)},
        {velox::cudf_velox::CudfConfig::kCudfMemoryResource,
         backendConf_->get(kCudfMemoryResource, kCudfMemoryResourceDefault)},
        {velox::cudf_velox::CudfConfig::kCudfMemoryPercent,
         backendConf_->get(kCudfMemoryPercent, kCudfMemoryPercentDefault)}};
    auto& cudfConfig = velox::cudf_velox::CudfConfig::getInstance();
    cudfConfig.initialize(std::move(options));
    velox::cudf_velox::registerCudf();
    velox::exec::Operator::registerOperator(std::make_unique<CudfVectorStreamOperatorTranslator>());
    velox::cudf_velox::registerSparkFunctions("");
    velox::cudf_velox::registerSparkAggregateFunctions("");
  }
#endif

  initJolFilesystem();
  initConnector(hiveConf);

  velox::dwio::common::registerFileSinks();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
  velox::orc::registerOrcReaderFactory();
  velox::exec::ExprToSubfieldFilterParser::registerParser(std::make_unique<SparkExprToSubfieldFilterParser>());
  velox::connector::hive::BufferedInputBuilder::registerBuilder(std::make_shared<GlutenBufferedInputBuilder>());

  // Register Velox functions
  registerAllFunctions();
  if (!facebook::velox::isRegisteredVectorSerde()) {
    // serde, for spill
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde("Presto")) {
    // RSS shuffle serde.
    facebook::velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }

  initUdf();

  // Initialize Velox-side memory manager for current process. The memory manager
  // will be used during spill calls so we don't track it with Spark off-heap memory instead
  // we rely on overhead memory. If we track it with off-heap memory, recursive reservations from
  // Spark off-heap memory pool will be conducted to cause unexpected OOMs.
  auto sparkOverhead = backendConf_->get<int64_t>(kSparkOverheadMemory);
  int64_t memoryManagerCapacity;
  if (sparkOverhead.has_value()) {
    // 0.75 * total overhead memory is used for Velox global memory manager.
    // FIXME: Make this configurable.
    memoryManagerCapacity = sparkOverhead.value() * 0.75;
  } else {
    memoryManagerCapacity = facebook::velox::memory::kMaxMemory;
  }
  LOG(INFO) << "Setting global Velox memory manager with capacity: " << memoryManagerCapacity;
  facebook::velox::memory::MemoryManager::Options options;
  options.allocatorCapacity = memoryManagerCapacity;
  facebook::velox::memory::initializeMemoryManager(options);

  // local cache persistent relies on the cache pool from root memory pool so we need to init this
  // after the memory manager instanced
  initCache();

  registerShuffleDictionaryWriterFactory([](MemoryManager* memoryManager, arrow::util::Codec* codec) {
    return std::make_unique<ArrowShuffleDictionaryWriter>(memoryManager, codec);
  });
}

facebook::velox::cache::AsyncDataCache* VeloxBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void VeloxBackend::initJolFilesystem() {
  int64_t maxSpillFileSize = backendConf_->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  registerJolFileSystem(maxSpillFileSize);
}

std::unique_ptr<facebook::velox::cache::SsdCache> VeloxBackend::initSsdCache(uint64_t ssdCacheSize) {
  FLAGS_velox_ssd_odirect = backendConf_->get<bool>(kVeloxSsdODirectEnabled, false);
  int32_t ssdCacheShards = backendConf_->get<int32_t>(kVeloxSsdCacheShards, kVeloxSsdCacheShardsDefault);
  int32_t ssdCacheIOThreads = backendConf_->get<int32_t>(kVeloxSsdCacheIOThreads, kVeloxSsdCacheIOThreadsDefault);
  std::string ssdCachePathPrefix = backendConf_->get<std::string>(kVeloxSsdCachePath, kVeloxSsdCachePathDefault);
  uint64_t ssdCheckpointIntervalSize = backendConf_->get<uint64_t>(kVeloxSsdCheckpointIntervalBytes, 0);
  bool disableFileCow = backendConf_->get<bool>(kVeloxSsdDisableFileCow, false);
  bool checksumEnabled = backendConf_->get<bool>(kVeloxSsdCheckSumEnabled, false);
  bool checksumReadVerificationEnabled = backendConf_->get<bool>(kVeloxSsdCheckSumReadVerificationEnabled, false);

  cachePathPrefix_ = ssdCachePathPrefix;
  cacheFilePrefix_ = getCacheFilePrefix();
  std::string ssdCachePath = ssdCachePathPrefix + "/" + cacheFilePrefix_;
  ssdCacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ssdCacheIOThreads);
  const cache::SsdCache::Config config(
      ssdCachePath,
      ssdCacheSize,
      ssdCacheShards,
      ssdCacheExecutor_.get(),
      ssdCheckpointIntervalSize,
      disableFileCow,
      checksumEnabled,
      checksumReadVerificationEnabled);
  auto ssd = std::make_unique<velox::cache::SsdCache>(config);
  std::error_code ec;
  const std::filesystem::space_info si = std::filesystem::space(ssdCachePathPrefix, ec);
  if (si.available < ssdCacheSize) {
    VELOX_FAIL(
        "not enough space for ssd cache in " + ssdCachePath + " cache size: " + std::to_string(ssdCacheSize) +
        "free space: " + std::to_string(si.available));
  }
  LOG(INFO) << "Initializing SSD cache with: " << config.toString();
  return ssd;
}

void VeloxBackend::initCache() {
  if (backendConf_->get<bool>(kVeloxCacheEnabled, false)) {
    uint64_t memCacheSize = backendConf_->get<uint64_t>(kVeloxMemCacheSize, kVeloxMemCacheSizeDefault);
    uint64_t ssdCacheSize = backendConf_->get<uint64_t>(kVeloxSsdCacheSize, kVeloxSsdCacheSizeDefault);

    velox::memory::MmapAllocator::Options options;
    options.capacity = memCacheSize;
    cacheAllocator_ = std::make_shared<velox::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get());
    } else {
      // TODO: this is not tracked by Spark.
      auto ssd = initSsdCache(ssdCacheSize);
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get(), std::move(ssd));
    }

    VELOX_CHECK_NOT_NULL(dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get()));
    LOG(INFO) << "AsyncDataCache is ready";
  }
}

void VeloxBackend::initConnector(const std::shared_ptr<velox::config::ConfigBase>& hiveConf) {
  auto ioThreads = backendConf_->get<int32_t>(kVeloxIOThreads, kVeloxIOThreadsDefault);
  GLUTEN_CHECK(
      ioThreads >= 0,
      kVeloxIOThreads + " was set to negative number " + std::to_string(ioThreads) + ", this should not happen.");
  if (ioThreads > 0) {
    ioExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        ioThreads, std::make_unique<folly::UnboundedBlockingQueue<folly::CPUThreadPoolExecutor::CPUTask>>());
  }
  velox::connector::registerConnector(
      std::make_shared<velox::connector::hive::HiveConnector>(kHiveConnectorId, hiveConf, ioExecutor_.get()));

  // Register value-stream connector for runtime iterator-based inputs
  auto valueStreamDynamicFilterEnabled =
      backendConf_->get<bool>(kValueStreamDynamicFilterEnabled, kValueStreamDynamicFilterEnabledDefault);
  velox::connector::registerConnector(
      std::make_shared<ValueStreamConnector>(kIteratorConnectorId, hiveConf, valueStreamDynamicFilterEnabled));

#ifdef GLUTEN_ENABLE_GPU
  if (backendConf_->get<bool>(kCudfEnableTableScan, kCudfEnableTableScanDefault) &&
      backendConf_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    facebook::velox::cudf_velox::connector::hive::CudfHiveConnectorFactory factory;
    auto hiveConnector = factory.newConnector(kCudfHiveConnectorId, hiveConf, ioExecutor_.get());
    facebook::velox::connector::registerConnector(hiveConnector);
  }
#endif
}

void VeloxBackend::initUdf() {
  auto got = backendConf_->get<std::string>(kVeloxUdfLibraryPaths, "");
  if (!got.empty()) {
    auto udfLoader = UdfLoader::getInstance();
    udfLoader->loadUdfLibraries(got);
    udfLoader->registerUdf();
  }
}

std::unique_ptr<VeloxBackend> VeloxBackend::instance_ = nullptr;

void VeloxBackend::create(
    std::unique_ptr<AllocationListener> listener,
    const std::unordered_map<std::string, std::string>& conf) {
  instance_ = std::unique_ptr<VeloxBackend>(new VeloxBackend(std::move(listener), conf));
}

VeloxBackend* VeloxBackend::get() {
  if (!instance_) {
    LOG(WARNING) << "VeloxBackend instance is null, please invoke VeloxBackend#create before use.";
    throw GlutenException("VeloxBackend instance is null.");
  }
  return instance_.get();
}

void VeloxBackend::tearDown() {
#ifdef ENABLE_HDFS
  for (const auto& [_, filesystem] : facebook::velox::filesystems::registeredFilesystems) {
    filesystem->close();
  }
#endif
#ifdef ENABLE_S3
  velox::filesystems::finalizeS3FileSystem();
#endif

  // Destruct IOThreadPoolExecutor will join all threads.
  // On threads exit, thread local variables can be constructed with referencing global variables.
  // So, we need to destruct IOThreadPoolExecutor and stop the threads before global variables get destructed.
  ioExecutor_.reset();
  globalMemoryManager_.reset();

  // dump cache stats on exit if enabled
  if (dynamic_cast<facebook::velox::cache::AsyncDataCache*>(asyncDataCache_.get())) {
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

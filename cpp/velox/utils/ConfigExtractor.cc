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

// This File includes common helper functions with Arrow dependency.

#include "ConfigExtractor.h"
#include <algorithm>
#include <stdexcept>

#include "config/VeloxConfig.h"
#include "utils/Exception.h"
#include "utils/Macros.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/core/QueryConfig.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/parquet/common/ParquetConfig.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#ifdef GLUTEN_ENABLE_GPU
#include "velox/experimental/cudf/CudfConfig.h"
#endif

namespace gluten {

namespace {

void getS3HiveConfig(
    std::shared_ptr<facebook::velox::config::ConfigBase> conf,
    FileSystemType fsType,
    std::unordered_map<std::string, std::string>& hiveConfMap) {
#ifdef ENABLE_S3
  using namespace facebook::velox::filesystems;
  std::string_view kSparkHadoopS3Prefix = "spark.hadoop.fs.s3a.";
  std::string_view kSparkHadoopS3BucketPrefix = "spark.hadoop.fs.s3a.bucket.";

  // Log granularity of AWS C++ SDK
  const std::string kVeloxAwsSdkLogLevel = "spark.gluten.velox.awsSdkLogLevel";
  const std::string kVeloxAwsSdkLogLevelDefault = "FATAL";

  // Whether to use proxy from env for s3 c++ client
  const std::string kVeloxS3UseProxyFromEnv = "spark.gluten.velox.s3UseProxyFromEnv";
  const std::string kVeloxS3UseProxyFromEnvDefault = "false";

  // Payload signing policy
  const std::string kVeloxS3PayloadSigningPolicy = "spark.gluten.velox.s3PayloadSigningPolicy";
  const std::string kVeloxS3PayloadSigningPolicyDefault = "Never";

  // Log location of AWS C++ SDK
  const std::string kVeloxS3LogLocation = "spark.gluten.velox.s3LogLocation";

  // Whether to upload S3 multipart parts asynchronously.
  const std::string kVeloxS3UploadPartAsync = "spark.gluten.velox.s3UploadPartAsync";

  // Maximum number of in-flight S3 part uploads per file.
  const std::string kVeloxS3MaxConcurrentUploadNum = "spark.gluten.velox.s3MaxConcurrentUploadNum";

  // Number of shared S3 part upload threads.
  const std::string kVeloxS3UploadThreads = "spark.gluten.velox.s3UploadThreads";

  const std::unordered_map<S3Config::Keys, std::pair<std::string, std::optional<std::string>>> sparkSuffixes = {
      {S3Config::Keys::kAccessKey, std::make_pair("access.key", std::nullopt)},
      {S3Config::Keys::kSecretKey, std::make_pair("secret.key", std::nullopt)},
      {S3Config::Keys::kEndpoint, std::make_pair("endpoint", std::nullopt)},
      {S3Config::Keys::kSSLEnabled, std::make_pair("connection.ssl.enabled", "false")},
      {S3Config::Keys::kPathStyleAccess, std::make_pair("path.style.access", "false")},
      {S3Config::Keys::kMaxAttempts, std::make_pair("retry.limit", std::nullopt)},
      {S3Config::Keys::kRetryMode, std::make_pair("retry.mode", "legacy")},
      {S3Config::Keys::kMaxConnections, std::make_pair("connection.maximum", "25")},
      {S3Config::Keys::kSocketTimeout, std::make_pair("connection.timeout", "200s")},
      {S3Config::Keys::kConnectTimeout, std::make_pair("connection.establish.timeout", "30s")},
      {S3Config::Keys::kUseInstanceCredentials, std::make_pair("instance.credentials", "false")},
      {S3Config::Keys::kIamRole, std::make_pair("iam.role", std::nullopt)},
      {S3Config::Keys::kIamRoleSessionName, std::make_pair("iam.role.session.name", "gluten-session")},
      {S3Config::Keys::kEndpointRegion, std::make_pair("endpoint.region", std::nullopt)},
      {S3Config::Keys::kIMDSEnabled, std::make_pair("aws.imds.enabled", "true")},
  };

  // get Velox S3 config key from Spark Suffix.
  auto getVeloxKey = [&](std::string_view suffix) {
    for (const auto& [key, value] : sparkSuffixes) {
      if (value.first == suffix) {
        return std::optional<S3Config::Keys>(key);
      }
    }
    return std::optional<S3Config::Keys>(std::nullopt);
  };

  auto sparkBaseConfigValue = [&](S3Config::Keys key) {
    std::stringstream ss;
    auto keyValue = sparkSuffixes.find(key)->second;
    ss << kSparkHadoopS3Prefix << keyValue.first;
    auto sparkKey = ss.str();
    if (conf->valueExists(sparkKey)) {
      return static_cast<std::optional<std::string>>(conf->get<std::string>(sparkKey));
    }
    // Return default value.
    return keyValue.second;
  };

  auto setConfigIfPresent = [&](S3Config::Keys key) {
    auto sparkConfig = sparkBaseConfigValue(key);
    if (sparkConfig.has_value()) {
      hiveConfMap[S3Config::baseConfigKey(key)] = sparkConfig.value();
    }
  };

  auto setFromEnvOrConfigIfPresent = [&](std::string_view envName, S3Config::Keys key) {
    const char* envValue = std::getenv(envName.data());
    if (envValue != nullptr) {
      hiveConfMap[S3Config::baseConfigKey(key)] = std::string(envValue);
    } else {
      setConfigIfPresent(key);
    }
  };

  auto setGlutenS3ConfigIfPresent = [&](const std::string& glutenKey, std::string_view s3Suffix) {
    auto value = conf->get<std::string>(glutenKey);
    if (value.has_value()) {
      hiveConfMap[std::string(S3Config::kS3Prefix) + std::string(s3Suffix)] = value.value();
    }
  };

  setFromEnvOrConfigIfPresent("AWS_ENDPOINT", S3Config::Keys::kEndpoint);
  setFromEnvOrConfigIfPresent("AWS_MAX_ATTEMPTS", S3Config::Keys::kMaxAttempts);
  setFromEnvOrConfigIfPresent("AWS_RETRY_MODE", S3Config::Keys::kRetryMode);
  setFromEnvOrConfigIfPresent("AWS_ACCESS_KEY_ID", S3Config::Keys::kAccessKey);
  setFromEnvOrConfigIfPresent("AWS_SECRET_ACCESS_KEY", S3Config::Keys::kSecretKey);
  setConfigIfPresent(S3Config::Keys::kUseInstanceCredentials);
  setConfigIfPresent(S3Config::Keys::kIamRole);
  setConfigIfPresent(S3Config::Keys::kIamRoleSessionName);
  setConfigIfPresent(S3Config::Keys::kSSLEnabled);
  setConfigIfPresent(S3Config::Keys::kPathStyleAccess);
  setConfigIfPresent(S3Config::Keys::kMaxConnections);
  setConfigIfPresent(S3Config::Keys::kSocketTimeout);
  setConfigIfPresent(S3Config::Keys::kConnectTimeout);
  setConfigIfPresent(S3Config::Keys::kEndpointRegion);
  setConfigIfPresent(S3Config::Keys::kIMDSEnabled);

  hiveConfMap[S3Config::kS3LogLevel] = conf->get<std::string>(kVeloxAwsSdkLogLevel, kVeloxAwsSdkLogLevelDefault);
  hiveConfMap[S3Config::baseConfigKey(S3Config::Keys::kUseProxyFromEnv)] =
      conf->get<std::string>(kVeloxS3UseProxyFromEnv, kVeloxS3UseProxyFromEnvDefault);
  hiveConfMap[S3Config::kS3PayloadSigningPolicy] =
      conf->get<std::string>(kVeloxS3PayloadSigningPolicy, kVeloxS3PayloadSigningPolicyDefault);
  auto logLocation = conf->get<std::string>(kVeloxS3LogLocation);
  if (logLocation.has_value()) {
    hiveConfMap[S3Config::kS3LogLocation] = logLocation.value();
  };
  setGlutenS3ConfigIfPresent(kVeloxS3UploadPartAsync, "part-upload-async");
  setGlutenS3ConfigIfPresent(kVeloxS3MaxConcurrentUploadNum, "max-concurrent-upload-num");
  setGlutenS3ConfigIfPresent(kVeloxS3UploadThreads, "upload-threads");

  // Convert all Spark bucket configs to Velox bucket configs.
  for (const auto& [key, value] : conf->rawConfigs()) {
    if (key.find(kSparkHadoopS3BucketPrefix) == 0) {
      std::string_view skey = key;
      auto remaining = skey.substr(kSparkHadoopS3BucketPrefix.size());
      int dot = remaining.find(".");
      auto bucketName = remaining.substr(0, dot);
      auto suffix = remaining.substr(dot + 1);
      auto veloxKey = getVeloxKey(suffix);

      if (veloxKey.has_value()) {
        hiveConfMap[S3Config::bucketConfigKey(veloxKey.value(), bucketName)] = value;
      }
    }
  }
#endif
}

void getGcsHiveConfig(
    std::shared_ptr<facebook::velox::config::ConfigBase> conf,
    FileSystemType fsType,
    std::unordered_map<std::string, std::string>& hiveConfMap) {
#ifdef ENABLE_GCS
  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#api-client-configuration
  auto gsStorageRootUrl = conf->get<std::string>("spark.hadoop.fs.gs.storage.root.url");
  if (gsStorageRootUrl.has_value()) {
    std::string gcsEndpoint = gsStorageRootUrl.value();

    if (!gcsEndpoint.empty()) {
      hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsEndpoint] = gcsEndpoint;
    }
  }

  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#http-transport-configuration
  // https://cloud.google.com/cpp/docs/reference/storage/latest/classgoogle_1_1cloud_1_1storage_1_1LimitedErrorCountRetryPolicy
  auto gsMaxRetryCount = conf->get<std::string>("spark.hadoop.fs.gs.http.max.retry");
  if (gsMaxRetryCount.has_value()) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsMaxRetryCount] = gsMaxRetryCount.value();
  }

  // https://cloud.google.com/cpp/docs/reference/storage/latest/classgoogle_1_1cloud_1_1storage_1_1LimitedTimeRetryPolicy
  auto gsMaxRetryTime = conf->get<std::string>("spark.hadoop.fs.gs.http.max.retry-time");
  if (gsMaxRetryTime.has_value()) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsMaxRetryTime] = gsMaxRetryTime.value();
  }

  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#authentication
  auto gsAuthType = conf->get<std::string>("spark.hadoop.fs.gs.auth.type");
  auto gsAuthServiceAccountJsonKeyfile = conf->get<std::string>("spark.hadoop.fs.gs.auth.service.account.json.keyfile");
  if (gsAuthType.has_value() && gsAuthType.value() == "SERVICE_ACCOUNT_JSON_KEYFILE") {
    if (gsAuthServiceAccountJsonKeyfile.has_value()) {
      hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsCredentialsPath] =
          gsAuthServiceAccountJsonKeyfile.value();
    } else {
      LOG(WARNING) << "STARTUP: conf spark.hadoop.fs.gs.auth.type is set to SERVICE_ACCOUNT_JSON_KEYFILE, "
                      "however conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set";
      throw GlutenException("Conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set");
    }
  } else if (gsAuthServiceAccountJsonKeyfile.has_value()) {
    LOG(WARNING) << "STARTUP: conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is set, "
                    "but conf spark.hadoop.fs.gs.auth.type is not SERVICE_ACCOUNT_JSON_KEYFILE";
    throw GlutenException("Conf spark.hadoop.fs.gs.auth.type is missing or incorrect");
  }
#endif
}

void getAbfsHiveConfig(
    std::shared_ptr<facebook::velox::config::ConfigBase> conf,
    FileSystemType fsType,
    std::unordered_map<std::string, std::string>& hiveConfMap) {
#ifdef ENABLE_ABFS
  std::string_view kSparkHadoopPrefix = "spark.hadoop.";
  std::string_view kSparkHadoopAbfsPrefix = "spark.hadoop.fs.azure.";
  for (const auto& [key, value] : conf->rawConfigs()) {
    if (key.find(kSparkHadoopAbfsPrefix) == 0) {
      // Remove the SparkHadoopPrefix
      hiveConfMap[key.substr(kSparkHadoopPrefix.size())] = value;
    }
  }
#endif
}

std::string parquetSessionProperty(std::string_view key) {
  return facebook::velox::dwio::common::formatConfigPrefix(facebook::velox::dwio::common::FileFormat::PARQUET, "_") +
      std::string(key);
}

std::string orcSessionProperty(std::string_view key) {
  return facebook::velox::dwio::common::formatConfigPrefix(facebook::velox::dwio::common::FileFormat::ORC, "_") +
      std::string(key);
}

} // namespace

std::unordered_map<std::string, std::string> getQueryContextConf(
    const facebook::velox::config::ConfigBase* conf,
    int32_t partitionId) {
  using namespace facebook;
  using facebook::velox::functions::sparksql::SparkQueryConfig;
  std::unordered_map<std::string, std::string> configs = {};
  // Find batch size from Spark confs. If found, set the preferred and max batch size.
  configs[velox::core::QueryConfig::kPreferredOutputBatchRows] =
      std::to_string(conf->get<uint32_t>(kSparkBatchSize, 4096));
  configs[velox::core::QueryConfig::kMaxOutputBatchRows] = std::to_string(conf->get<uint32_t>(kSparkBatchSize, 4096));
  configs[velox::core::QueryConfig::kPreferredOutputBatchBytes] =
      std::to_string(conf->get<uint64_t>(kVeloxPreferredBatchBytes, 10L << 20));
  try {
    configs[SparkQueryConfig::qualify(SparkQueryConfig::kAnsiEnabled)] = conf->get<std::string>(kAnsiEnabled, "false");
    configs[velox::core::QueryConfig::kSessionTimezone] =
        normalizeSessionTimezone(conf->get<std::string>(kSessionTimezone, ""));
    // Adjust timestamp according to the above configured session timezone.
    configs[velox::core::QueryConfig::kAdjustTimestampToTimezone] = "true";

    {
      // Find offheap size from Spark confs. If found, set the max memory usage of partial aggregation.
      // Partial aggregation memory configurations.
      // TODO: Move the calculations to Java side.
      auto offHeapMemory = conf->get<int64_t>(kSparkTaskOffHeapMemory, facebook::velox::memory::kMaxMemory);
      auto maxPartialAggregationMemory = std::max<int64_t>(
          1 << 24,
          conf->get<int64_t>(kMaxPartialAggregationMemory).has_value()
              ? conf->get<int64_t>(kMaxPartialAggregationMemory).value()
              : static_cast<int64_t>(conf->get<double>(kMaxPartialAggregationMemoryRatio, 0.1) * offHeapMemory));
      auto maxExtendedPartialAggregationMemory = std::max<int64_t>(
          1 << 26,
          conf->get<int64_t>(kMaxExtendedPartialAggregationMemory).has_value()
              ? conf->get<int64_t>(kMaxExtendedPartialAggregationMemory).value()
              : static_cast<int64_t>(
                    conf->get<double>(kMaxExtendedPartialAggregationMemoryRatio, 0.15) * offHeapMemory));
      configs[velox::core::QueryConfig::kMaxPartialAggregationMemory] = std::to_string(maxPartialAggregationMemory);
      configs[velox::core::QueryConfig::kMaxExtendedPartialAggregationMemory] =
          std::to_string(maxExtendedPartialAggregationMemory);
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinPct] =
          std::to_string(conf->get<int32_t>(kAbandonPartialAggregationMinPct, 90));
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinRows] =
          std::to_string(conf->get<int32_t>(kAbandonPartialAggregationMinRows, 100000));
    }
    // Spill configs
    if (conf->get<std::string>(kSpillStrategy, kSpillStrategyDefaultValue) == "none") {
      configs[velox::core::QueryConfig::kSpillEnabled] = "false";
    } else {
      configs[velox::core::QueryConfig::kSpillEnabled] = "true";
    }
    configs[velox::core::QueryConfig::kAggregationSpillEnabled] =
        std::to_string(conf->get<bool>(kAggregationSpillEnabled, true));
    configs[velox::core::QueryConfig::kJoinSpillEnabled] = std::to_string(conf->get<bool>(kJoinSpillEnabled, true));
    configs[velox::core::QueryConfig::kOrderBySpillEnabled] =
        std::to_string(conf->get<bool>(kOrderBySpillEnabled, true));
    configs[velox::core::QueryConfig::kWindowSpillEnabled] = std::to_string(conf->get<bool>(kWindowSpillEnabled, true));
    configs[velox::core::QueryConfig::kMaxSpillLevel] = std::to_string(conf->get<int32_t>(kMaxSpillLevel, 4));
    configs[velox::core::QueryConfig::kMaxSpillFileSize] =
        std::to_string(conf->get<uint64_t>(kMaxSpillFileSize, 1L * 1024 * 1024 * 1024));
    configs[velox::core::QueryConfig::kSpillNumMaxMergeFiles] =
        std::to_string(conf->get<uint32_t>(kSpillNumMaxMergeFiles, 0));
    configs[velox::core::QueryConfig::kMaxSpillRunRows] =
        std::to_string(conf->get<uint64_t>(kMaxSpillRunRows, 3L * 1024 * 1024));
    configs[velox::core::QueryConfig::kMaxSpillBytes] =
        std::to_string(conf->get<uint64_t>(kMaxSpillBytes, 107374182400LL));
    configs[velox::core::QueryConfig::kSpillWriteBufferSize] =
        std::to_string(conf->get<uint64_t>(kShuffleSpillDiskWriteBufferSize, 1L * 1024 * 1024));
    configs[velox::core::QueryConfig::kSpillReadBufferSize] =
        std::to_string(conf->get<int32_t>(kSpillReadBufferSize, 1L * 1024 * 1024));
    configs[velox::core::QueryConfig::kSpillStartPartitionBit] =
        std::to_string(conf->get<uint8_t>(kSpillStartPartitionBit, 48));
    configs[velox::core::QueryConfig::kSpillNumPartitionBits] =
        std::to_string(conf->get<uint8_t>(kSpillPartitionBits, 3));
    configs[velox::core::QueryConfig::kSpillableReservationGrowthPct] =
        std::to_string(conf->get<uint8_t>(kSpillableReservationGrowthPct, 25));
    configs[velox::core::QueryConfig::kSpillPrefixSortEnabled] =
        conf->get<std::string>(kSpillPrefixSortEnabled, "false");
    if (conf->get<bool>(kSparkShuffleSpillCompress, true)) {
      configs[velox::core::QueryConfig::kSpillCompressionKind] =
          conf->get<std::string>(kSpillCompressionKind, conf->get<std::string>(kCompressionKind, "lz4"));
    } else {
      configs[velox::core::QueryConfig::kSpillCompressionKind] = "none";
    }

    configs[velox::core::QueryConfig::kHashProbeDynamicFilterPushdownEnabled] =
        std::to_string(conf->get<bool>(kHashProbeDynamicFilterPushdownEnabled, true));
    configs[velox::core::QueryConfig::kHashProbeBloomFilterPushdownMaxSize] =
        std::to_string(conf->get<uint64_t>(kHashProbeBloomFilterPushdownMaxSize, 0));
    configs[velox::core::QueryConfig::kBypassHashProbeBloomFilterMinRows] = std::to_string(
        conf->get<int32_t>(kHashProbeBloomFilterBypassMinRows, kHashProbeBloomFilterBypassMinRowsDefault));
    configs[velox::core::QueryConfig::kBypassHashProbeBloomFilterMinPct] =
        std::to_string(conf->get<int32_t>(kHashProbeBloomFilterBypassMinPct, kHashProbeBloomFilterBypassMinPctDefault));

    if (const auto opt = conf->get<std::string>(kSparkBloomFilterExpectedNumItems)) {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterExpectedNumItems)] = opt.value();
    }
    if (const auto opt = conf->get<std::string>(kSparkBloomFilterNumBits)) {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterNumBits)] = opt.value();
    }
    if (const auto opt = conf->get<std::string>(kSparkBloomFilterMaxNumBits)) {
      // Velox will check memory cannot exceed 4194304.
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterMaxNumBits)] = opt.value();
    }
    if (const auto opt = conf->get<std::string>(kSparkBloomFilterMaxNumItems)) {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterMaxNumItems)] = opt.value();
    }
    // spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver takes no effect if
    // spark.gluten.sql.columnar.backend.velox.IOThreads is set to 0
    configs[velox::core::QueryConfig::kMaxSplitPreloadPerDriver] =
        std::to_string(conf->get<int32_t>(kVeloxSplitPreloadPerDriver, 2));

    // hashtable build optimizations
    configs[velox::core::QueryConfig::kAbandonDedupHashMapMinRows] =
        std::to_string(conf->get<int32_t>(kAbandonDedupHashMapMinRows, 100000));
    configs[velox::core::QueryConfig::kAbandonDedupHashMapMinPct] =
        std::to_string(conf->get<int32_t>(kAbandonDedupHashMapMinPct, 0));

    // Disable driver cpu time slicing.
    configs[velox::core::QueryConfig::kDriverCpuTimeSliceLimitMs] = "0";

    configs[SparkQueryConfig::qualify(SparkQueryConfig::kPartitionId)] = std::to_string(partitionId);

    // Spark keeps each call of a non-deterministic function separate, with its own state. Without
    // this, Velox merges identical calls such as two monotonically_increasing_id() into one.
    configs[velox::core::QueryConfig::kExprDedupNonDeterministic] = "false";

    // Enable Spark legacy date formatter if spark.sql.legacy.timeParserPolicy is set to 'LEGACY'
    // or 'legacy'
    if (conf->get<std::string>(kSparkLegacyTimeParserPolicy, "") == "LEGACY") {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kLegacyDateFormatter)] = "true";
    } else {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kLegacyDateFormatter)] = "false";
    }

    if (conf->get<std::string>(kSparkMapKeyDedupPolicy, "") == "EXCEPTION") {
      configs[velox::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "true";
    } else {
      configs[velox::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "false";
    }

    configs[SparkQueryConfig::qualify(SparkQueryConfig::kLegacyStatisticalAggregate)] =
        std::to_string(conf->get<bool>(kSparkLegacyStatisticalAggregate, false));

    configs[SparkQueryConfig::qualify(SparkQueryConfig::kJsonIgnoreNullFields)] =
        std::to_string(conf->get<bool>(kSparkJsonIgnoreNullFields, true));

    configs[SparkQueryConfig::qualify(SparkQueryConfig::kDecimalToFloatHighPrecisionCastEnabled)] =
        std::to_string(conf->get<bool>(kDecimalToFloatHighPrecisionCastEnabled, false));

    configs[velox::core::QueryConfig::kExprMaxCompiledRegexes] =
        std::to_string(conf->get<int32_t>(kExprMaxCompiledRegexes, 100));

#ifdef GLUTEN_ENABLE_GPU
    configs[velox::cudf_velox::CudfConfig::kCudfEnabled] = std::to_string(conf->get<bool>(kCudfEnabled, false));
#endif

    const auto setIfExists = [&](const std::string& glutenKey, const std::string& veloxKey) {
      const auto valueOptional = conf->get<std::string>(glutenKey);
      if (valueOptional.has_value()) {
        configs[veloxKey] = valueOptional.value();
      }
    };
    setIfExists(kQueryTraceEnabled, velox::core::QueryConfig::kQueryTraceEnabled);
    setIfExists(kQueryTraceDir, velox::core::QueryConfig::kQueryTraceDir);
    setIfExists(kQueryTraceMaxBytes, velox::core::QueryConfig::kQueryTraceMaxBytes);
    setIfExists(kQueryTraceTaskRegExp, velox::core::QueryConfig::kQueryTraceTaskRegExp);
    setIfExists(kOpTraceDirectoryCreateConfig, velox::core::QueryConfig::kOpTraceDirectoryCreateConfig);

    overwriteVeloxConf(conf, configs, kDynamicBackendConfPrefix);
  } catch (const std::invalid_argument& err) {
    std::string errDetails = err.what();
    throw std::runtime_error("Invalid conf arg: " + errDetails);
  }
  return configs;
}

std::shared_ptr<facebook::velox::config::ConfigBase> createHiveConnectorSessionConfig(
    const std::shared_ptr<facebook::velox::config::ConfigBase>& conf) {
  // The configs below are used at session level.
  std::unordered_map<std::string, std::string> configs = {};
  // The semantics of reading as lower case is opposite with case-sensitive.
  configs[facebook::velox::connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession] =
      !conf->get<bool>(kCaseSensitive, false) ? "true" : "false";
  configs[facebook::velox::connector::hive::HiveConfig::kPartitionPathAsLowerCaseSession] = "false";
  configs[parquetSessionProperty(facebook::velox::parquet::ParquetConfig::kWriterTimestampUnitSession)] =
      std::string("6");
  configs[facebook::velox::connector::hive::HiveConfig::kReadTimestampUnitSession] = std::string("6");
  configs[facebook::velox::connector::hive::HiveConfig::kMaxPartitionsPerWritersSession] =
      conf->get<std::string>(kMaxPartitions, "10000");
  configs[facebook::velox::connector::hive::HiveConfig::kParquetMaxTargetFileSizeSession] =
      conf->get<std::string>(kParquetMaxTargetFileSize, "0B"); // 0 means no limit on target file size
  configs[facebook::velox::connector::hive::HiveConfig::kIgnoreMissingFilesSession] =
      conf->get<bool>(kIgnoreMissingFiles, false) ? "true" : "false";
  configs[facebook::velox::connector::hive::HiveConfig::kAllowInt32NarrowingSession] =
      conf->get<bool>(kAllowInt32Narrowing, true) ? "true" : "false";
  configs[parquetSessionProperty(facebook::velox::parquet::ParquetConfig::kWriterPageSizeSession)] =
      conf->get<std::string>(kWriteParquetPageSizeBytes, "1MB");
  configs[parquetSessionProperty(facebook::velox::parquet::ParquetConfig::kWriterRowGroupSizeSession)] =
      conf->get<std::string>(kColumnarParquetWriteBlockSize, "128MB");
  configs[parquetSessionProperty(facebook::velox::parquet::ParquetConfig::kWriterDictionaryPageSizeLimitSession)] =
      conf->get<std::string>(kWriteParquetDictSizeBytes, "2MB");
  configs[parquetSessionProperty(facebook::velox::parquet::ParquetConfig::kNullStructIfAllFieldsMissingSession)] =
      conf->get<bool>(kLegacyParquetReturnNullStructIfAllFieldsMissing, true) ? "true" : "false";

  overwriteVeloxConf(conf.get(), configs, kDynamicBackendConfPrefix);
  return std::make_shared<facebook::velox::config::ConfigBase>(std::move(configs));
}

std::string getConfigValue(
    const std::unordered_map<std::string, std::string>& confMap,
    const std::string& key,
    const std::optional<std::string>& fallbackValue) {
  auto got = confMap.find(key);
  if (got == confMap.end()) {
    if (fallbackValue == std::nullopt) {
      throw std::runtime_error("No such config key: " + key);
    }
    return fallbackValue.value();
  }
  return got->second;
}

std::shared_ptr<facebook::velox::config::ConfigBase> createHiveConnectorConfig(
    const std::shared_ptr<facebook::velox::config::ConfigBase>& conf,
    FileSystemType fsType) {
  std::unordered_map<std::string, std::string> hiveConfMap;

  switch (fsType) {
    case FileSystemType::kS3:
      getS3HiveConfig(conf, fsType, hiveConfMap);
      break;
    case FileSystemType::kAbfs:
      getAbfsHiveConfig(conf, fsType, hiveConfMap);
      break;
    case FileSystemType::kGcs:
      getGcsHiveConfig(conf, fsType, hiveConfMap);
      break;
    case FileSystemType::kHdfs:
      break;
    case FileSystemType::kAll:
      getS3HiveConfig(conf, fsType, hiveConfMap);
      getAbfsHiveConfig(conf, fsType, hiveConfMap);
      getGcsHiveConfig(conf, fsType, hiveConfMap);
      break;
    default:
      GLUTEN_UNREACHABLE();
  }

  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kEnableFileHandleCache] =
      conf->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false";
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kNumCacheFileHandles] =
      std::to_string(conf->get<int32_t>(kVeloxNumCacheFileHandles, kVeloxNumCacheFileHandlesDefault));
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kFileHandleExpirationDurationMs] = std::to_string(
      conf->get<int64_t>(kVeloxFileHandleExpirationDurationMs, kVeloxFileHandleExpirationDurationMsDefault));
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kMaxCoalescedBytes] =
      conf->get<std::string>(kMaxCoalescedBytes, "67108864"); // 64M
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kMaxCoalescedDistance] =
      conf->get<std::string>(kMaxCoalescedDistance, "512KB"); // 512KB
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kPrefetchRowGroups] =
      conf->get<std::string>(kPrefetchRowGroups, "1");
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kLoadQuantum] =
      conf->get<std::string>(kLoadQuantum, "268435456"); // 256M

  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kFilePreloadThreshold] =
      conf->get<std::string>(kFilePreloadThreshold, "1048576"); // 1M

  // read as UTC
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kReadTimestampPartitionValueAsLocalTime] = "false";

  overwriteVeloxConf(conf.get(), hiveConfMap, kStaticBackendConfPrefix);
  return std::make_shared<facebook::velox::config::ConfigBase>(std::move(hiveConfMap));
}

void overwriteVeloxConf(
    const facebook::velox::config::ConfigBase* from,
    std::unordered_map<std::string, std::string>& to,
    const std::string& prefix) {
  for (const auto& [k, v] : from->rawConfigs()) {
    if (k.starts_with(prefix)) {
      to[k.substr(prefix.size())] = v;
    }
  }
}

} // namespace gluten

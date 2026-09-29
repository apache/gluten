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

#include "filesystem/GlutenS3FileSystem.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/AdaptiveRetryStrategy.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <folly/Conv.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/synchronization/ThrottledLifoSem.h>
#include <glog/logging.h>

#include "velox/common/base/StatsReporter.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Counters.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/dwio/common/DataBuffer.h"

namespace gluten {

namespace velox = facebook::velox;
namespace filesystems = facebook::velox::filesystems;

namespace {

using namespace facebook::velox::filesystems;

constexpr std::string_view kPartUploadAsync{"part-upload-async"};
constexpr std::string_view kPartUploadAsyncLegacy{"uploadPartAsync"};
constexpr std::string_view kMaxConcurrentUploadNum{"max-concurrent-upload-num"};
constexpr std::string_view kUploadThreads{"upload-threads"};
constexpr uint32_t kDefaultMaxConcurrentUploadNum{4};
constexpr uint32_t kDefaultUploadThreads{16};
constexpr const char* kApplicationOctetStream = "application/octet-stream";

std::string baseS3ConfigKey(std::string_view suffix) {
  std::string key(filesystems::S3Config::kS3Prefix);
  key.append(suffix);
  return key;
}

std::string bucketS3ConfigKey(std::string_view bucketName, std::string_view suffix) {
  std::string key(filesystems::S3Config::kS3BucketPrefix);
  key.append(bucketName);
  key.append(".");
  key.append(suffix);
  return key;
}

std::optional<std::string> getS3ConfigValue(
    const std::shared_ptr<const velox::config::ConfigBase>& config,
    std::string_view bucketName,
    std::string_view suffix) {
  if (auto value = config->get<std::string>(bucketS3ConfigKey(bucketName, suffix))) {
    return value;
  }
  return config->get<std::string>(baseS3ConfigKey(suffix));
}

std::optional<std::string> getS3ConfigValue(
    const std::shared_ptr<const velox::config::ConfigBase>& config,
    std::string_view bucketName,
    std::string_view suffix,
    std::string_view legacySuffix) {
  auto value = getS3ConfigValue(config, bucketName, suffix);
  if (value.has_value()) {
    return value;
  }
  return getS3ConfigValue(config, bucketName, legacySuffix);
}

uint32_t getUInt32S3Config(
    const std::shared_ptr<const velox::config::ConfigBase>& config,
    std::string_view bucketName,
    std::string_view suffix,
    uint32_t defaultValue) {
  const auto value = getS3ConfigValue(config, bucketName, suffix);
  return value.has_value() ? folly::to<uint32_t>(value.value()) : defaultValue;
}

// Supported values are "Always", "RequestDependent", "Never"(default).
Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy inferPayloadSign(std::string sign) {
  std::transform(sign.begin(), sign.end(), sign.begin(), [](unsigned char c) { return std::toupper(c); });
  if (sign == "ALWAYS") {
    return Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always;
  }
  if (sign == "REQUESTDEPENDENT") {
    return Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent;
  }
  return Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
}

std::optional<std::shared_ptr<Aws::Client::RetryStrategy>> getRetryStrategy(
    const std::shared_ptr<filesystems::S3Config>& s3Config) {
  auto retryMode = s3Config->retryMode();
  auto maxAttempts = s3Config->maxAttempts();
  if (!retryMode.has_value()) {
    return std::nullopt;
  }

  if (retryMode.value() == "standard") {
    if (maxAttempts.has_value()) {
      VELOX_USER_CHECK_GE(
          maxAttempts.value(),
          0,
          "Invalid configuration: specified 'hive.s3.max-attempts' value {} is < 0.",
          maxAttempts.value());
      return std::make_shared<Aws::Client::StandardRetryStrategy>(maxAttempts.value());
    }
    return std::make_shared<Aws::Client::StandardRetryStrategy>();
  }

  if (retryMode.value() == "adaptive") {
    if (maxAttempts.has_value()) {
      VELOX_USER_CHECK_GE(
          maxAttempts.value(),
          0,
          "Invalid configuration: specified 'hive.s3.max-attempts' value {} is < 0.",
          maxAttempts.value());
      return std::make_shared<Aws::Client::AdaptiveRetryStrategy>(maxAttempts.value());
    }
    return std::make_shared<Aws::Client::AdaptiveRetryStrategy>();
  }

  if (retryMode.value() == "legacy") {
    if (maxAttempts.has_value()) {
      VELOX_USER_CHECK_GE(
          maxAttempts.value(),
          0,
          "Invalid configuration: specified 'hive.s3.max-attempts' value {} is < 0.",
          maxAttempts.value());
      return std::make_shared<Aws::Client::DefaultRetryStrategy>(maxAttempts.value());
    }
    return std::make_shared<Aws::Client::DefaultRetryStrategy>();
  }

  VELOX_USER_FAIL("Invalid retry mode for S3: {}", retryMode.value());
  return std::nullopt;
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> getCredentialsProvider(const filesystems::S3Config& s3Config) {
  VELOX_USER_CHECK(
      !s3Config.credentialsProvider().has_value(),
      "Gluten async S3 multipart upload does not support custom AWS credentials providers yet.");

  auto accessKey = s3Config.accessKey();
  auto secretKey = s3Config.secretKey();
  const auto iamRole = s3Config.iamRole();

  int keyCount = accessKey.has_value() + secretKey.has_value();
  VELOX_USER_CHECK(keyCount != 1, "Invalid configuration: both access key and secret key must be specified");

  int configCount =
      (accessKey.has_value() && secretKey.has_value()) + iamRole.has_value() + s3Config.useInstanceCredentials();
  VELOX_USER_CHECK(
      configCount <= 1,
      "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");

  if (accessKey.has_value() && secretKey.has_value()) {
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
        filesystems::awsString(accessKey.value()), filesystems::awsString(secretKey.value()));
  }

  if (s3Config.useInstanceCredentials()) {
    return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
  }

  if (iamRole.has_value()) {
    return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
        filesystems::awsString(iamRole.value()), filesystems::awsString(s3Config.iamRoleSessionName()));
  }

  return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

std::shared_ptr<Aws::S3::S3Client> createWriteClient(const std::shared_ptr<filesystems::S3Config>& s3Config) {
  Aws::Client::ClientConfigurationInitValues initValues;
  initValues.shouldDisableIMDS = !s3Config->useIMDS();
  Aws::S3::S3ClientConfiguration clientConfig(initValues);
  clientConfig.checksumConfig.requestChecksumCalculation = Aws::Client::RequestChecksumCalculation::WHEN_REQUIRED;
  clientConfig.checksumConfig.responseChecksumValidation = Aws::Client::ResponseChecksumValidation::WHEN_REQUIRED;

  if (s3Config->endpoint().has_value()) {
    clientConfig.endpointOverride = s3Config->endpoint().value();
  }
  if (s3Config->endpointRegion().has_value()) {
    clientConfig.region = s3Config->endpointRegion().value();
  }
  if (s3Config->useProxyFromEnv()) {
    auto proxyConfig =
        filesystems::S3ProxyConfigurationBuilder(s3Config->endpoint().has_value() ? s3Config->endpoint().value() : "")
            .useSsl(s3Config->useSSL())
            .build();
    if (proxyConfig.has_value()) {
      clientConfig.proxyScheme = Aws::Http::SchemeMapper::FromString(proxyConfig.value().scheme().c_str());
      clientConfig.proxyHost = filesystems::awsString(proxyConfig.value().host());
      clientConfig.proxyPort = proxyConfig.value().port();
      clientConfig.proxyUserName = filesystems::awsString(proxyConfig.value().username());
      clientConfig.proxyPassword = filesystems::awsString(proxyConfig.value().password());
    }
  }

  clientConfig.scheme = s3Config->useSSL() ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;

  if (s3Config->connectTimeout().has_value()) {
    clientConfig.connectTimeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        velox::config::toDuration(s3Config->connectTimeout().value()))
                                        .count();
  }
  if (s3Config->socketTimeout().has_value()) {
    clientConfig.requestTimeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        velox::config::toDuration(s3Config->socketTimeout().value()))
                                        .count();
  }
  if (s3Config->maxConnections().has_value()) {
    clientConfig.maxConnections = s3Config->maxConnections().value();
  }

  auto retryStrategy = getRetryStrategy(s3Config);
  if (retryStrategy.has_value()) {
    clientConfig.retryStrategy = retryStrategy.value();
  }

  clientConfig.useVirtualAddressing = s3Config->useVirtualAddressing();
  clientConfig.payloadSigningPolicy = inferPayloadSign(s3Config->payloadSigningPolicy());

  return std::make_shared<Aws::S3::S3Client>(
      getCredentialsProvider(*s3Config), nullptr /* endpointProvider */, clientConfig);
}

std::shared_ptr<folly::CPUThreadPoolExecutor> createUploadThreadPool(uint32_t uploadThreads) {
  return std::make_shared<folly::CPUThreadPoolExecutor>(
      uploadThreads, std::make_shared<folly::NamedThreadFactory>("s3-upload-thread"));
}

class GlutenS3WriteFile : public velox::WriteFile {
 public:
  GlutenS3WriteFile(
      std::string_view path,
      Aws::S3::S3Client* client,
      velox::memory::MemoryPool* pool,
      size_t minPartSize,
      uint32_t maxConcurrentUploadNum,
      std::shared_ptr<folly::CPUThreadPoolExecutor> uploadThreadPool)
      : client_(client),
        pool_(pool),
        minPartSize_(minPartSize),
        uploadThrottle_(std::make_unique<folly::ThrottledLifoSem>(maxConcurrentUploadNum)),
        uploadThreadPool_(std::move(uploadThreadPool)) {
    VELOX_CHECK_NOT_NULL(client_);
    VELOX_CHECK_NOT_NULL(pool_);
    VELOX_CHECK_NOT_NULL(uploadThreadPool_);
    filesystems::getBucketAndKeyFromPath(path, bucket_, key_);
    currentPart_ = std::make_unique<velox::dwio::common::DataBuffer<char>>(*pool_);
    currentPart_->reserve(minPartSize_);
    ensureObjectDoesNotExist();
    createBucketIfMissing();
    fileSize_ = 0;
  }

  ~GlutenS3WriteFile() override {
    cleanupMultipartUpload();
  }

  void append(std::string_view data) override {
    VELOX_CHECK(!closed(), "File is closed");
    if (data.size() + currentPart_->size() >= minPartSize_) {
      if (uploadState_.partNumber == 0) {
        createMultipartUploadRequest();
      }
      upload(data);
    } else {
      currentPart_->unsafeAppend(data.data(), data.size());
    }
    fileSize_ += data.size();
  }

  void flush() override {
    VELOX_CHECK(!closed(), "File is closed");
    VELOX_CHECK_LT(currentPart_->size(), minPartSize_);
  }

  void close() override {
    if (closed()) {
      return;
    }
    if (uploadState_.partNumber == 0) {
      putObjectRequest();
      currentPart_->clear();
      return;
    }

    try {
      RECORD_METRIC_VALUE(filesystems::kMetricS3StartedUploads);
      uploadPart({currentPart_->data(), currentPart_->size()}, true);
      waitForAsyncUploads();
      VELOX_CHECK_EQ(uploadState_.partNumber, uploadState_.completedParts.size());
      completeMultipartUpload();
      currentPart_->clear();
    } catch (...) {
      abortMultipartUpload();
      currentPart_->clear();
      throw;
    }
  }

  uint64_t size() const override {
    return fileSize_;
  }

 private:
  struct UploadState {
    Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;
    int64_t partNumber = 0;
    Aws::String id;
  };

  bool closed() const {
    return currentPart_->capacity() == 0;
  }

  bool multipartUploadInProgress() const {
    return !uploadState_.id.empty() && !multipartUploadCompleted_ && !multipartUploadAborted_;
  }

  void cleanupMultipartUpload() noexcept {
    if (!multipartUploadInProgress()) {
      return;
    }

    if (!uploadFutures_.empty()) {
      try {
        waitForAsyncUploads();
      } catch (const std::exception& e) {
        LOG(ERROR) << "Failed while waiting for S3 async uploads: " << e.what();
      } catch (...) {
        LOG(ERROR) << "Failed while waiting for S3 async uploads.";
      }
    }

    abortMultipartUpload();
  }

  void ensureObjectDoesNotExist() {
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(filesystems::awsString(bucket_));
    request.SetKey(filesystems::awsString(key_));
    RECORD_METRIC_VALUE(filesystems::kMetricS3MetadataCalls);
    auto objectMetadata = client_->HeadObject(request);
    if (!objectMetadata.IsSuccess()) {
      RECORD_METRIC_VALUE(filesystems::kMetricS3GetMetadataErrors);
    }
    RECORD_METRIC_VALUE(filesystems::kMetricS3GetObjectRetries, objectMetadata.GetRetryCount());
    VELOX_CHECK(!objectMetadata.IsSuccess(), "S3 object already exists: bucket={}, key={}", bucket_, key_);
  }

  void createBucketIfMissing() {
    Aws::S3::Model::HeadBucketRequest request;
    request.SetBucket(filesystems::awsString(bucket_));
    auto bucketMetadata = client_->HeadBucket(request);
    if (bucketMetadata.IsSuccess()) {
      return;
    }

    Aws::S3::Model::CreateBucketRequest createRequest;
    createRequest.SetBucket(filesystems::awsString(bucket_));
    auto outcome = client_->CreateBucket(createRequest);
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to create S3 bucket", bucket_, "");
  }

  void createMultipartUploadRequest() {
    Aws::S3::Model::CreateMultipartUploadRequest request;
    request.SetBucket(filesystems::awsString(bucket_));
    request.SetKey(filesystems::awsString(key_));
    request.SetContentType(kApplicationOctetStream);
    auto outcome = client_->CreateMultipartUpload(request);
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed initiating multiple part upload", bucket_, key_);
    uploadState_.id = outcome.GetResult().GetUploadId();
  }

  void putObjectRequest() {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(filesystems::awsString(bucket_));
    request.SetKey(filesystems::awsString(key_));
    request.SetContentType(kApplicationOctetStream);
    request.SetContentLength(currentPart_->size());
    request.SetBody(std::make_shared<filesystems::StringViewStream>(currentPart_->data(), currentPart_->size()));
    RECORD_METRIC_VALUE(filesystems::kMetricS3StartedUploads);
    auto outcome = client_->PutObject(request);
    if (outcome.IsSuccess()) {
      RECORD_METRIC_VALUE(filesystems::kMetricS3SuccessfulUploads);
    } else {
      RECORD_METRIC_VALUE(filesystems::kMetricS3FailedUploads);
    }
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed single object upload", bucket_, key_);
  }

  void completeMultipartUpload() {
    Aws::S3::Model::CompletedMultipartUpload completedUpload;
    completedUpload.SetParts(uploadState_.completedParts);
    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.SetBucket(filesystems::awsString(bucket_));
    request.SetKey(filesystems::awsString(key_));
    request.SetUploadId(uploadState_.id);
    request.SetMultipartUpload(std::move(completedUpload));

    auto outcome = client_->CompleteMultipartUpload(request);
    if (outcome.IsSuccess()) {
      RECORD_METRIC_VALUE(filesystems::kMetricS3SuccessfulUploads);
    } else {
      RECORD_METRIC_VALUE(filesystems::kMetricS3FailedUploads);
    }
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to complete multiple part upload", bucket_, key_);
    multipartUploadCompleted_ = true;
  }

  void abortMultipartUpload() noexcept {
    if (!multipartUploadInProgress()) {
      return;
    }

    try {
      Aws::S3::Model::AbortMultipartUploadRequest request;
      request.SetBucket(filesystems::awsString(bucket_));
      request.SetKey(filesystems::awsString(key_));
      request.SetUploadId(uploadState_.id);

      auto outcome = client_->AbortMultipartUpload(request);
      if (outcome.IsSuccess()) {
        multipartUploadAborted_ = true;
        return;
      }

      const auto& error = outcome.GetError();
      LOG(ERROR) << "Failed to abort S3 multipart upload: bucket=" << bucket_ << ", key=" << key_
                 << ", uploadId=" << uploadState_.id << ", message=" << error.GetMessage();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to abort S3 multipart upload: " << e.what();
    } catch (...) {
      LOG(ERROR) << "Failed to abort S3 multipart upload.";
    }
  }

  void upload(std::string_view data) {
    auto dataPtr = data.data();
    auto dataSize = data.size();
    auto remainingBufferSize = currentPart_->capacity() - currentPart_->size();
    currentPart_->unsafeAppend(dataPtr, remainingBufferSize);
    uploadPart({currentPart_->data(), currentPart_->size()});
    currentPart_->clear();
    currentPart_->reserve(minPartSize_);
    dataPtr += remainingBufferSize;
    dataSize -= remainingBufferSize;
    while (dataSize > minPartSize_) {
      uploadPart({dataPtr, minPartSize_});
      dataPtr += minPartSize_;
      dataSize -= minPartSize_;
    }
    currentPart_->unsafeAppend(dataPtr, dataSize);
  }

  void uploadPart(std::string_view part, bool isLast = false) {
    VELOX_CHECK(isLast || part.size() == minPartSize_);
    uploadThrottle_->wait();
    const int64_t partNumber = ++uploadState_.partNumber;
    auto uploadId = uploadState_.id;
    auto partData = std::make_shared<std::string>(part.data(), part.size());
    uploadFutures_.emplace_back(
        folly::via(uploadThreadPool_.get(), [this, uploadId, partNumber, partData = std::move(partData)]() {
          SCOPE_EXIT {
            uploadThrottle_->post();
          };
          return uploadPartSeq(uploadId, partNumber, *partData);
        }));
  }

  Aws::S3::Model::CompletedPart uploadPartSeq(const Aws::String& uploadId, int64_t partNumber, std::string_view part) {
    Aws::S3::Model::UploadPartRequest request;
    request.SetBucket(filesystems::awsString(bucket_));
    request.SetKey(filesystems::awsString(key_));
    request.SetUploadId(uploadId);
    request.SetPartNumber(partNumber);
    request.SetContentLength(part.size());
    request.SetBody(std::make_shared<filesystems::StringViewStream>(part.data(), part.size()));
    auto outcome = client_->UploadPart(request);
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to upload", bucket_, key_);

    auto result = outcome.GetResult();
    Aws::S3::Model::CompletedPart completedPart;
    completedPart.SetPartNumber(partNumber);
    completedPart.SetETag(result.GetETag());
    if (!result.GetChecksumCRC32().empty()) {
      completedPart.SetChecksumCRC32(result.GetChecksumCRC32());
    }
    return completedPart;
  }

  void waitForAsyncUploads() {
    if (uploadFutures_.empty()) {
      return;
    }

    auto results = folly::collectAll(std::move(uploadFutures_)).get();
    uploadFutures_.clear();
    std::exception_ptr firstException;
    for (auto& result : results) {
      if (result.hasException()) {
        if (!firstException) {
          try {
            result.throwUnlessValue();
          } catch (...) {
            firstException = std::current_exception();
          }
        }
        continue;
      }
      uploadState_.completedParts.push_back(std::move(result.value()));
    }
    std::sort(
        uploadState_.completedParts.begin(),
        uploadState_.completedParts.end(),
        [](const auto& left, const auto& right) { return left.GetPartNumber() < right.GetPartNumber(); });
    if (firstException) {
      std::rethrow_exception(firstException);
    }
  }

  Aws::S3::S3Client* const client_;
  velox::memory::MemoryPool* const pool_;
  std::unique_ptr<velox::dwio::common::DataBuffer<char>> currentPart_;
  std::string bucket_;
  std::string key_;
  size_t fileSize_ = -1;
  const size_t minPartSize_;
  UploadState uploadState_;
  std::unique_ptr<folly::ThrottledLifoSem> uploadThrottle_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> uploadThreadPool_;
  std::vector<folly::Future<Aws::S3::Model::CompletedPart>> uploadFutures_;
  bool multipartUploadCompleted_ = false;
  bool multipartUploadAborted_ = false;
};

std::shared_ptr<filesystems::FileSystem> glutenS3FileSystemFactory(
    std::string_view bucketName,
    std::shared_ptr<const velox::config::ConfigBase> config) {
  return std::make_shared<GlutenS3FileSystem>(bucketName, config);
}

} // namespace

GlutenS3FileSystem::GlutenS3FileSystem(
    std::string_view bucketName,
    const std::shared_ptr<const velox::config::ConfigBase>& config)
    : S3FileSystem(bucketName, config),
      bucketName_(bucketName),
      s3Config_(std::make_shared<filesystems::S3Config>(bucketName, config)) {
  if (uploadPartAsync()) {
    writeClient_ = createWriteClient(s3Config_);
  }
}

bool GlutenS3FileSystem::uploadPartAsync() const {
  auto value = getS3ConfigValue(config_, bucketName_, kPartUploadAsync, kPartUploadAsyncLegacy);
  return value.has_value() && folly::to<bool>(value.value());
}

std::shared_ptr<folly::CPUThreadPoolExecutor> GlutenS3FileSystem::uploadThreadPool(uint32_t uploadThreads) {
  std::lock_guard<std::mutex> l(uploadThreadPoolMutex_);
  if (!uploadThreadPool_) {
    uploadThreadPool_ = createUploadThreadPool(uploadThreads);
  }
  return uploadThreadPool_;
}

Aws::S3::S3Client* GlutenS3FileSystem::writeClient() {
  std::lock_guard<std::mutex> l(writeClientMutex_);
  if (!writeClient_) {
    writeClient_ = createWriteClient(s3Config_);
  }
  return writeClient_.get();
}

std::unique_ptr<velox::WriteFile> GlutenS3FileSystem::openFileForWrite(
    std::string_view s3Path,
    const filesystems::FileOptions& options) {
  if (!uploadPartAsync()) {
    return filesystems::S3FileSystem::openFileForWrite(s3Path, options);
  }

  VELOX_CHECK_NOT_NULL(options.pool);
  const auto maxConcurrentUploadNum =
      getUInt32S3Config(config_, bucketName_, kMaxConcurrentUploadNum, kDefaultMaxConcurrentUploadNum);
  const auto uploadThreads = getUInt32S3Config(config_, bucketName_, kUploadThreads, kDefaultUploadThreads);
  VELOX_USER_CHECK_GT(
      maxConcurrentUploadNum, 0, "The hive.s3.max-concurrent-upload-num S3 configuration must be greater than 0.");
  VELOX_USER_CHECK_GT(uploadThreads, 0, "The hive.s3.upload-threads S3 configuration must be greater than 0.");

  const auto path = filesystems::getPath(s3Path);
  return std::make_unique<GlutenS3WriteFile>(
      path,
      writeClient(),
      options.pool,
      s3Config_->minPartSize(),
      maxConcurrentUploadNum,
      uploadThreadPool(uploadThreads));
}

void registerGlutenS3FileSystem(filesystems::CacheKeyFn cacheKeyFunc) {
  filesystems::registerS3FileSystem(std::move(cacheKeyFunc), glutenS3FileSystemFactory);
}

void finalizeGlutenS3FileSystem() {
  filesystems::finalizeS3FileSystem();
}

} // namespace gluten

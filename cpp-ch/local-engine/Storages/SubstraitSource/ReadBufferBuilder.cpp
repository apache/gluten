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

#include "config.h"

#include <memory>
#include <shared_mutex>
#include <Core/Settings.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <IO/BoundedReadBuffer.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadSettings.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3Common.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <IO/SplittableBzip2ReadBuffer.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/HDFS/AsynchronousReadBufferFromHDFS.h>
#include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/compute/detail/lru_cache.hpp>
#include <sys/stat.h>
#include <Poco/Logger.h>
#include <Poco/URI.h>
#include <Common/CHUtil.h>
#include <Common/GlutenConfig.h>
#include <Common/GlutenSettings.h>
#include <Common/logger_useful.h>
#include <Common/safe_cast.h>

#if USE_AZURE_BLOB_STORAGE
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#endif

#if USE_AWS_S3
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#endif

#if USE_HDFS
#include <hdfs/hdfs.h>
#endif

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 s3_max_redirects;
extern const SettingsBool s3_disable_checksum;
extern const SettingsUInt64 s3_retry_attempts;
extern const SettingsMaxThreads max_download_threads;
extern const SettingsUInt64 max_download_buffer_size;
extern const SettingsBool input_format_allow_seeks;
extern const SettingsUInt64 max_read_buffer_size;
extern const SettingsBool s3_slow_all_threads_after_network_error;
extern const SettingsBool s3_slow_all_threads_after_retryable_error;
extern const SettingsBool enable_s3_requests_logging;
}
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_OPEN_FILE;
extern const int UNKNOWN_FILE_SIZE;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

namespace FileCacheSetting
{
extern const FileCacheSettingsUInt64 max_size;
extern const FileCacheSettingsString path;
}
}

namespace local_engine
{

using namespace DB;
FileCacheConcurrentMap ReadBufferBuilder::files_cache_time_map;

template <class key_type, class value_type>
class ConcurrentLRU
{
public:
    explicit ConcurrentLRU(size_t size) : cache(size) { }
    boost::optional<value_type> get(const key_type & key)
    {
        std::shared_lock<std::shared_mutex> lock(rwLock);
        return cache.get(key);
    }


    void insert(const key_type & key, const value_type & value)
    {
        std::unique_lock<std::shared_mutex> lock(rwLock);
        cache.insert(key, value);
    }

    void clear()
    {
        std::unique_lock<std::shared_mutex> lock(rwLock);
        cache.clear();
    }

private:
    boost::compute::detail::lru_cache<key_type, value_type> cache;
    std::shared_mutex rwLock;
};

static std::pair<size_t, size_t> getAdjustedReadRange(SeekableReadBuffer & buffer, const std::pair<size_t, size_t> & start_end)
{
    auto get_next_line_pos = [&](SeekableReadBuffer & buf) -> size_t
    {
        while (!buf.eof())
        {
            /// Search for \n or \r\n or \n\r or \r in buffer.
            if (*buf.position() == '\r')
            {
                ++buf.position();

                if (!buf.eof() && *buf.position() == '\n')
                {
                    ++buf.position();
                }

                return buf.getPosition();
            }
            else if (*buf.position() == '\n')
            {
                ++buf.position();

                if (!buf.eof() && *buf.position() == '\r')
                {
                    ++buf.position();
                }

                return buf.getPosition();
            }

            ++buf.position();
        }

        return buf.getPosition();
    };

    size_t read_start_pos = start_end.first;
    size_t read_end_pos = start_end.second;
    std::pair<size_t, size_t> result;

    if (read_start_pos == 0)
        result.first = read_start_pos;
    else
    {
        buffer.seek(read_start_pos, SEEK_SET);
        result.first = get_next_line_pos(buffer);
    }

    if (read_end_pos == 0)
        result.second = read_end_pos;
    else
    {
        buffer.seek(read_end_pos, SEEK_SET);
        result.second = get_next_line_pos(buffer);
    }

    return result;
}

static std::unique_ptr<SeekableReadBuffer>
adjustReadRangeIfNeeded(std::unique_ptr<SeekableReadBuffer> read_buffer, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info)
{
    /// Skip formats in which rows are not seperated by newline characters.
    if (!(file_info.has_text() || file_info.has_json()))
        return std::move(read_buffer);

    /// Skip text/json files with compression.
    /// When the file is compressed, its read range is adjusted in [[buildWithCompressionWrapper]]
    Poco::URI file_uri(file_info.uri_file());
    DB::CompressionMethod compression = DB::chooseCompressionMethod(file_uri.getPath(), "auto");
    if (compression != CompressionMethod::None)
        return std::move(read_buffer);

    std::pair<size_t, size_t> start_end{file_info.start(), file_info.start() + file_info.length()};
    start_end = getAdjustedReadRange(*read_buffer, start_end);

    LOG_DEBUG(
        &Poco::Logger::get("ReadBufferBuilder"),
        "File read start and end position adjusted from {},{} to {},{}",
        file_info.start(),
        file_info.start() + file_info.length(),
        start_end.first,
        start_end.second);
#if USE_HDFS
    /// If read buffer doesn't support right bounded reads, wrap it with BoundedReadBuffer to enable right bounded reads.
    if (dynamic_cast<DB::ReadBufferFromHDFS *>(read_buffer.get()) || dynamic_cast<DB::AsynchronousReadBufferFromHDFS *>(read_buffer.get())
        || dynamic_cast<DB::ReadBufferFromFile *>(read_buffer.get()))
        read_buffer = std::make_unique<DB::BoundedReadBuffer>(std::move(read_buffer));
#else
    if (dynamic_cast<DB::ReadBufferFromFile *>(read_buffer.get()))
        read_buffer = std::make_unique<DB::BoundedReadBuffer>(std::move(read_buffer));
#endif
    read_buffer->seek(start_end.first, SEEK_SET);
    read_buffer->setReadUntilPosition(start_end.second);
    return std::move(read_buffer);
}

class LocalFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit LocalFileReadBufferBuilder(const DB::ContextPtr & context_) : ReadBufferBuilder(context_) { }
    ~LocalFileReadBufferBuilder() override = default;

    bool isRemote() const override { return false; }

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBufferFromFileBase> read_buffer;
        const String & file_path = file_uri.getPath();
        struct stat file_stat;
        if (stat(file_path.c_str(), &file_stat))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "file stat failed for {}", file_path);

        if (S_ISREG(file_stat.st_mode))
            read_buffer = std::make_unique<DB::ReadBufferFromFilePRead>(file_path);
        else
            read_buffer = std::make_unique<DB::ReadBufferFromFile>(file_path);

        return adjustReadRangeIfNeeded(std::move(read_buffer), file_info);
    }
};

#if USE_HDFS
class HDFSFileReadBufferBuilder : public ReadBufferBuilder
{
    using ReadBufferCreator
        = std::function<std::unique_ptr<DB::ReadBufferFromFileBase>(bool restricted_seek, const DB::StoredObject & object)>;

public:
    explicit HDFSFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_), context(context_) { }
    ~HDFSFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        DB::ReadSettings read_settings = getReadSettings();
        const auto & config = context->getConfigRef();

        /// Get hdfs_uri
        Poco::URI uri(file_info.uri_file());
        auto hdfs_file_path = uri.getPath();

        std::string new_file_uri = uri.toString();
        if (uri.getUserInfo().empty() && BackendInitializerUtil::spark_user.has_value())
        {
            uri.setUserInfo(*BackendInitializerUtil::spark_user);
            new_file_uri = uri.toString();
        }

        auto begin_of_path = new_file_uri.find('/', new_file_uri.find("//") + 2);
        auto hdfs_uri = new_file_uri.substr(0, begin_of_path);

        std::optional<size_t> file_size;
        std::optional<size_t> modified_time;
        if (file_info.has_properties())
        {
            if (file_info.properties().filesize() > 0)
            {
                /// filesize may be zero, under such condition we should not set file_size
                file_size = file_info.properties().filesize();
            }

            modified_time = file_info.properties().modificationtime();
        }

        std::unique_ptr<SeekableReadBuffer> read_buffer;
        if (!read_settings.enable_filesystem_cache)
        {
            bool thread_pool_read = read_settings.remote_fs_method == DB::RemoteFSReadMethod::threadpool;
            /// ORC and Parquet reader had already implemented async prefetch. They don't rely on AsynchronousReadBufferFromHDFS
            bool use_async_prefetch
                = read_settings.remote_fs_prefetch && thread_pool_read && (file_info.has_text() || file_info.has_json());
            auto raw_read_buffer = std::make_unique<ReadBufferFromHDFS>(
                hdfs_uri,
                hdfs_file_path,
                config,
                read_settings,
                /* read_until_position */ 0,
                /* use_external_buffer */ false,
                file_size);

            if (use_async_prefetch)
                read_buffer = std::make_unique<AsynchronousReadBufferFromHDFS>(
                    getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER), read_settings, std::move(raw_read_buffer));
            else
                read_buffer = std::move(raw_read_buffer);
        }
        else
        {
            if (!file_size.has_value())
            {
                // only for spark3.2 file partition not contained file size
                // so first compute file size first
                auto tmp_read_buffer = std::make_unique<DB::ReadBufferFromHDFS>(
                    hdfs_uri,
                    hdfs_file_path,
                    config,
                    read_settings,
                    /* read_until_position */ 0);
                file_size = tmp_read_buffer->getFileSize();
            }

            if (!modified_time.has_value())
                modified_time = 0;

            ReadBufferCreator read_buffer_creator
                = [this, hdfs_uri = hdfs_uri, hdfs_file_path = hdfs_file_path, read_settings, &config](
                      bool /* restricted_seek */, const DB::StoredObject & object) -> std::unique_ptr<DB::ReadBufferFromHDFS>
            {
                return std::make_unique<DB::ReadBufferFromHDFS>(
                    hdfs_uri, hdfs_file_path, config, read_settings, 0, true, object.bytes_size);
            };

            auto remote_path = uri.getPath().substr(1);
            DB::StoredObjects stored_objects{DB::StoredObject{remote_path, "", *file_size}};
            auto cache_creator = wrapWithCache(read_buffer_creator, read_settings, remote_path, *modified_time, *file_size);
            size_t buffer_size = std::max<size_t>(read_settings.remote_fs_buffer_size, DBMS_DEFAULT_BUFFER_SIZE);
            if (*file_size > 0)
                buffer_size = std::min(*file_size, buffer_size);
            auto cache_hdfs_read = std::make_unique<DB::ReadBufferFromRemoteFSGather>(
                std::move(cache_creator), stored_objects, read_settings, /* use_external_buffer */ false, buffer_size);
            read_buffer = std::move(cache_hdfs_read);
        }

        return adjustReadRangeIfNeeded(std::move(read_buffer), file_info);
    }

private:
    DB::ContextPtr context;
};
#endif

#if USE_AWS_S3
class S3FileReadBufferBuilder : public ReadBufferBuilder
{
    friend void registerReadBufferBuilders();


public:
    explicit S3FileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_)
    {
        auto config = S3Config::loadFromContext(context);
        // use gluten cache config is first priority
        if (!file_cache && config.s3_local_cache_enabled)
        {
            DB::FileCacheSettings file_cache_settings;
            file_cache_settings[FileCacheSetting::max_size] = config.s3_local_cache_max_size;
            auto cache_base_path = config.s3_local_cache_cache_path;

            if (!std::filesystem::exists(cache_base_path))
                std::filesystem::create_directories(cache_base_path);

            file_cache_settings[FileCacheSetting::path] = cache_base_path;
            file_cache = DB::FileCacheFactory::instance().getOrCreate("s3_local_cache", file_cache_settings, "");
            file_cache->initialize();
        }
    }

    ~S3FileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        DB::ReadSettings read_settings = getReadSettings();
        Poco::URI file_uri(file_info.uri_file());
        // file uri looks like: s3a://my-dev-bucket/tpch100/part/0001.parquet
        const std::string & bucket = file_uri.getHost();
        const auto client = getClient(bucket);
        std::string pathKey = file_uri.getPath().substr(1);

        size_t object_size = 0;
        size_t object_modified_time = 0;
        if (file_info.has_properties())
        {
            object_size = file_info.properties().filesize();
            object_modified_time = file_info.properties().modificationtime();
        }
        else
        {
            DB::S3::ObjectInfo object_info = DB::S3::getObjectInfo(*client, bucket, pathKey, "");
            object_size = object_info.size;
            object_modified_time = object_info.last_modification_time;
        }

        auto read_buffer_creator = [bucket, client, read_settings, this](
                                       bool restricted_seek, const DB::StoredObject & object) -> std::unique_ptr<DB::ReadBufferFromFileBase>
        {
            return std::make_unique<DB::ReadBufferFromS3>(
                client,
                bucket,
                object.remote_path,
                "",
                DB::S3::S3RequestSettings(),
                read_settings,
                /* use_external_buffer */ true,
                /* offset */ 0,
                /* read_until_position */ 0,
                restricted_seek);
        };

        auto cache_creator = wrapWithCache(read_buffer_creator, read_settings, pathKey, object_modified_time, object_size);

        DB::StoredObjects stored_objects{DB::StoredObject{pathKey, "", object_size}};
        auto s3_impl = std::make_unique<DB::ReadBufferFromRemoteFSGather>(
            std::move(cache_creator), stored_objects, read_settings, /* use_external_buffer */ true, 0);

        auto & pool_reader = context->getThreadPoolReader(DB::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
        size_t buffer_size = std::max<size_t>(read_settings.remote_fs_buffer_size, DBMS_DEFAULT_BUFFER_SIZE);
        if (object_size > 0)
            buffer_size = std::min(object_size, buffer_size);
        auto async_reader = std::make_unique<DB::AsynchronousBoundedReadBuffer>(
            std::move(s3_impl), pool_reader, read_settings, buffer_size, read_settings.remote_read_min_bytes_for_seek);
        if (read_settings.remote_fs_prefetch)
            async_reader->prefetch(Priority{});

        return adjustReadRangeIfNeeded(std::move(async_reader), file_info);
    }

private:
    static const std::string SHARED_CLIENT_KEY;
    static ConcurrentLRU<std::string, std::shared_ptr<DB::S3::Client>> per_bucket_clients;

    static std::string toBucketNameSetting(const std::string & bucket_name, const std::string & config_name)
    {
        if (!config_name.starts_with(BackendInitializerUtil::S3A_PREFIX))
        {
            // Currently per bucket only support fs.s3a.xxx
            return config_name;
        }
        // like: fs.s3a.bucket.bucket_name.assumed.role.externalId
        return BackendInitializerUtil::S3A_PREFIX + "bucket." + bucket_name + "."
            + config_name.substr(BackendInitializerUtil::S3A_PREFIX.size());
    }

    static std::string getSetting(
        const DB::Settings & settings,
        const std::string & bucket_name,
        const std::string & config_name,
        const std::string & default_value = "",
        const bool require_per_bucket = false)
    {
        std::string ret;
        // if there's a bucket specific config, prefer it to non per bucket config
        if (tryGetString(settings, toBucketNameSetting(bucket_name, config_name), ret))
            return ret;

        if (!require_per_bucket && tryGetString(settings, config_name, ret))
            return ret;

        return default_value;
    }

    void cacheClient(const std::string & bucket_name, const bool is_per_bucket, std::shared_ptr<DB::S3::Client> client)
    {
        if (is_per_bucket)
        {
            per_bucket_clients.insert(bucket_name, client);
        }
        else
        {
            per_bucket_clients.insert(SHARED_CLIENT_KEY, client);
        }
    }

    std::shared_ptr<DB::S3::Client> getClient(const std::string & bucket_name)
    {
        const auto & config = context->getConfigRef();
        const auto & settings = context->getSettingsRef();
        bool use_assumed_role = false;
        bool is_per_bucket = false;

        if (!getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_ROLE).empty())
            use_assumed_role = true;

        if (!getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_ROLE, "", true).empty())
            is_per_bucket = true;

        if (is_per_bucket && "true" != getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_CLIENT_CACHE_IGNORE))
        {
            auto client = per_bucket_clients.get(bucket_name);
            if (client.has_value())
            {
                return client.get();
            }
        }

        if (!is_per_bucket && "true" != getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_CLIENT_CACHE_IGNORE))
        {
            auto client = per_bucket_clients.get(SHARED_CLIENT_KEY);
            if (client.has_value())
                return client.get();
        }

        String config_prefix = "s3";
        auto endpoint = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ENDPOINT, "https://s3.us-west-2.amazonaws.com");
        bool end_point_start_with_http_or_https = endpoint.starts_with("https://") || endpoint.starts_with("http://");
        if (!end_point_start_with_http_or_https)
        {
            if (endpoint.starts_with("s3"))
                // as https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.0.1/bk_cloud-data-access/content/s3-config-parameters.html
                // the fs.s3a.endpoint does not contain https:// prefix
                endpoint = "https://" + endpoint;
            else
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "S3 Endpoint format not right: {}", endpoint);
        }
        String region_name;
        const char * amazon_suffix = ".amazonaws.com";
        const char * amazon_prefix = "https://s3.";
        auto pos = endpoint.find(amazon_suffix);
        if (pos != std::string::npos)
        {
            assert(endpoint.starts_with(amazon_prefix));
            region_name = endpoint.substr(strlen(amazon_prefix), pos - strlen(amazon_prefix));
            assert(region_name.find('.') == std::string::npos);
        }
        // for AWS CN, the endpoint is like: https://s3.cn-north-1.amazonaws.com.cn, can still work

        unsigned int s3_retry_attempts = static_cast<unsigned>(context->getSettingsRef()[DB::Setting::s3_retry_attempts]);
        DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
            region_name,
            context->getRemoteHostFilter(),
            static_cast<unsigned>(context->getSettingsRef()[DB::Setting::s3_max_redirects]),
            S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = s3_retry_attempts},
            context->getSettingsRef()[DB::Setting::s3_slow_all_threads_after_network_error],
            context->getSettingsRef()[Setting::s3_slow_all_threads_after_retryable_error],
            context->getSettingsRef()[DB::Setting::enable_s3_requests_logging],
            false,
            nullptr,
            nullptr);

        client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 10000);
        client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 5000);
        client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
        client_configuration.endpointOverride = endpoint;

        client_configuration.retryStrategy
            = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.getUInt(config_prefix + ".retry_attempts", 10));

        std::string ak;
        std::string sk;
        std::string path_style_access;
        bool addressing_type = false;
        tryGetString(settings, BackendInitializerUtil::HADOOP_S3_ACCESS_KEY, ak);
        tryGetString(settings, BackendInitializerUtil::HADOOP_S3_SECRET_KEY, sk);
        tryGetString(settings, BackendInitializerUtil::HADOOP_S3_PATH_STYLE_ACCESS, path_style_access);
        const DB::Settings & global_settings = context->getGlobalContext()->getSettingsRef();
        const DB::Settings & local_settings = context->getSettingsRef();
        boost::algorithm::to_lower(path_style_access);
        addressing_type = (path_style_access == "false");
        DB::S3::ClientSettings client_settings{
            .use_virtual_addressing = addressing_type,
            .disable_checksum = local_settings[DB::Setting::s3_disable_checksum],
            .gcs_issue_compose_request = context->getConfigRef().getBool("s3.gcs_issue_compose_request", false),
        };
        if (use_assumed_role)
        {
            auto new_client = DB::S3::ClientFactory::instance().create(
                client_configuration,
                client_settings,
                ak, // access_key_id
                sk, // secret_access_key
                "", // server_side_encryption_customer_key_base64
                {}, // sse_kms_config
                {}, // headers
                DB::S3::CredentialsConfiguration{
                    .use_environment_credentials = true,
                    .use_insecure_imds_request = false,
                    .role_arn = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_ROLE),
                    .role_session_name = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_SESSION_NAME),
                    .external_id = getSetting(settings, bucket_name, BackendInitializerUtil::HADOOP_S3_ASSUMED_EXTERNAL_ID)});

            //TODO: support online change config for cached per_bucket_clients
            std::shared_ptr<DB::S3::Client> ret = std::move(new_client);
            cacheClient(bucket_name, is_per_bucket, ret);
            return ret;
        }
        else
        {
            auto new_client = DB::S3::ClientFactory::instance().create(
                client_configuration,
                client_settings,
                ak, // access_key_id
                sk, // secret_access_key
                "", // server_side_encryption_customer_key_base64
                {}, // sse_kms_config
                {}, // headers
                DB::S3::CredentialsConfiguration{.use_environment_credentials = true, .use_insecure_imds_request = false});

            std::shared_ptr<DB::S3::Client> ret = std::move(new_client);
            cacheClient(bucket_name, is_per_bucket, ret);
            return ret;
        }
    }
};
const std::string S3FileReadBufferBuilder::SHARED_CLIENT_KEY = "___shared-client___";
ConcurrentLRU<std::string, std::shared_ptr<DB::S3::Client>> S3FileReadBufferBuilder::per_bucket_clients(100);

#endif

#if USE_AZURE_BLOB_STORAGE
class AzureBlobReadBuffer : public ReadBufferBuilder
{
public:
    explicit AzureBlobReadBuffer(const DB::ContextPtr & context_) : ReadBufferBuilder(context_) { }
    ~AzureBlobReadBuffer() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        Poco::URI file_uri(file_info.uri_file());
        return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(getClient(), file_uri.getPath(), DB::ReadSettings(), 5, 5);
    }

private:
    std::shared_ptr<DB::AzureBlobStorage::ContainerClient> shared_client;

    std::shared_ptr<DB::AzureBlobStorage::ContainerClient> getClient()
    {
        if (shared_client)
            return shared_client;

        const std::string config_prefix = "blob";
        const Poco::Util::AbstractConfiguration & config = context->getConfigRef();
        bool is_client_for_disk = false;
        auto new_settings = DB::AzureBlobStorage::getRequestSettings(config, config_prefix, context->getSettingsRef());
        DB::AzureBlobStorage::ConnectionParams params{
            .endpoint = DB::AzureBlobStorage::processEndpoint(config, config_prefix),
            .auth_method = DB::AzureBlobStorage::getAuthMethod(config, config_prefix),
            .client_options = DB::AzureBlobStorage::getClientOptions(context, context->getSettingsRef(), *new_settings, is_client_for_disk),
        };

        shared_client = DB::AzureBlobStorage::getContainerClient(params, true);
        return shared_client;
    }
};
#endif

void registerReadBufferBuilders()
{
    auto & factory = ReadBufferBuilderFactory::instance();
    factory.registerBuilder("file", [](DB::ContextPtr context_) { return std::make_shared<LocalFileReadBufferBuilder>(context_); });

#if USE_HDFS
    factory.registerBuilder("hdfs", [](DB::ContextPtr context_) { return std::make_shared<HDFSFileReadBufferBuilder>(context_); });
#endif

#if USE_AWS_S3
    factory.registerBuilder("s3", [](DB::ContextPtr context_) { return std::make_shared<S3FileReadBufferBuilder>(context_); });
    factory.registerBuilder("s3a", [](DB::ContextPtr context_) { return std::make_shared<S3FileReadBufferBuilder>(context_); });
    factory.registerCleaner([]() { S3FileReadBufferBuilder::per_bucket_clients.clear(); });
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerBuilder("wasb", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
    factory.registerBuilder("wasbs", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
#endif
}

DB::ReadSettings ReadBufferBuilder::getReadSettings() const
{
    DB::ReadSettings read_settings = context->getReadSettings();
    const auto & config = context->getConfigRef();

    /// Override enable_filesystem_cache with gluten config
    read_settings.enable_filesystem_cache = config.getBool(GlutenCacheConfig::ENABLED, false);

    /// Override remote_fs_prefetch with gluten config
    read_settings.remote_fs_prefetch = config.getBool("hdfs.enable_async_io", false);

    return read_settings;
}

ReadBufferBuilder::ReadBufferBuilder(const DB::ContextPtr & context_) : context(context_)
{
}

std::unique_ptr<DB::ReadBuffer>
ReadBufferBuilder::wrapWithBzip2(std::unique_ptr<DB::ReadBuffer> in, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) const
{
    /// Bzip2 compressed file is splittable and we need to adjust read range for each split
    auto * seekable = dynamic_cast<SeekableReadBuffer *>(in.release());
    if (!seekable)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ReadBuffer underlying BZIP2 decompressor must be seekable");
    std::unique_ptr<SeekableReadBuffer> seekable_in(seekable);

    size_t file_size = getFileSizeFromReadBuffer(*seekable_in);
    size_t start = file_info.start();
    size_t end = file_info.start() + file_info.length();

    /// No need to adjust start becuase it is already processed inside SplittableBzip2ReadBuffer
    size_t new_start = start;

    /// Extend end to the end of next block.
    size_t new_end = end;
    if (end < file_size)
    {
        Int64 bs_buff = 0;
        Int64 bs_live = 0;

        /// From end position skip to the second block delimiter.
        seekable_in->seek(end, SEEK_SET);
        for (size_t i = 0; i < 2; ++i)
        {
            size_t pos = seekable_in->getPosition();
            bool ok = SplittableBzip2ReadBuffer::skipToNextMarker(
                SplittableBzip2ReadBuffer::BLOCK_DELIMITER,
                SplittableBzip2ReadBuffer::DELIMITER_BIT_LENGTH,
                *seekable_in,
                bs_buff,
                bs_live);

            if (seekable_in->eof())
                break;

            if (!ok)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find next block delimiter in after offset: {}", pos);
        }
        new_end = seekable->eof() ? file_size : seekable_in->getPosition() - SplittableBzip2ReadBuffer::DELIMITER_BIT_LENGTH / 8 + 1;
    }

    LOG_DEBUG(
        &Poco::Logger::get("ReadBufferBuilder"),
        "File read start and end position adjusted from {},{} to {},{}",
        start,
        end,
        new_start,
        new_end);

    std::unique_ptr<SeekableReadBuffer> bounded_in;
#if USE_HDFS
    if (dynamic_cast<DB::ReadBufferFromHDFS *>(seekable_in.get()) || dynamic_cast<DB::AsynchronousReadBufferFromHDFS *>(seekable_in.get())
        || dynamic_cast<DB::ReadBufferFromFile *>(seekable_in.get()))
        bounded_in = std::make_unique<BoundedReadBuffer>(std::move(seekable_in));
    else
        bounded_in = std::move(seekable_in);
#else
    if (dynamic_cast<DB::ReadBufferFromFile *>(seekable_in.get()))
        bounded_in = std::make_unique<BoundedReadBuffer>(std::move(seekable_in));
    else
        bounded_in = std::move(seekable_in);
#endif
    bounded_in->seek(new_start, SEEK_SET);
    bounded_in->setReadUntilPosition(new_end);
    bool first_block_need_special_process = (new_start > 0);
    bool last_block_need_special_process = (new_end < file_size);
    size_t buffer_size = context->getSettingsRef()[DB::Setting::max_read_buffer_size];
    auto decompressed_in = std::make_unique<SplittableBzip2ReadBuffer>(
        std::move(bounded_in), first_block_need_special_process, last_block_need_special_process, buffer_size);
    return std::move(decompressed_in);
}

std::unique_ptr<DB::ReadBuffer>
ReadBufferBuilder::buildWithCompressionWrapper(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info)
{
    auto in = build(file_info);

    /// Wrap the read buffer with compression method if exists
    Poco::URI file_uri(file_info.uri_file());
    DB::CompressionMethod compression = DB::chooseCompressionMethod(file_uri.getPath(), "auto");

    if (compression == CompressionMethod::Bzip2)
        return wrapWithBzip2(std::move(in), file_info);
    else
    {
        /// In this case we are pretty sure that current split covers the whole file because only bzip2 compression is splittable
        auto parallel = wrapWithParallelIfNeeded(std::move(in), file_info);
        return wrapReadBufferWithCompressionMethod(std::move(parallel), compression);
    }
}

ReadBufferBuilder::ReadBufferCreator ReadBufferBuilder::wrapWithCache(
    ReadBufferCreator read_buffer_creator,
    DB::ReadSettings & read_settings,
    const String & key,
    size_t last_modified_time,
    size_t file_size)
{
    const auto & config = context->getConfigRef();
    if (!config.getBool(GlutenCacheConfig::ENABLED, false))
        return read_buffer_creator;

    read_settings.enable_filesystem_cache = true;
    if (!file_cache)
    {
        DB::FileCacheSettings file_cache_settings;
        file_cache_settings.loadFromConfig(config, GlutenCacheConfig::PREFIX, DB::getPathPrefixForRelativeCachePath(context), "");

        auto & base_path = file_cache_settings[FileCacheSetting::path].value;
        if (std::filesystem::path(base_path).is_relative())
            base_path = std::filesystem::path(context->getPath()) / "caches" / base_path;

        if (!std::filesystem::exists(base_path))
            std::filesystem::create_directories(base_path);

        const auto name = config.getString(GlutenCacheConfig::PREFIX + ".name");
        std::string config_prefix;
        file_cache = DB::FileCacheFactory::instance().getOrCreate(name, file_cache_settings, config_prefix);
        file_cache->initialize();
    }

    if (!file_cache->isInitialized())
    {
        file_cache->throwInitExceptionIfNeeded();
        return read_buffer_creator;
    }

    updateCaches(key, last_modified_time, file_size);

    return [read_buffer_creator, read_settings, this](
               bool restricted_seek, const DB::StoredObject & object) -> std::unique_ptr<DB::ReadBufferFromFileBase>
    {
        auto cache_key = DB::FileCacheKey::fromPath(object.remote_path);
        auto modified_read_settings = read_settings.withNestedBuffer();
        auto rbc = [=, this]() { return read_buffer_creator(restricted_seek, object); };

        return std::make_unique<DB::CachedOnDiskReadBufferFromFile>(
            object.remote_path,
            cache_key,
            file_cache,
            DB::FileCache::getCommonUser(),
            rbc,
            modified_read_settings,
            std::string(DB::CurrentThread::getQueryId()),
            object.bytes_size,
            /* allow_seeks */ !read_settings.remote_read_buffer_restrict_seek,
            /* use_external_buffer */ true,
            /* read_until_position */ std::nullopt,
            context->getFilesystemCacheLog());
    };
}

void ReadBufferBuilder::updateCaches(const String & key, size_t last_modified_time, size_t file_size) const
{
    if (!file_cache)
        return;

    auto file_cache_key = DB::FileCacheKey::fromPath(key);
    auto last_cache_time = files_cache_time_map.get(file_cache_key);
    // quick check
    if (last_cache_time != std::nullopt && last_cache_time.has_value())
    {
        auto & [cached_modified_time, cached_file_size] = last_cache_time.value();
        if (cached_modified_time < last_modified_time || cached_file_size != file_size)
            files_cache_time_map.update_cache_time(file_cache_key, last_modified_time, file_size, file_cache);
    }
    else
    {
        // if process restart, cache map will be empty,
        //   we recommend continuing to use caching instead of renew it
        files_cache_time_map.insert(file_cache_key, last_modified_time, file_size);
    }
}

std::unique_ptr<DB::ReadBuffer> ReadBufferBuilder::wrapWithParallelIfNeeded(
    std::unique_ptr<DB::ReadBuffer> in, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info)
{
    /// Only use parallel downloading for text and json format because data are read serially in those formats.
    if (!file_info.has_text() && !file_info.has_json())
        return std::move(in);

    const auto & settings = context->getSettingsRef();
    auto max_download_threads = settings[DB::Setting::max_download_threads];
    auto max_download_buffer_size = settings[DB::Setting::max_download_buffer_size];

    bool parallel_read = isRemote() && max_download_threads > 1 && isBufferWithFileSize(*in);
    if (!parallel_read)
        return std::move(in);

    size_t file_size = getFileSizeFromReadBuffer(*in);
    if (file_size < 4 * max_download_buffer_size)
        return std::move(in);

    LOG_TRACE(
        getLogger("ReadBufferBuilder"),
        "Using ParallelReadBuffer with {} workers with chunks of {} bytes",
        max_download_threads.value,
        max_download_buffer_size.value);

    return wrapInParallelReadBufferIfSupported(
        {std::move(in)},
        DB::threadPoolCallbackRunnerUnsafe<void>(DB::getIOThreadPool().get(), "ParallelRead"),
        max_download_threads,
        max_download_buffer_size,
        file_size);
}

ReadBufferBuilderFactory & ReadBufferBuilderFactory::instance()
{
    static ReadBufferBuilderFactory instance;
    return instance;
}

void ReadBufferBuilderFactory::registerBuilder(const String & schema, NewBuilder newer)
{
    auto it = builders.find(schema);
    if (it != builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "readbuffer builder for {} has been registered", schema);
    builders[schema] = newer;
}

ReadBufferBuilderPtr ReadBufferBuilderFactory::createBuilder(const String & schema, DB::ContextPtr context)
{
    auto it = builders.find(schema);
    if (it == builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found read buffer builder for {}", schema);
    return it->second(context);
}

void ReadBufferBuilderFactory::registerCleaner(Cleaner cleaner)
{
    cleaners.push_back(cleaner);
}

void ReadBufferBuilderFactory::clean()
{
    for (auto c : cleaners)
    {
        c();
    }
}
}

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

#include <config.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Common/Throttler.h>
#include <Common/typeid_cast.h>
#if USE_HDFS
#include <Disks/ObjectStorages/GlutenHDFSObjectStorage.h>
#endif

namespace local_engine
{
#if USE_HDFS
class GlutenDiskHDFS : public DB::DiskObjectStorage
{
public:
    GlutenDiskHDFS(
        const String & name_,
        const String & object_key_prefix_,
        DB::MetadataStoragePtr metadata_storage_,
        DB::ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        std::function<DB::ObjectStoragePtr(
            const Poco::Util::AbstractConfiguration & conf, DB::ContextPtr context)> _object_storage_creator)
        : DiskObjectStorage(name_, object_key_prefix_, metadata_storage_, object_storage_, config, config_prefix)
        , object_key_prefix(object_key_prefix_)
        , hdfs_config_prefix(config_prefix)
        , object_storage_creator(_object_storage_creator)
    {
        hdfs_object_storage = typeid_cast<std::shared_ptr<GlutenHDFSObjectStorage>>(object_storage_);
        hdfsSetWorkingDirectory(hdfs_object_storage->getHDFSFS(), "/");
        auto max_speed = config.getUInt(config_prefix + ".write_speed", 450);
        throttler = std::make_shared<DB::Throttler>(max_speed);
    }

    DB::DiskTransactionPtr createTransaction() override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void removeDirectory(const String & path) override;

    void removeRecursive(const String & path) override;

    DB::DiskObjectStoragePtr createDiskObjectStorage() override;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(const String& path, size_t buf_size, DB::WriteMode mode,
        const DB::WriteSettings& settings) override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        DB::ContextPtr context,
        const String & config_prefix,
        const DB::DisksMap & map) override
    {
        DB::ObjectStoragePtr tmp = object_storage_creator(config, context);
        hdfs_object_storage = typeid_cast<std::shared_ptr<GlutenHDFSObjectStorage>>(tmp);
        // only for java ut
        bool is_cache = object_storage->supportsCache();
        if (is_cache)
        {
            std::shared_ptr<DB::CachedObjectStorage> cache_os = typeid_cast<std::shared_ptr<DB::CachedObjectStorage>>(object_storage);
            object_storage = hdfs_object_storage;
            auto cache = DB::FileCacheFactory::instance().getOrCreate(cache_os->getCacheName(), cache_os->getCacheSettings(), "storage_configuration.disks.hdfs_cache");
            wrapWithCache(cache, cache_os->getCacheSettings(), cache_os->getCacheConfigName());
        }
        else
            object_storage = hdfs_object_storage;
    }
private:
    std::shared_ptr<GlutenHDFSObjectStorage> hdfs_object_storage;
    String object_key_prefix;
    DB::ThrottlerPtr throttler;
    const String hdfs_config_prefix;
    std::function<DB::ObjectStoragePtr(
        const Poco::Util::AbstractConfiguration & conf, DB::ContextPtr context)>
        object_storage_creator;
};
#endif
}


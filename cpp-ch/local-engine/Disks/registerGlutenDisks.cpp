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
#include "registerGlutenDisks.h"
#include "config.h"

#include <Disks/DiskFactory.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Common/GlutenConfig.h>

#if USE_HDFS
#include <Disks/ObjectStorages/GlutenDiskHDFS.h>
#endif

#if USE_AWS_S3
#include <Disks/ObjectStorages/GlutenDiskS3.h>
#endif

#if USE_ROCKSDB
#include <Disks/ObjectStorages/MetadataStorageFromRocksDB.h>
#endif

namespace local_engine
{
#if USE_AWS_S3
void registerGlutenS3ObjectStorage(DB::ObjectStorageFactory & factory);
#endif

#if USE_HDFS
void registerGlutenHDFSObjectStorage(DB::ObjectStorageFactory & factory);
#endif

void registerGlutenDisks(bool global_skip_access_check)
{
    auto & factory = DB::DiskFactory::instance();
    auto & object_factory = DB::ObjectStorageFactory::instance();

#if USE_AWS_S3
    auto creator = [global_skip_access_check](
                       const String & name,
                       const Poco::Util::AbstractConfiguration & config,
                       const String & config_prefix,
                       DB::ContextPtr context,
                       const DB::DisksMap & /* map */,
                       bool,
                       bool) -> DB::DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage_creator = [name, skip_access_check, config_prefix](
                                          const Poco::Util::AbstractConfiguration & conf, DB::ContextPtr ctx) -> DB::ObjectStoragePtr
        { return DB::ObjectStorageFactory::instance().create(name, conf, config_prefix, ctx, skip_access_check); };
        auto object_storage = DB::ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
        DB::MetadataStoragePtr metadata_storage;
        auto metadata_type = DB::MetadataStorageFactory::getMetadataType(config, config_prefix, "local");
        if (metadata_type == "rocksdb")
        {
#if USE_ROCKSDB
            metadata_storage = MetadataStorageFromRocksDB::create(name, config, config_prefix, object_storage);
#else
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "RocksDB metadata storage is not enabled in the build");
#endif
        }
        else
            metadata_storage = DB::MetadataStorageFactory::instance().create(name, config, config_prefix, object_storage, "local");

        DB::DiskObjectStoragePtr disk = std::make_shared<local_engine::GlutenDiskS3>(
            name,
            object_storage->getCommonKeyPrefix(),
            std::move(metadata_storage),
            std::move(object_storage),
            config,
            config_prefix,
            object_storage_creator);

        disk->startup(skip_access_check);
        return disk;
    };


    registerGlutenS3ObjectStorage(object_factory);

    factory.registerDiskType(GlutenObjectStorageConfig::S3_DISK_TYPE, creator); /// For compatibility
#endif

#if USE_HDFS
    auto hdfs_creator = [global_skip_access_check](
                            const String & name,
                            const Poco::Util::AbstractConfiguration & config,
                            const String & config_prefix,
                            DB::ContextPtr context,
                            const DB::DisksMap & /* map */,
                            bool,
                            bool) -> DB::DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage_creator = [name, skip_access_check, config_prefix](
                                          const Poco::Util::AbstractConfiguration & conf, DB::ContextPtr ctx) -> DB::ObjectStoragePtr
        { return DB::ObjectStorageFactory::instance().create(name, conf, config_prefix, ctx, skip_access_check); };
        auto object_storage = object_storage_creator(config, context);
        DB::MetadataStoragePtr metadata_storage;
        auto metadata_type = DB::MetadataStorageFactory::getMetadataType(config, config_prefix, "local");
        if (metadata_type == "rocksdb")
        {
#if USE_ROCKSDB
            metadata_storage = MetadataStorageFromRocksDB::create(name, config, config_prefix, object_storage);
#else
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "RocksDB metadata storage is not enabled in the build");
#endif
        }
        else
            metadata_storage = DB::MetadataStorageFactory::instance().create(name, config, config_prefix, object_storage, "local");

        DB::DiskObjectStoragePtr disk = std::make_shared<local_engine::GlutenDiskHDFS>(
            name,
            object_storage->getCommonKeyPrefix(),
            std::move(metadata_storage),
            std::move(object_storage),
            config,
            config_prefix,
            object_storage_creator);

        disk->startup(skip_access_check);
        return disk;
    };

    registerGlutenHDFSObjectStorage(object_factory);
    factory.registerDiskType(GlutenObjectStorageConfig::HDFS_DISK_TYPE, hdfs_creator); /// For compatibility
#endif
}
}

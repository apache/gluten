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


#include <Disks/DiskObjectStorage/DiskObjectStorage.h>


#if USE_AWS_S3
namespace local_engine
{
class GlutenDiskS3 : public DB::DiskObjectStorage
{
public:
    GlutenDiskS3(
        const String & name_,
        const String & object_key_prefix_,
        DB::MetadataStoragePtr metadata_storage_,
        DB::ObjectStoragePtr object_storage_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        std::function<DB::ObjectStoragePtr(const Poco::Util::AbstractConfiguration & conf, DB::ContextPtr context)> creator)
        : DiskObjectStorage(name_, object_key_prefix_, metadata_storage_, object_storage_, config, config_prefix),
        object_storage_creator(creator) {}

    DB::DiskTransactionPtr createTransaction() override;

    DB::DiskObjectStoragePtr createDiskObjectStorage() override;

private:
    std::function<DB::ObjectStoragePtr(const Poco::Util::AbstractConfiguration & conf, DB::ContextPtr context)> object_storage_creator;
};
}

#endif

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
#include "MetadataStorageFromRocksDB.h"
#if USE_ROCKSDB
#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/MetadataStorageFromRocksDBTransactionOperations.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StaticDirectoryIterator.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <rocksdb/db.h>
#include <Common/QueryContext.h>

namespace local_engine
{
static std::string getObjectKeyCompatiblePrefix(
    const DB::IObjectStorage & object_storage, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    return config.getString(config_prefix + ".key_compatibility_prefix", object_storage.getCommonKeyPrefix());
}

DB::MetadataStoragePtr MetadataStorageFromRocksDB::create(
    const std::string &,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    DB::ObjectStoragePtr object_storage)
{
    auto metadata_path = config.getString(config_prefix + ".metadata_path");
    size_t clean_meta_task_interval_seconds = config.getUInt(config_prefix + ".clean_meta_task_interval_seconds", 60 * 60 * 12);
    fs::create_directories(metadata_path);
    auto key_compatibility_prefix = getObjectKeyCompatiblePrefix(*object_storage, config, config_prefix);
    return std::make_shared<MetadataStorageFromRocksDB>(
        key_compatibility_prefix, metadata_path, object_storage, clean_meta_task_interval_seconds);
}

MetadataStorageFromRocksDB::MetadataStorageFromRocksDB(
    const String & compatible_key_prefix,
    const String & rocksdb_dir,
    DB::ObjectStoragePtr & object_storage_,
    size_t metadata_clean_task_interval_seconds_)
    : compatible_key_prefix(compatible_key_prefix)
    , rocksdb_dir(rocksdb_dir)
    , object_storage(object_storage_)
    , metadata_clean_task_interval_seconds(metadata_clean_task_interval_seconds_)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    throwRockDBErrorNotOk(rocksdb::DB::Open(options, rocksdb_dir, &rocksdb));
    // TODO: rebase-25.12, is it correct to use createEmpty storageID?
    metadata_clean_task = QueryContext::globalContext()->getSchedulePool().createTask(
        DB::StorageID::createEmpty(), "MetadataStorageFromRocksDB", [this] { cleanOutdatedMetadataThreadFunc(); });
    metadata_clean_task->scheduleAfter(metadata_clean_task_interval_seconds * 1000);
    logger = getLogger("MetadataStorageFromRocksDB");
}

DB::MetadataTransactionPtr MetadataStorageFromRocksDB::createTransaction()
{
    return std::make_shared<MetadataStorageFromRocksDBTransaction>(*this);
}

const std::string & MetadataStorageFromRocksDB::getPath() const
{
    return rocksdb_dir;
}

DB::MetadataStorageType MetadataStorageFromRocksDB::getType() const
{
    return DB::MetadataStorageType::None;
}

bool MetadataStorageFromRocksDB::existsFileOrDirectory(const std::string & path) const
{
    return exist(getRocksDB(), path);
}

bool MetadataStorageFromRocksDB::existsFile(const std::string & path) const
{
    std::string data;
    return tryGetData(getRocksDB(), path, &data) && data != RocksDBCreateDirectoryOperation::DIR_DATA;
}

bool MetadataStorageFromRocksDB::existsDirectory(const std::string & path) const
{
    std::string data;
    return tryGetData(getRocksDB(), path, &data) && data == RocksDBCreateDirectoryOperation::DIR_DATA;
}

uint64_t MetadataStorageFromRocksDB::getFileSize(const std::string & path) const
{
    return getTotalSize(readMetadata(path)->objects);
}

Poco::Timestamp MetadataStorageFromRocksDB::getLastModified(const std::string & /*path*/) const
{
    return {};
}

bool MetadataStorageFromRocksDB::supportsChmod() const
{
    return false;
}

bool MetadataStorageFromRocksDB::supportsStat() const
{
    return false;
}

bool MetadataStorageFromRocksDB::supportsPartitionCommand(const DB::PartitionCommand & command) const
{
    return false;
}

std::vector<std::string> MetadataStorageFromRocksDB::listDirectory(const std::string & path) const
{
    return listKeys(getRocksDB(), path);
}

DB::DirectoryIteratorPtr MetadataStorageFromRocksDB::iterateDirectory(const std::string & path) const
{
    auto files = listKeys(getRocksDB(), path);
    std::vector<std::filesystem::path> paths;
    paths.reserve(files.size());
    for (const auto & file : files)
        paths.emplace_back(file);
    return std::make_unique<DB::StaticDirectoryIterator>(std::move(paths));
}

uint32_t MetadataStorageFromRocksDB::getHardlinkCount(const std::string & /*path*/) const
{
    return 0;
}

DB::StoredObjects MetadataStorageFromRocksDB::getStorageObjects(const std::string & path) const
{
    return readMetadata(path)->objects;
}

DB::DiskObjectStorageMetadataPtr MetadataStorageFromRocksDB::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

DB::DiskObjectStorageMetadataPtr
MetadataStorageFromRocksDB::readMetadataUnlocked(const std::string & path, std::unique_lock<DB::SharedMutex> &) const
{
    auto metadata = std::make_unique<DB::DiskObjectStorageMetadata>(compatible_key_prefix, path);
    auto str = getData(getRocksDB(), path);
    metadata->deserializeFromString(str);
    return metadata;
}

DB::DiskObjectStorageMetadataPtr
MetadataStorageFromRocksDB::readMetadataUnlocked(const std::string & path, std::shared_lock<DB::SharedMutex> &) const
{
    auto metadata = std::make_unique<DB::DiskObjectStorageMetadata>(compatible_key_prefix, path);
    auto str = getData(getRocksDB(), path);
    metadata->deserializeFromString(str);
    return metadata;
}

std::string MetadataStorageFromRocksDB::readFileToString(const std::string & path) const
{
    return getData(getRocksDB(), path);
}

void MetadataStorageFromRocksDB::shutdown()
{
    metadata_clean_task->deactivate();
    if (rocksdb)
    {
        rocksdb->Close();
        rocksdb = nullptr;
    }
}

void MetadataStorageFromRocksDB::cleanOutdatedMetadataThreadFunc()
{
    LOG_INFO(logger, "start to clean disk metadata in rocksdb.");
    std::queue<String> part_queue;
    size_t total_count_remove = 0;
    auto removeParts = [&]
    {
        while (!part_queue.empty())
        {
            auto meta_name = part_queue.front();
            part_queue.pop();
            std::filesystem::path meta_path(meta_name);
            auto part_path = meta_path.parent_path();
            auto files = listDirectory(part_path);
            total_count_remove += (files.size() + 1);
            getRocksDB().DeleteRange({}, part_path.generic_string(), files.back());
            getRocksDB().Delete({}, files.back());
        }
    };
    auto * it = getRocksDB().NewIterator({});
    String prev_key;
    String prev_data;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        auto file_name = it->key().ToString();
        // mark outdated part
        if (isMergeTreePartMetaDataFile(file_name))
        {
            auto objects = getStorageObjects(it->key().ToString());
            if (!object_storage->exists(objects.front()))
                part_queue.push(file_name);
        }
        // clean empty directory
        if (!prev_key.empty() && !file_name.starts_with(prev_key) && prev_data == RocksDBCreateDirectoryOperation::DIR_DATA)
        {
            getRocksDB().Delete({}, prev_key);
            total_count_remove ++;
        }
        if (part_queue.size() > 10000)
        {
            removeParts();
        }
    }
    removeParts();
    rocksdb::Slice begin(nullptr, 0);
    rocksdb::Slice end(nullptr, 0);
    rocksdb::Status s = getRocksDB().CompactRange({}, &begin, &end);
    LOG_INFO(logger, "Clean meta finish, totally clean {} meta", total_count_remove);
    metadata_clean_task->scheduleAfter(metadata_clean_task_interval_seconds * 1000);
}

DB::SharedMutex & MetadataStorageFromRocksDB::getMetadataMutex() const
{
    return metadata_mutex;
}

rocksdb::DB & MetadataStorageFromRocksDB::getRocksDB() const
{
    if (!rocksdb)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "RocksDB is not initialized");
    return *rocksdb;
}

void MetadataStorageFromRocksDBTransaction::commit(const DB::TransactionCommitOptionsVariant & options)
{
    // TODO: rebase-25.12, fix later, is it OK to call 'DB::MetadataOperationsHolder::commit()'?
    DB::MetadataOperationsHolder::commit();
}

std::optional<DB::StoredObjects> MetadataStorageFromRocksDBTransaction::tryGetBlobsFromTransactionIfExists(const std::string & path) const
{
    return metadata_storage.getStorageObjectsIfExist(path);
}

const DB::IMetadataStorage & MetadataStorageFromRocksDBTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

bool MetadataStorageFromRocksDBTransaction::supportsChmod() const
{
    return false;
}

void MetadataStorageFromRocksDBTransaction::createEmptyMetadataFile(const std::string & path)
{
    auto metadata = std::make_unique<DB::DiskObjectStorageMetadata>(metadata_storage.compatible_key_prefix, path);
    writeStringToFile(path, metadata->serializeToString());
}

void MetadataStorageFromRocksDBTransaction::createMetadataFile(const std::string & path, DB::ObjectStorageKey key, uint64_t size_in_bytes)
{
    auto metadata = std::make_unique<DB::DiskObjectStorageMetadata>(metadata_storage.compatible_key_prefix, path);
    metadata->objects.emplace_back(key.serialize(), "", size_in_bytes, 0);

    auto data = metadata->serializeToStringWithOffset();
    if (!data.empty())
        addOperation(std::make_unique<RocksDBWriteFileOperation>(path, metadata_storage.getRocksDB(), data));
}

void MetadataStorageFromRocksDBTransaction::writeStringToFile(const std::string & path, const std::string & data)
{
    addOperation(std::make_unique<RocksDBWriteFileOperation>(path, metadata_storage.getRocksDB(), data));
}

void MetadataStorageFromRocksDBTransaction::createDirectory(const std::string & path)
{
    addOperation(std::make_unique<RocksDBCreateDirectoryOperation>(path, metadata_storage.getRocksDB()));
}

void MetadataStorageFromRocksDBTransaction::createDirectoryRecursive(const std::string & path)
{
    addOperation(std::make_unique<RocksDBCreateDirectoryRecursiveOperation>(path, metadata_storage.getRocksDB()));
}

void MetadataStorageFromRocksDBTransaction::removeDirectory(const std::string & path)
{
    addOperation(std::make_unique<RocksDBRemoveDirectoryOperation>(path, metadata_storage.getRocksDB()));
}

void MetadataStorageFromRocksDBTransaction::removeRecursive(const std::string & path)
{
    addOperation(std::make_unique<RocksDBRemoveRecursiveOperation>(path, metadata_storage.getRocksDB()));
}

void MetadataStorageFromRocksDBTransaction::unlinkFile(const std::string & path)
{
    addOperation(std::make_unique<RocksDBUnlinkFileOperation>(path, metadata_storage.getRocksDB()));
}

DB::ObjectStorageKey MetadataStorageFromRocksDBTransaction::generateObjectKeyForPath(const std::string & path)
{
    // TODO: rebase-25.12, how to return ObjectStorageKey
    // return metadata_storage.key_generator->generate();
    return DB::ObjectStorageKey();
}

void MetadataStorageFromRocksDBTransaction::createMetadataFile(const std::string & path, const DB::StoredObjects & objects)
{
    // TODO: rebase-25.12, fix later, there is no operations
    // operations.addOperation(std::make_unique<RewriteFileOperation>(path, objects, metadata_storage.compatible_key_prefix, *metadata_storage.disk));
}
}
#endif
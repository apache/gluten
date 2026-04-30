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
#include <config.h>
#if USE_ROCKSDB
#include "MetadataStorageFromRocksDBTransactionOperations.h"

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_STATE;
}
}

namespace local_engine
{

void throwRockDBErrorNotOk(const rocksdb::Status & status)
{
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::INVALID_STATE, "Access rocksdb failed: {}", status.ToString());
}

bool exist(rocksdb::DB & db, const std::string & path)
{
    std::string data;
    return tryGetData(db, path, &data);
}

bool tryGetData(rocksdb::DB & db, const std::string & path, std::string * value)
{
    auto status = db.Get({}, path, value);
    if (status.IsNotFound())
        return false;
    throwRockDBErrorNotOk(status);
    return status.ok();
}

String getData(rocksdb::DB & db, const std::string & path)
{
    std::string data;
    throwRockDBErrorNotOk(db.Get({}, path, &data));
    return data;
}

std::vector<String> listKeys(rocksdb::DB & db, const std::string & path)
{
    std::vector<String> result;
    auto *it = db.NewIterator({});
    for (it->Seek(path); it->Valid() && it->key().starts_with(path); it->Next())
    {
        if (it->key() == path)
            continue;
        result.push_back(it->key().ToString());
    }
    return result;
}

void RocksDBWriteFileOperation::execute()
{
    auto status = db.Get({}, path, &prev_data);
    if (status.IsNotFound())
        existed = false;
    else
        throwRockDBErrorNotOk(status);
    db.Put({}, path, data);
}

void RocksDBWriteFileOperation::undo()
{
    if (existed)
        throwRockDBErrorNotOk(db.Put({}, path, prev_data));
    else
        throwRockDBErrorNotOk(db.Delete({}, path));
}

void RocksDBCreateDirectoryOperation::execute()
{
    existed = exist(db, path);
    if (existed)
        return;
    throwRockDBErrorNotOk(db.Put({}, path, DIR_DATA));
}

void RocksDBCreateDirectoryOperation::undo()
{
    if (existed) return;
    throwRockDBErrorNotOk(db.Delete({}, path));
}

void RocksDBCreateDirectoryRecursiveOperation::execute( )
{
    namespace fs = std::filesystem;
    fs::path p(path);
    while (!exist(db, p.string()))
    {
        paths_created.push_back(p);
        if (!p.has_parent_path())
            break;
        p = p.parent_path();
    }
    for (const auto & path_to_create : paths_created | std::views::reverse)
        throwRockDBErrorNotOk(db.Put({}, path_to_create, RocksDBCreateDirectoryOperation::DIR_DATA));
}

void RocksDBCreateDirectoryRecursiveOperation::undo( )
{
    for (const auto & path_created : paths_created)
        throwRockDBErrorNotOk(db.Delete({}, path_created));
}

void RocksDBRemoveDirectoryOperation::execute()
{
    auto *it = db.NewIterator({});
    bool empty_dir = true;
    for (it->Seek(path); it->Valid() && it->key().starts_with(path); it->Next())
    {
        if (it->key() != path && it->key().starts_with(path))
        {
            empty_dir = false;
            break;
        }
        if (it->key() == path)
            existed = true;
    }
    if (!empty_dir)
    {
        throw DB::Exception(DB::ErrorCodes::INVALID_STATE, "Directory {} is not empty", path);
    }
    if (existed)
        throwRockDBErrorNotOk(db.Delete({}, path));
}

void RocksDBRemoveDirectoryOperation::undo()
{
    if (existed)
        throwRockDBErrorNotOk(db.Put({}, path, RocksDBCreateDirectoryOperation::DIR_DATA));
}

void RocksDBRemoveRecursiveOperation::execute()
{
    auto *it = db.NewIterator({});
    for (it->Seek(path); it->Valid() && it->key().starts_with(path); it->Next())
    {
        files.emplace(it->key().ToString(), it->value().ToString());
        throwRockDBErrorNotOk(db.Delete({}, it->key()));
    }
}

void RocksDBRemoveRecursiveOperation::undo()
{
    for (const auto & [key, value] : files)
        throwRockDBErrorNotOk(db.Put({}, key, value));
}

void RocksDBUnlinkFileOperation::execute()
{
    prev_data = getData(db, path);
    throwRockDBErrorNotOk(db.Delete({}, path));
}

void RocksDBUnlinkFileOperation::undo()
{
    throwRockDBErrorNotOk(db.Put({}, path, prev_data));
}
}
#endif
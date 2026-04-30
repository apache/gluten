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
#if USE_ROCKSDB
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataOperation.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <rocksdb/db.h>

namespace local_engine
{
void throwRockDBErrorNotOk(const rocksdb::Status & status);
bool exist(rocksdb::DB & db, const std::string & path);
bool tryGetData(rocksdb::DB & db, const std::string & path, std::string* value);
String getData(rocksdb::DB & db, const std::string & path);
std::vector<String> listKeys(rocksdb::DB & db, const std::string & path);

struct RocksDBWriteFileOperation final : public DB::IMetadataOperation
{
    RocksDBWriteFileOperation(const std::string& path_, rocksdb::DB& db_, const std::string& data_) : path(path_),
        db(db_), data(data_)
    {
    }

    void execute() override;

    void undo() override;

private:
    std::string path;
    rocksdb::DB & db;
    std::string data;
    bool existed = false;
    std::string prev_data;
};

struct RocksDBCreateDirectoryOperation final : public DB::IMetadataOperation
{
    RocksDBCreateDirectoryOperation(const std::string & path_, rocksdb::DB & db_) : path(path_), db(db_)
    {
    }

    void execute() override;

    void undo() override;

    const static inline String DIR_DATA = "__DIR__";
private:
    std::string path;
    bool existed = false;
    rocksdb::DB & db;

};

struct RocksDBCreateDirectoryRecursiveOperation final : public DB::IMetadataOperation
{
    RocksDBCreateDirectoryRecursiveOperation(const std::string & path_, rocksdb::DB & db_) : path(path_), db(db_)
    {
    };

    void execute() override;

    void undo() override;

private:
    std::string path;
    std::vector<std::string> paths_created;
    rocksdb::DB & db;
};

struct RocksDBRemoveDirectoryOperation final : public DB::IMetadataOperation
{
    RocksDBRemoveDirectoryOperation(const std::string & path_, rocksdb::DB & db_): path(path_), db(db_)
    {
    }

    void execute() override;

    void undo() override;

private:
    std::string path;
    bool existed = false;
    rocksdb::DB & db;
};

struct RocksDBRemoveRecursiveOperation final : public DB::IMetadataOperation
{
    RocksDBRemoveRecursiveOperation(const std::string & path_, rocksdb::DB & db_) : path(path_), db(db_)
    {
    }

    void execute() override;

    void undo() override;

private:
    std::string path;
    rocksdb::DB & db;
    std::unordered_map<String, String> files;
};

struct RocksDBUnlinkFileOperation final : public DB::IMetadataOperation
{
    RocksDBUnlinkFileOperation(const std::string & path_, rocksdb::DB & db_) : path(path_), db(db_)
    {
    }

    void execute() override;

    void undo() override;

private:
    std::string path;
    rocksdb::DB & db;
    std::string prev_data;
};


}
#endif


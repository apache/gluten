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

#include "HdfsPaimonFileSystem.h"

#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <Common/logger_useful.h>
#include <Poco/URI.h>
#include <hdfs/hdfs.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <limits>
#include <mutex>
#include <utility>

namespace local_engine::paimon
{
namespace
{
using Status = ::paimon::Status;

struct HdfsConnection
{
    DB::HDFSFSPtr fs;
    explicit HdfsConnection(const std::string & uri, DB::ContextPtr context)
    {
        auto builder = DB::createHDFSBuilder(uri, context->getConfigRef());
        fs = DB::createHDFSFS(builder.get());
    }
};

std::string pathPart(const std::string & uri)
{
    Poco::URI parsed(uri);
    return parsed.getPath();
}

Status hdfsError(const std::string & operation, const std::string & path)
{
    return Status::IOError(operation, " ", path, ": ", hdfsGetLastError());
}

class HdfsStatus final : public ::paimon::FileStatus, public ::paimon::BasicFileStatus
{
    std::string path_;
    int64_t size_;
    int64_t modification_time_;
    bool directory_;

public:
    explicit HdfsStatus(const hdfsFileInfo & info)
        : path_(info.mName)
        , size_(info.mSize)
        , modification_time_(static_cast<int64_t>(info.mLastMod) * 1000)
        , directory_(info.mKind == kObjectKindDirectory)
    {
    }

    int64_t GetLen() const override { return size_; }
    bool IsDir() const override { return directory_; }
    std::string GetPath() const override { return path_; }
    int64_t GetModificationTime() const override { return modification_time_; }
};

class HdfsInputStream final : public ::paimon::InputStream
{
    std::shared_ptr<HdfsConnection> connection_;
    hdfsFile file_;
    std::string uri_;
    int64_t length_;
    int64_t position_ = 0;
    uint64_t read_calls_ = 0;
    uint64_t read_bytes_ = 0;
    uint64_t read_time_ns_ = 0;
    mutable std::mutex mutex_;

public:
    HdfsInputStream(std::shared_ptr<HdfsConnection> connection, hdfsFile file, std::string uri, int64_t length)
        : connection_(std::move(connection)), file_(file), uri_(std::move(uri)), length_(length) { }
    ~HdfsInputStream() override
    {
        Close();
        if (read_calls_ > 0)
            LOG_INFO(
                &Poco::Logger::get("PaimonHDFS"),
                "HDFS read summary: path={}, calls={}, bytes={}, read_time_ms={}",
                uri_,
                read_calls_,
                read_bytes_,
                read_time_ns_ / 1000000);
    }

    Status Close() override
    {
        std::lock_guard lock(mutex_);
        if (file_ && hdfsCloseFile(connection_->fs.get(), file_) != 0)
            return hdfsError("close", uri_);
        file_ = nullptr;
        return Status::OK();
    }

    Status Seek(int64_t offset, ::paimon::SeekOrigin origin) override
    {
        std::lock_guard lock(mutex_);
        if (origin != ::paimon::FS_SEEK_SET && origin != ::paimon::FS_SEEK_CUR && origin != ::paimon::FS_SEEK_END)
            return Status::Invalid("Invalid HDFS seek origin: ", uri_);
        int64_t base = origin == ::paimon::FS_SEEK_END ? length_ : origin == ::paimon::FS_SEEK_CUR ? position_ : 0;
        if (offset < -base || offset > length_ - base)
            return Status::Invalid("HDFS seek out of range: ", uri_);
        position_ = base + offset;
        return Status::OK();
    }

    ::paimon::Result<int64_t> GetPos() const override
    {
        std::lock_guard lock(mutex_);
        return position_;
    }

    ::paimon::Result<int64_t> Read(char * buffer, int64_t size) override
    {
        std::lock_guard lock(mutex_);
        auto result = readAt(buffer, size, position_);
        if (result.ok())
            position_ += result.value();
        return result;
    }

    ::paimon::Result<int64_t> Read(char * buffer, int64_t size, int64_t offset) override
    {
        std::lock_guard lock(mutex_);
        return readAt(buffer, size, offset);
    }

    void ReadAsync(char * buffer, int64_t size, int64_t offset, std::function<void(Status)> && callback) override
    {
        auto result = Read(buffer, size, offset);
        callback(result.ok() ? Status::OK() : result.status());
    }

    ::paimon::Result<std::string> GetUri() const override { return uri_; }
    ::paimon::Result<int64_t> Length() const override { return length_; }

private:
    ::paimon::Result<int64_t> readAt(char * buffer, int64_t size, int64_t offset)
    {
        if (!file_ || size < 0 || offset < 0)
            return Status::Invalid("Invalid HDFS read: ", uri_);
        if (offset > length_ || size > length_ - offset)
            return Status::IOError("HDFS read exceeds file length: ", uri_);
        int64_t total = 0;
        while (total < size)
        {
            auto count = static_cast<tSize>(std::min<int64_t>(size - total, std::numeric_limits<tSize>::max()));
            auto start = std::chrono::steady_clock::now();
            auto read = hdfsPread(connection_->fs.get(), file_, buffer + total, count, offset + total);
            read_time_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();
            ++read_calls_;
            if (read < 0)
                return hdfsError("read", uri_);
            if (read == 0)
                return Status::IOError("Unexpected EOF reading HDFS file: ", uri_);
            read_bytes_ += read;
            total += read;
        }
        return total;
    }
};

class HdfsPaimonFileSystem final : public ::paimon::FileSystem
{
    std::shared_ptr<HdfsConnection> connection_;

public:
    HdfsPaimonFileSystem(const std::string & table_path, DB::ContextPtr context)
    {
        Poco::URI uri(table_path);
        connection_ = std::make_shared<HdfsConnection>(uri.getScheme() + "://" + uri.getAuthority(), context);
    }

    ::paimon::Result<std::unique_ptr<::paimon::InputStream>> Open(const std::string & path) const override
    {
        auto hdfs_path = pathPart(path);
        auto * info = hdfsGetPathInfo(connection_->fs.get(), hdfs_path.c_str());
        if (!info)
            return hdfsError("stat", path);
        int64_t length = info->mSize;
        hdfsFreeFileInfo(info, 1);
        auto file = hdfsOpenFile(connection_->fs.get(), hdfs_path.c_str(), O_RDONLY, 0, 0, 0);
        if (!file)
            return hdfsError("open", path);
        return std::unique_ptr<::paimon::InputStream>(std::make_unique<HdfsInputStream>(connection_, file, path, length));
    }

    ::paimon::Result<bool> Exists(const std::string & path) const override
    {
        auto hdfs_path = pathPart(path);
        if (hdfsExists(connection_->fs.get(), hdfs_path.c_str()) == 0)
            return true;
        if (errno == ENOENT)
            return false;
        return hdfsError("exists", path);
    }

    ::paimon::Result<std::unique_ptr<::paimon::FileStatus>> GetFileStatus(const std::string & path) const override
    {
        auto hdfs_path = pathPart(path);
        auto * info = hdfsGetPathInfo(connection_->fs.get(), hdfs_path.c_str());
        if (!info)
            return hdfsError("stat", path);
        std::unique_ptr<::paimon::FileStatus> status = std::make_unique<HdfsStatus>(*info);
        hdfsFreeFileInfo(info, 1);
        return status;
    }

    Status ListDir(const std::string & path, std::vector<std::unique_ptr<::paimon::BasicFileStatus>> * result) const override
    {
        int count = 0;
        auto hdfs_path = pathPart(path);
        errno = 0;
        auto * files = hdfsListDirectory(connection_->fs.get(), hdfs_path.c_str(), &count);
        if (!files && errno != 0)
            return hdfsError("list", path);
        for (int i = 0; i < count; ++i)
            result->push_back(std::make_unique<HdfsStatus>(files[i]));
        if (files)
            hdfsFreeFileInfo(files, count);
        return Status::OK();
    }

    Status ListFileStatus(const std::string & path, std::vector<std::unique_ptr<::paimon::FileStatus>> * result) const override
    {
        int count = 0;
        auto hdfs_path = pathPart(path);
        errno = 0;
        auto * files = hdfsListDirectory(connection_->fs.get(), hdfs_path.c_str(), &count);
        if (!files && errno != 0)
            return hdfsError("list", path);
        for (int i = 0; i < count; ++i)
            result->push_back(std::make_unique<HdfsStatus>(files[i]));
        if (files)
            hdfsFreeFileInfo(files, count);
        return Status::OK();
    }

    ::paimon::Result<std::unique_ptr<::paimon::OutputStream>> Create(const std::string &, bool) const override
    { return Status::NotImplemented("HDFS Paimon writer is not supported"); }
    Status Mkdirs(const std::string &) const override { return Status::NotImplemented("HDFS Paimon writer is not supported"); }
    Status Rename(const std::string &, const std::string &) const override { return Status::NotImplemented("HDFS Paimon writer is not supported"); }
    Status Delete(const std::string &, bool) const override { return Status::NotImplemented("HDFS Paimon writer is not supported"); }
};
}

std::shared_ptr<::paimon::FileSystem> createHdfsPaimonFileSystem(const std::string & table_path, DB::ContextPtr context)
{
    return std::make_shared<HdfsPaimonFileSystem>(table_path, std::move(context));
}
}

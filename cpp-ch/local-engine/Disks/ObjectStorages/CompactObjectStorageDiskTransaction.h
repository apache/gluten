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
#include <Disks/IDiskTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Interpreters/TemporaryDataOnDisk.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{

class TemporaryWriteBufferWrapper : public DB::WriteBufferFromFileBase
{
public:
    TemporaryWriteBufferWrapper(const String & file_name_, const std::shared_ptr<DB::TemporaryDataBuffer> & data_buffer_);

    void sync() override { data_buffer->nextImpl(); }

    void preFinalize() override;

protected:
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

private:
    void nextImpl() override;

public:
    std::string getFileName() const override
    {
        return file_name;
    }

private:
    String file_name;
    std::shared_ptr<DB::TemporaryDataBuffer> data_buffer;
};

class CompactObjectStorageDiskTransaction: public DB::IDiskTransaction {
    public:
    static inline const String PART_DATA_FILE_NAME = "part_data.gluten";
    static inline const String PART_META_FILE_NAME = "part_meta.gluten";

    explicit CompactObjectStorageDiskTransaction(DB::IDisk & disk_, const DB::TemporaryDataOnDiskScopePtr tmp_)
        : disk(disk_), tmp_data(tmp_)
    {
    }

    void commit(const DB::TransactionCommitOptionsVariant & options) override;

    void undo() noexcept override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `undo` is not implemented");
    }

    void createDirectory(const std::string & path) override
    {
        disk.createDirectory(path);
    }

    void createDirectories(const std::string & path) override
    {
        disk.createDirectories(path);
    }

    void createFile(const std::string & path) override
    {
        disk.createFile(path);
    }

    void moveDirectory(const std::string & from_path, const std::string & to_path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `moveDirectory` is not implemented");
    }

    void moveFile(const String & from_path, const String & to_path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `moveFile` is not implemented");
    }

    void replaceFile(const std::string & from_path, const std::string & to_path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `replaceFile` is not implemented");
    }

    void copyFile(const std::string & from_file_path, const std::string & to_file_path, const DB::ReadSettings & read_settings, const DB::WriteSettings & write_settings) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `copyFile` is not implemented");
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings) override;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile( /// NOLINT
        const std::string & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings) override;


    void writeFileUsingBlobWritingFunction(const String & path, DB::WriteMode mode, WriteBlobFunction && write_blob_function) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `writeFileUsingBlobWritingFunction` is not implemented");
    }

    void removeFile(const std::string & path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeFile` is not implemented");
    }

    void removeFileIfExists(const std::string & path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeFileIfExists` is not implemented");
    }

    void removeDirectory(const std::string & path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeDirectory` is not implemented");
    }

    void removeRecursive(const std::string & path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeRecursive` is not implemented");
    }

    void removeSharedFile(const std::string & path, bool keep_shared_data) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeSharedFile` is not implemented");
    }

    void removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const DB::NameSet & file_names_remove_metadata_only) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeSharedRecursive` is not implemented");
    }

    void removeSharedFileIfExists(const std::string & path, bool keep_shared_data) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeSharedFileIfExists` is not implemented");
    }

    void removeSharedFiles(const DB::RemoveBatchRequest & files, bool keep_all_batch_data, const DB::NameSet & file_names_remove_metadata_only) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `removeSharedFiles` is not implemented");
    }

    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override
    {
        disk.setLastModified(path, timestamp);
    }

    void chmod(const String & path, mode_t mode) override
    {
        disk.chmod(path, mode);
    }

    void setReadOnly(const std::string & path) override
    {
        disk.setReadOnly(path);
    }

    void createHardLink(const std::string & src_path, const std::string & dst_path) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `createHardLink` is not implemented");
    }

    void truncateFile(const std::string & /* src_path */, size_t /* target_size */) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `truncateFile` is not implemented");
    }

private:
    DB::IDisk & disk;
    DB::TemporaryDataOnDiskScopePtr tmp_data;
    std::vector<std::pair<String, std::shared_ptr<DB::TemporaryDataBuffer>>> files;
    String prefix_path = "";
};
}


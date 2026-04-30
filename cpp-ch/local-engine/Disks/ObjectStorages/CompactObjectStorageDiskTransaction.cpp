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

#include "CompactObjectStorageDiskTransaction.h"

#include <format>
#include <ranges>

namespace local_engine
{

bool isMetaDataFile(const std::string & path)
{
    return !path.ends_with("bin");
}

TemporaryWriteBufferWrapper::TemporaryWriteBufferWrapper(
    const String & file_name_, const std::shared_ptr<DB::TemporaryDataBuffer> & data_buffer_)
    : WriteBufferFromFileBase(data_buffer_->buffer().size(), data_buffer_->buffer().begin(), 0)
    , file_name(file_name_)
    , data_buffer(data_buffer_)
{
}
void TemporaryWriteBufferWrapper::preFinalize()
{
    next();
}

void TemporaryWriteBufferWrapper::finalizeImpl()
{
    next();
    data_buffer->finalizeImpl();
}

void TemporaryWriteBufferWrapper::cancelImpl() noexcept
{
    data_buffer->cancelImpl();
}

void TemporaryWriteBufferWrapper::nextImpl()
{
    data_buffer->position() = position();
    data_buffer->next();
    BufferBase::set(data_buffer->buffer().begin(), data_buffer->buffer().size(), data_buffer->offset());
}

void CompactObjectStorageDiskTransaction::commit(const DB::TransactionCommitOptionsVariant & options)
{
    auto metadata_tx = disk.getMetadataStorage()->createTransaction();
    std::filesystem::path data_path = std::filesystem::path(prefix_path) / PART_DATA_FILE_NAME;
    std::filesystem::path meta_path = std::filesystem::path(prefix_path) / PART_META_FILE_NAME;

    auto object_storage = disk.getObjectStorage();
    auto data_key = metadata_tx->generateObjectKeyForPath(data_path);
    auto meta_key = metadata_tx->generateObjectKeyForPath(meta_path);

    disk.createDirectories(prefix_path);
    auto data_write_buffer = object_storage->writeObject(DB::StoredObject(data_key.serialize(), data_path), DB::WriteMode::Rewrite);
    auto meta_write_buffer = object_storage->writeObject(DB::StoredObject(meta_key.serialize(), meta_path), DB::WriteMode::Rewrite);
    String buffer;
    buffer.resize(1024 * 1024);

    auto merge_files = [&](std::ranges::input_range auto && list, DB::WriteBuffer & out, const DB::ObjectStorageKey & key , const String &local_path)
    {
        size_t offset = 0;
        std::ranges::for_each(
            list,
            [&](auto & item)
            {
                DB::DiskObjectStorageMetadata metadata(object_storage->getCommonKeyPrefix(), item.first);
                auto read = item.second->read();
                int file_size = 0;
                while (int count = read->readBig(buffer.data(), buffer.size()))
                {
                    file_size += count;
                    out.write(buffer.data(), count);
                }
                metadata.objects.emplace_back(key.serialize(), "", file_size, offset);
                metadata_tx->writeStringToFile(item.first, metadata.serializeToStringWithOffset());
                offset += file_size;
            });

        // You can load the complete file in advance through this metadata original, which improves the download efficiency of mergetree metadata.
        DB::DiskObjectStorageMetadata whole_meta(object_storage->getCommonKeyPrefix(), local_path);
        whole_meta.objects.emplace_back(key.serialize(), "", offset, 0);
        metadata_tx->writeStringToFile(local_path, whole_meta.serializeToStringWithOffset());
        out.sync();
        out.finalize();
    };

    merge_files(files | std::ranges::views::filter([](auto file) { return !isMetaDataFile(file.first); }), *data_write_buffer, data_key, data_path);
    merge_files(files | std::ranges::views::filter([](auto file) { return isMetaDataFile(file.first); }), *meta_write_buffer, meta_key, meta_path);

    metadata_tx->commit(options);
    files.clear();
}

std::unique_ptr<DB::WriteBufferFromFileBase> CompactObjectStorageDiskTransaction::writeFileWithAutoCommit(
        const std::string & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings)
{
    return writeFile(path, buf_size, mode, settings);
}

std::unique_ptr<DB::WriteBufferFromFileBase> CompactObjectStorageDiskTransaction::writeFile(
    const std::string & path,
    size_t buf_size,
    DB::WriteMode mode,
    const DB::WriteSettings &)
{
    if (mode != DB::WriteMode::Rewrite)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Operation `writeFile` with Append is not implemented");
    }
    if (prefix_path.empty())
        prefix_path = path.substr(0, path.find_last_of('/'));
    else if (!path.starts_with(prefix_path))
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Don't support write file in different dirs, path {}, prefix path: {}",
            path,
            prefix_path);
    auto tmp = std::make_shared<DB::TemporaryDataBuffer>(tmp_data);
    files.emplace_back(path, tmp);
    auto tx = disk.getMetadataStorage()->createTransaction();
    tx->createDirectoryRecursive(std::filesystem::path(path).parent_path());
    tx->createMetadataFile(path, {});
    tx->commit();

    return std::make_unique<TemporaryWriteBufferWrapper>(path, tmp);
}
}
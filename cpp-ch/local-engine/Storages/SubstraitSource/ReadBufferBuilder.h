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

#include <functional>
#include <memory>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <substrait/plan.pb.h>
#include <Common/FileCacheConcurrentMap.h>


namespace local_engine
{

class ReadBufferBuilder
{
public:
    explicit ReadBufferBuilder(const DB::ContextPtr & context_);

    virtual ~ReadBufferBuilder() = default;

    virtual bool isRemote() const { return true; }

    /// build a new read buffer
    virtual std::unique_ptr<DB::ReadBuffer>
    build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) = 0;

    /// build a new read buffer, consider compression method
    std::unique_ptr<DB::ReadBuffer> buildWithCompressionWrapper(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info);

protected:
    using ReadBufferCreator = std::function<std::unique_ptr<DB::ReadBufferFromFileBase>(bool restricted_seek, const DB::StoredObject & object)>;

    std::unique_ptr<DB::ReadBuffer>
    wrapWithBzip2(std::unique_ptr<DB::ReadBuffer> in, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) const;

    ReadBufferCreator wrapWithCache(
        ReadBufferCreator read_buffer_creator,
        DB::ReadSettings & read_settings,
        const String & key,
        size_t last_modified_time,
        size_t file_size);

    std::unique_ptr<DB::ReadBuffer>
    wrapWithParallelIfNeeded(std::unique_ptr<DB::ReadBuffer> in, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info);

    DB::ReadSettings getReadSettings() const;

    DB::ContextPtr context;

private:
    void updateCaches(const String & key, size_t last_modified_time, size_t file_size) const;

public:
    DB::FileCachePtr file_cache = nullptr;
    static FileCacheConcurrentMap files_cache_time_map;
};

using ReadBufferBuilderPtr = std::shared_ptr<ReadBufferBuilder>;

class ReadBufferBuilderFactory : public boost::noncopyable
{
public:
    static ReadBufferBuilderFactory & instance();

    using NewBuilder = std::function<ReadBufferBuilderPtr(DB::ContextPtr)>;
    void registerBuilder(const String & schema, NewBuilder newer);
    ReadBufferBuilderPtr createBuilder(const String & schema, DB::ContextPtr context);

    using Cleaner = std::function<void()>; // used to clean cache and so on for each kind of builder
    void registerCleaner(Cleaner);
    void clean();


private:
    std::map<String, NewBuilder> builders;
    std::vector<Cleaner> cleaners;
};

void registerReadBufferBuilders();
}

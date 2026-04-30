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
#include "CacheManager.h"

#include <ranges>
#include <Core/Settings.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <jni/jni_common.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_block_size;
}
namespace ErrorCodes
{
extern const int INVALID_STATE;
}
}

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

namespace local_engine
{
using namespace DB;
jclass CacheManager::cache_result_class = nullptr;
jmethodID CacheManager::cache_result_constructor = nullptr;

void CacheManager::initJNI(JNIEnv * env)
{
    cache_result_class = CreateGlobalClassReference(env, "Lorg/apache/gluten/execution/CacheResult;");
    cache_result_constructor = GetMethodID(env, cache_result_class, "<init>", "(ILjava/lang/String;)V");
}

CacheManager & CacheManager::instance()
{
    static CacheManager cache_manager;
    return cache_manager;
}

void CacheManager::initialize(const DB::ContextMutablePtr & context_)
{
    auto & manager = instance();
    manager.context = context_;
}

struct CacheJobContext
{
    MergeTreeTableInstance table;
};

Task CacheManager::cachePart(
    const MergeTreeTableInstance & table, const MergeTreePart & part, const std::unordered_set<String> & columns, bool only_meta_cache)
{
    CacheJobContext job_context{table};
    job_context.table.parts.clear();
    job_context.table.parts.push_back(part);
    job_context.table.snapshot_id = "";
    MergeTreeCacheConfig config = MergeTreeCacheConfig::loadFromContext(context);
    Task task = [job_detail = job_context, context = this->context, read_columns = columns, only_meta_cache,
        prefetch_data = config.enable_data_prefetch]()
    {
        try
        {
            auto storage = job_detail.table.restoreStorage(context);
            std::vector<DataPartPtr> selected_parts
                = StorageMergeTreeFactory::getDataPartsByNames(storage->getStorageID(), "", {job_detail.table.parts.front().name});

            if (only_meta_cache)
            {
                LOG_INFO(
                    getLogger("CacheManager"),
                    "Load meta cache of table {}.{} part {} success.",
                    job_detail.table.database,
                    job_detail.table.table,
                    job_detail.table.parts.front().name);
                return;
            }
            // prefetch part data
            if (prefetch_data)
                storage->prefetchPartDataFile({job_detail.table.parts.front().name});

            auto storage_snapshot = std::make_shared<StorageSnapshot>(*storage, storage->getInMemoryMetadataPtr());
            NamesAndTypesList names_and_types_list;
            auto meta_columns = storage->getInMemoryMetadata().getColumns();
            for (const auto & column : meta_columns)
            {
                if (read_columns.contains(column.name))
                    names_and_types_list.push_back(NameAndTypePair(column.name, column.type));
            }
            auto query_info = buildQueryInfo(names_and_types_list);
            auto read_step = storage->reader.readFromParts(
                std::make_shared<const RangesInDataParts>(selected_parts),
                storage->getMutationsSnapshot({}),
                names_and_types_list.getNames(),
                storage_snapshot,
                *query_info,
                context,
                context->getSettingsRef()[Setting::max_block_size],
                1);
            QueryPlan plan;
            plan.addStep(std::move(read_step));
            DB::QueryPlanOptimizationSettings optimization_settings{context};
            DB::BuildQueryPipelineSettings build_settings{context};
            auto pipeline_builder = plan.buildQueryPipeline(optimization_settings, build_settings);
            auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder.get()));
            PullingPipelineExecutor executor(pipeline);
            while (true)
            {
                if (Chunk chunk; !executor.pull(chunk))
                    break;
            }
            LOG_INFO(getLogger("CacheManager"), "Load cache of table {}.{} part {} success.", job_detail.table.database, job_detail.table.table, job_detail.table.parts.front().name);
        }
        catch (std::exception& e)
        {
            LOG_ERROR(getLogger("CacheManager"), "Load cache of table {}.{} part {} failed.\n {}", job_detail.table.database, job_detail.table.table, job_detail.table.parts.front().name, e.what());
            std::rethrow_exception(std::current_exception());
        }
    };
    LOG_INFO(getLogger("CacheManager"), "Loading cache of table {}.{} part {}", job_context.table.database, job_context.table.table, job_context.table.parts.front().name);
    return std::move(task);
}

JobId CacheManager::cacheParts(const MergeTreeTableInstance & table, const std::unordered_set<String>& columns, bool only_meta_cache)
{
    JobId id = toString(UUIDHelpers::generateV4());
    Job job(id);
    for (const auto & part : table.parts)
    {
        job.addTask(cachePart(table, part, columns, only_meta_cache));
    }
    auto& scheduler = JobScheduler::instance();
    scheduler.scheduleJob(std::move(job));
    return id;
}

jobject CacheManager::getCacheStatus(JNIEnv * env, const String & jobId)
{
    auto & scheduler = JobScheduler::instance();
    auto job_status = scheduler.getJobSatus(jobId);
    int status = 0;
    String message;
    if (job_status.has_value())
    {
        switch (job_status.value().status)
        {
            case JobSatus::RUNNING:
                status = 0;
                break;
            case JobSatus::FINISHED:
                status = 1;
                break;
            case JobSatus::FAILED:
                status = 2;
                for (const auto & msg : job_status->messages)
                {
                    message.append(msg);
                    message.append(";");
                }
                break;
        }
    }
    else
    {
        status = 2;
        message = fmt::format("job {} not found", jobId);
    }
    return env->NewObject(cache_result_class, cache_result_constructor, status, charTojstring(env, message.c_str()));
}

Task CacheManager::cacheFile(const substrait::ReadRel::LocalFiles::FileOrFiles & file, ReadBufferBuilderPtr read_buffer_builder)
{
    auto task = [file, read_buffer_builder, context = this->context]()
    {
        LOG_INFO(getLogger("CacheManager"), "Loading cache file {}", file.uri_file());

        try
        {
            std::unique_ptr<DB::ReadBuffer> rb = read_buffer_builder->build(file);
            while (!rb->eof())
                rb->ignoreAll();
        }
        catch (std::exception & e)
        {
            LOG_ERROR(getLogger("CacheManager"), "Load cache file {} failed.\n {}", file.uri_file(), e.what());
            std::rethrow_exception(std::current_exception());
        }
    };

    return std::move(task);
}

JobId CacheManager::cacheFiles(substrait::ReadRel::LocalFiles file_infos)
{
    JobId id = toString(UUIDHelpers::generateV4());
    Job job(id);
    DB::ReadSettings read_settings = context->getReadSettings();

    if (file_infos.items_size())
    {
        const Poco::URI file_uri(file_infos.items().Get(0).uri_file());
        const auto read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);

        if (context->getConfigRef().getBool(GlutenCacheConfig::ENABLED, false))
            for (const auto & file : file_infos.items())
                job.addTask(cacheFile(file, read_buffer_builder));
        else
            LOG_WARNING(getLogger("CacheManager"), "Load cache skipped because cache not enabled.");
    }

    auto & scheduler = JobScheduler::instance();
    scheduler.scheduleJob(std::move(job));
    return id;
}

void CacheManager::removeFiles(String file, String cache_name)
{
    // only for ut
    for (const auto & [name, file_cache] : FileCacheFactory::instance().getAll())
    {
        if (name != cache_name)
            continue;

        if (const auto cache = file_cache->cache)
            cache->removePathIfExists(file, DB::FileCache::getCommonUser().user_id);
    }
}

}
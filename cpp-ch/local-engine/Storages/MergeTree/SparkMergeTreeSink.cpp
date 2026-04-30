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
#include "SparkMergeTreeSink.h"

#include <Core/Settings.h>
#include <Disks/IDiskTransaction.h>
#include <IO/copyData.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <Common/QueryContext.h>
#include <Common/setThreadName.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
extern const Metric GlobalThread;
extern const Metric GlobalThreadActive;
extern const Metric GlobalThreadScheduled;
}

namespace DB::Setting
{
extern const SettingsUInt64 min_insert_block_size_rows;
extern const SettingsUInt64 min_insert_block_size_bytes;
}
namespace local_engine
{
using namespace DB;
void SparkMergeTreeSink::write(const Chunk & chunk)
{
    CurrentThread::flushUntrackedMemory();
    {
        PartWithStats part_with_stats{
            .data_part = nullptr,
            .delta_stats = empty_delta_stats_
                               .transform(
                                   [&](const auto & stats)
                                   {
                                       auto newStats = std::make_shared<DeltaStats>(stats);
                                       newStats->update(chunk);
                                       return newStats;
                                   })
                               .value_or(nullptr)};
        /// Reset earlier, so put it in the scope
        BlockWithPartition item{getHeader().cloneWithColumns(chunk.getColumns()), Row{}};

        sink_helper->writeTempPart(item, std::move(part_with_stats), context, part_num);
        part_num++;
    }
}

void SparkMergeTreeSink::consume(Chunk & chunk)
{
    Chunk tmp;
    tmp.swap(chunk);
    squashed_chunk = squashing.add(std::move(tmp));
    if (static_cast<bool>(squashed_chunk))
    {
        write(Squashing::squash(std::move(squashed_chunk)));
        sink_helper->checkAndMerge();
    }
    assert(squashed_chunk.getNumRows() == 0);
    assert(chunk.getNumRows() == 0);
}

void SparkMergeTreeSink::onStart()
{
    // DO NOTHING
}

void SparkMergeTreeSink::onFinish()
{
    assert(squashed_chunk.getNumRows() == 0);
    squashed_chunk = squashing.flush();
    if (static_cast<bool>(squashed_chunk))
        write(Squashing::squash(std::move(squashed_chunk)));
    assert(squashed_chunk.getNumRows() == 0);
    sink_helper->finish(context);
    if (stats_.has_value())
        (*stats_)->collectStats(sink_helper->unsafeGet(), sink_helper->write_settings.partition_settings.partition_dir);
}

/////
SinkToStoragePtr SparkMergeTreeSink::create(
    const MergeTreeTable & merge_tree_table,
    const SparkMergeTreeWriteSettings & write_settings_,
    const DB::ContextMutablePtr & context,
    const DeltaStatsOption & delta_stats,
    const SinkStatsOption & stats)
{
    if (write_settings_.partition_settings.part_name_prefix.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "empty part_name_prefix is not allowed.");

    auto dest_storage = merge_tree_table.getStorage(context);
    bool isRemoteStorage = dest_storage->getStoragePolicy()->getAnyDisk()->isRemote();
    bool insert_with_local_storage = !write_settings_.insert_without_local_storage;
    SinkHelperPtr sink_helper;
    if (insert_with_local_storage && isRemoteStorage)
    {
        auto temp = merge_tree_table.copyToDefaultPolicyStorage(context);
        LOG_DEBUG(
            &Poco::Logger::get("SparkMergeTreeWriter"),
            "Create temp table {} for local merge.",
            temp->getStorageID().getFullNameNotQuoted());
        sink_helper = std::make_shared<CopyToRemoteSinkHelper>(temp, dest_storage, write_settings_);
    }
    else
        sink_helper = std::make_shared<DirectSinkHelper>(dest_storage, write_settings_, isRemoteStorage);
    const DB::Settings & settings = context->getSettingsRef();
    return std::make_shared<SparkMergeTreeSink>(
        sink_helper,
        std::make_shared<const DB::Block>(sink_helper->metadata_snapshot->getSampleBlock()),
        context,
        delta_stats,
        stats,
        settings[Setting::min_insert_block_size_rows],
        settings[Setting::min_insert_block_size_bytes]);
}

SinkHelper::SinkHelper(const SparkStorageMergeTreePtr & data_, const SparkMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_)
    : data(data_)
    , isRemoteStorage(isRemoteStorage_)
    , thread_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1, 1, 100000)
    , write_settings(write_settings_)
    , metadata_snapshot(data->getInMemoryMetadataPtr())
{
}

void SinkHelper::saveMetadata(const DB::ContextPtr & context)
{
    if (!isRemoteStorage)
        return;

    const std::deque<PartWithStats> & parts = new_parts.unsafeGet();
    for (const auto & merge_tree_data_part : parts)
    {
        auto part = dest_storage().loadDataPartsWithNames({merge_tree_data_part.data_part->name});
        if (part.empty())
        {
            LOG_WARNING(
                &Poco::Logger::get("SparkMergeTreeWriter"),
                "Save metadata failed because dest storage load part name {} empty.",
                merge_tree_data_part.data_part->name);
            continue;
        }

        saveFileStatus(
            dest_storage(),
            context,
            merge_tree_data_part.data_part->name,
            const_cast<IDataPartStorage &>(part.at(0)->getDataPartStorage()));
    }
}

void SinkHelper::doMergePartsAsync(const std::vector<PartWithStats> & merge_parts_with_stats)
{
    for (const auto & selected_part : merge_parts_with_stats)
        tmp_parts.emplace(selected_part.data_part->name);

    // check a thread group initialized in task thread
    currentThreadGroupMemoryUsage();
    thread_pool.scheduleOrThrow(
        [this, merge_parts_with_stats, thread_group = CurrentThread::getGroup()]() -> void
        {
            ThreadGroupSwitcher switcher(thread_group, ThreadName::ASYNC_MERGE);

            Stopwatch watch;
            size_t before_size = 0;

            std::vector<DB::DataPartPtr> prepare_merge_parts_;
            for (const auto & prepare_merge_part : merge_parts_with_stats)
            {
                before_size += prepare_merge_part.data_part->getBytesOnDisk();
                prepare_merge_parts_.emplace_back(prepare_merge_part.data_part);
            }

            const auto merged_part = mergeParts(
                prepare_merge_parts_,
                toString(UUIDHelpers::generateV4()),
                dataRef(),
                write_settings.partition_settings.partition_dir,
                write_settings.partition_settings.bucket_dir);

            size_t after_size = merged_part->getBytesOnDisk();
            if (std::ranges::any_of(merge_parts_with_stats, [](const auto & part) { return part.delta_stats == nullptr; }))
            {
                // no stats
                new_parts.emplace_back(PartWithStats{std::move(merged_part), nullptr});
            }
            else
            {
                auto merge_stats = merge_parts_with_stats.begin()->delta_stats;
                for (auto begin = merge_parts_with_stats.begin() + 1; begin != merge_parts_with_stats.end(); ++begin)
                    merge_stats->merge(*begin->delta_stats);
                new_parts.emplace_back(PartWithStats{std::move(merged_part), std::move(merge_stats)});
            }

            watch.stop();
            LOG_INFO(
                &Poco::Logger::get("SparkMergeTreeWriter"),
                "Merge success. Before merge part size {}, part count {}, after part size {}, part count {}, "
                "total elapsed {} ms",
                before_size, // before size
                merge_parts_with_stats.size(), // before part count
                after_size, // after size
                1, // after part count
                watch.elapsedMilliseconds());
        });
}
void SinkHelper::writeTempPart(
    DB::BlockWithPartition & block_with_partition, PartWithStats part_with_stats, const ContextPtr & context, int part_num)
{
    assert(!metadata_snapshot->hasPartitionKey());
    const std::string & part_name_prefix = write_settings.partition_settings.part_name_prefix;
    std::string part_dir;
    if (write_settings.is_optimize_task)
        part_dir = fmt::format("{}-merged", part_name_prefix);
    else
        part_dir = fmt::format("{}_{:03d}", part_name_prefix, part_num);
    const auto tmp = dataRef().getWriter().writeTempPart(block_with_partition, metadata_snapshot, context, part_dir);
    part_with_stats.data_part = tmp->part;
    new_parts.emplace_back(std::move(part_with_stats));
}

void SinkHelper::checkAndMerge(bool force)
{
    if (!write_settings.merge_after_insert)
        return;
    // Only finalize should force merge.
    if (!force && new_parts.size() < write_settings.merge_limit_parts)
        return;

    std::vector<PartWithStats> selected_parts;
    selected_parts.reserve(write_settings.merge_limit_parts);
    size_t total_size = 0;
    std::vector<PartWithStats> skip_parts;

    while (const auto merge_tree_data_part_option = new_parts.pop_front())
    {
        auto merge_tree_data_part = merge_tree_data_part_option.value();
        if (merge_tree_data_part.data_part->getBytesOnDisk() >= write_settings.merge_min_size)
        {
            skip_parts.emplace_back(merge_tree_data_part);
            continue;
        }

        selected_parts.emplace_back(merge_tree_data_part);
        total_size += merge_tree_data_part.data_part->getBytesOnDisk();
        if (write_settings.merge_min_size > total_size && write_settings.merge_limit_parts > selected_parts.size())
            continue;

        doMergePartsAsync(selected_parts);
        selected_parts.clear();
        total_size = 0;
    }

    if (!selected_parts.empty())
    {
        if (force && selected_parts.size() > 1)
            doMergePartsAsync(selected_parts);
        else
            new_parts.emplace_back(selected_parts);
    }

    new_parts.emplace_back(skip_parts);
}
void SinkHelper::finish(const DB::ContextPtr & context)
{
    if (write_settings.merge_after_insert)
        finalizeMerge();
    commit(context->getReadSettings(), context->getWriteSettings());
    saveMetadata(context);
}

void SinkHelper::finalizeMerge()
{
    LOG_DEBUG(&Poco::Logger::get("SparkMergeTreeWriter"), "Waiting all merge task end and do final merge");
    // waiting all merge task end and do final merge
    thread_pool.wait();

    size_t before_merge_size;
    do
    {
        before_merge_size = new_parts.size();
        checkAndMerge(true);
        thread_pool.wait();
    } while (before_merge_size != new_parts.size());
    cleanup();
}

void CopyToRemoteSinkHelper::commit(const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    LOG_DEBUG(
        &Poco::Logger::get("SparkMergeTreeWriter"), "Begin upload to disk {}.", dest_storage().getStoragePolicy()->getAnyDisk()->getName());

    const std::deque<PartWithStats> & parts = new_parts.unsafeGet();

    Stopwatch watch;
    for (const auto & merge_tree_data_part : parts)
    {
        String local_relative_path = dataRef().getRelativeDataPath() + "/" + merge_tree_data_part.data_part->name;
        String remote_relative_path = dest_storage().getRelativeDataPath() + "/" + merge_tree_data_part.data_part->name;

        std::vector<String> files;
        dataRef().getStoragePolicy()->getAnyDisk()->listFiles(local_relative_path, files);
        auto src_disk = dataRef().getStoragePolicy()->getAnyDisk();
        auto dest_disk = dest_storage().getStoragePolicy()->getAnyDisk();
        auto tx = dest_disk->createTransaction();
        for (const auto & file : files)
        {
            auto read_buffer = src_disk->readFile(local_relative_path + "/" + file, read_settings);
            auto write_buffer
                = tx->writeFile(remote_relative_path + "/" + file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
            copyData(*read_buffer, *write_buffer);
            write_buffer->finalize();
        }
        tx->commit();
        LOG_DEBUG(
            &Poco::Logger::get("SparkMergeTreeWriter"),
            "Upload part {} to disk {} success.",
            merge_tree_data_part.data_part->name,
            dest_storage().getStoragePolicy()->getAnyDisk()->getName());
    }
    watch.stop();
    LOG_INFO(
        &Poco::Logger::get("SparkMergeTreeWriter"),
        "Upload to disk {} finished, total elapsed {} ms",
        dest_storage().getStoragePolicy()->getAnyDisk()->getName(),
        watch.elapsedMilliseconds());
    StorageMergeTreeFactory::freeStorage(temp_storage()->getStorageID());
    temp_storage()->dropAllData();
    LOG_DEBUG(
        &Poco::Logger::get("SparkMergeTreeWriter"), "Clean temp table {} success.", temp_storage()->getStorageID().getFullNameNotQuoted());
}

void DirectSinkHelper::cleanup()
{
    // default storage need clean temp.
    std::unordered_set<String> final_parts;
    for (const auto & merge_tree_data_part : new_parts.unsafeGet())
        final_parts.emplace(merge_tree_data_part.data_part->name);

    for (const auto & tmp_part : tmp_parts)
    {
        if (final_parts.contains(tmp_part))
            continue;

        GlobalThreadPool::instance().scheduleOrThrow(
            [storage_ = data, tmp = tmp_part]() -> void
            {
                for (const auto & disk : storage_->getDisks())
                {
                    auto rel_path = storage_->getRelativeDataPath() + "/" + tmp;
                    disk->removeRecursive(rel_path);
                }
            });
    }
}

}
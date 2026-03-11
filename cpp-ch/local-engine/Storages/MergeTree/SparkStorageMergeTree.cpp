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
#include "SparkStorageMergeTree.h"

#include <Disks/ObjectStorages/CompactObjectStorageDiskTransaction.h>
#include <Disks/SingleDiskVolume.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <Storages/MergeTree/checkDataPart.h>

namespace ProfileEvents
{
extern const Event LoadedDataParts;
extern const Event LoadedDataPartsMicroseconds;
}

namespace DB
{
namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool assign_part_uuids;
extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
extern const MergeTreeSettingsBool fsync_part_directory;
extern const MergeTreeSettingsBool fsync_after_insert;
}

namespace ErrorCodes
{
extern const int DUPLICATE_DATA_PART;
extern const int NO_SUCH_DATA_PART;

}
}

namespace local_engine
{
using namespace DB;
void SparkStorageMergeTree::analysisPartsByRanges(DB::ReadFromMergeTree & source, const DB::RangesInDataParts & ranges_in_data_parts)
{
    ReadFromMergeTree::AnalysisResult result;
    result.column_names_to_read = source.getAllColumnNames();
    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (result.column_names_to_read.empty())
    {
        NamesAndTypesList available_real_columns = source.getStorageMetadata()->getColumns().getAllPhysical();
        result.column_names_to_read.push_back(ExpressionActions::getSmallestColumn(available_real_columns).name);
    }

    result.sampling = MergeTreeDataSelectSamplingData();
    result.parts_with_ranges = ranges_in_data_parts;

    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    size_t sum_rows = 0;
    size_t total_marks_pk = 0;
    size_t sum_marks_pk = 0;

    for (const auto & part : result.parts_with_ranges)
    {
        sum_ranges += part.ranges.size();
        sum_marks += part.getMarksCount();
        sum_rows += part.getRowsCount();
        total_marks_pk += part.data_part->index_granularity->getMarksCountWithoutFinal();

        for (auto range : part.ranges)
            sum_marks_pk += range.getNumberOfMarks();
    }

    result.total_parts = ranges_in_data_parts.size();
    result.parts_before_pk = ranges_in_data_parts.size();
    result.selected_parts = ranges_in_data_parts.size();
    result.selected_ranges = sum_ranges;
    result.selected_marks = sum_marks;
    result.selected_marks_pk = sum_marks_pk;
    result.total_marks_pk = total_marks_pk;
    result.selected_rows = sum_rows;

    if (source.getQueryInfo().input_order_info)
        result.read_type
            = (source.getQueryInfo().input_order_info->direction > 0) ? MergeTreeReadType::InOrder : MergeTreeReadType::InReverseOrder;

    source.setAnalyzedResult(std::make_shared<ReadFromMergeTree::AnalysisResult>(std::move(result)));
}

void SparkStorageMergeTree::wrapRangesInDataParts(DB::ReadFromMergeTree & source, const DB::RangesInDataParts & ranges)
{
    auto result = source.getAnalysisResult();
    std::unordered_map<String, std::tuple<size_t, size_t>> range_index;
    for (const auto & part_with_range : ranges)
    {
        chassert(part_with_range.ranges.size() == 1);
        const auto & range = part_with_range.ranges.at(0);
        range_index.emplace(part_with_range.data_part->name, std::make_tuple(range.begin, range.end));
    }
    RangesInDataParts final;
    for (auto & parts_with_range : result.parts_with_ranges)
    {
        if (!range_index.contains(parts_with_range.data_part->name))
            continue;

        auto expected_range = range_index.at(parts_with_range.data_part->name);
        MarkRanges final_ranges;
        for (const auto & range : parts_with_range.ranges)
        {
            const size_t begin = std::max(range.begin, std::get<0>(expected_range));
            const size_t end = std::min(range.end, std::get<1>(expected_range));
            // [1, 1) or [5, 2) are invalid.
            if (begin >= end)
                continue;
            MarkRange final_range(begin, end);
            final_ranges.emplace_back(final_range);
        }
        parts_with_range.ranges = final_ranges;
        final.emplace_back(parts_with_range);
    }

    result.parts_with_ranges = final;
    source.setAnalyzedResult(std::make_shared<ReadFromMergeTree::AnalysisResult>(result));
}

SparkStorageMergeTree::SparkStorageMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach,
    const ContextMutablePtr & context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool /*has_force_restore_data_flag*/)
    : MergeTreeData(
          table_id_,
          metadata_,
          context_,
          date_column_name,
          merging_params_,
          std::move(storage_settings_),
          false, /// require_part_metadata
          attach ? LoadingStrictnessLevel::ATTACH : LoadingStrictnessLevel::FORCE_RESTORE)
    , reader(*this)
    , merger_mutator(*this)
{
    relative_data_path = relative_data_path_;
    format_version = 1;
}

std::atomic<int> SparkStorageMergeTree::part_num;

void SparkStorageMergeTree::prefetchPartDataFile(const std::unordered_set<std::string> & parts) const
{
    prefetchPartFiles(parts, CompactObjectStorageDiskTransaction::PART_DATA_FILE_NAME);
}

void SparkStorageMergeTree::prefetchPartFiles(const std::unordered_set<std::string> & parts, String file_name) const
{
    auto disk = getDisks().front();
    if (!disk->isRemote())
        return;
    std::vector<String> data_paths;
    std::ranges::for_each(parts, [&](const String & name) { data_paths.emplace_back(fs::path(relative_data_path) / name / file_name); });
    auto read_settings = ReadSettings{};
    read_settings.remote_fs_method = RemoteFSReadMethod::read;
    for (const auto & data_path : data_paths)
    {
        if (!disk->existsFile(data_path))
            continue;
        LOG_DEBUG(log, "Prefetching part file {}", data_path);
        auto in = disk->readFile(data_path, read_settings);
        String ignore_data;
        readStringUntilEOF(ignore_data, *in);
    }
}

void SparkStorageMergeTree::prefetchMetaDataFile(const std::unordered_set<std::string> & parts) const
{
    prefetchPartFiles(parts, CompactObjectStorageDiskTransaction::PART_META_FILE_NAME);
}

std::vector<MergeTreeDataPartPtr> SparkStorageMergeTree::loadDataPartsWithNames(const std::unordered_set<std::string> & parts)
{
    Stopwatch watch;
    prefetchMetaDataFile(parts);
    std::vector<MergeTreeDataPartPtr> data_parts;
    const auto disk = getStoragePolicy()->getDisks().at(0);
    for (const auto & name : parts)
    {
        const auto num = part_num.fetch_add(1);
        MergeTreePartInfo part_info = {"all", num, num, 0};
        auto res = loadDataPart(part_info, name, disk, MergeTreeDataPartState::Active);
        data_parts.emplace_back(res.part);
    }

    watch.stop();
    LOG_DEBUG(log, "Loaded data parts ({} items) took {} microseconds", parts.size(), watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::LoadedDataParts, parts.size());
    ProfileEvents::increment(ProfileEvents::LoadedDataPartsMicroseconds, watch.elapsedMicroseconds());
    return data_parts;
}

MergeTreeData::LoadPartResult SparkStorageMergeTree::loadDataPart(
    const MergeTreePartInfo & part_info, const String & part_name, const DiskPtr & part_disk_ptr, MergeTreeDataPartState to_state)
{
    LOG_TRACE(log, "Loading {} part {} from disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());

    LoadPartResult res;
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
    String part_path = fs::path(relative_data_path) / part_name;

    auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(single_disk_volume, "", part_path);

    try
    {
        res.part = getDataPartBuilder(part_name, single_disk_volume, part_name, getContext()->getReadSettings())
                       .withPartInfo(part_info)
                       .withPartFormatFromDisk()
                       .build();
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;
        LOG_DEBUG(log, "Failed to load data part {}, unknown exception", part_name);
        return res;
    }

    try
    {
        res.part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;
        return res;
    }

    res.part->modification_time = part_disk_ptr->getLastModified(fs::path(relative_data_path) / part_name).epochTime();
    res.part->loadVersionMetadata();

    res.part->setState(to_state);
    auto parts_lock = lockParts();

    DataPartIteratorByInfo it;
    bool inserted;

    {
        LOG_TEST(log, "loadDataPart: inserting {} into data_parts_indexes", res.part->getNameWithState());
        std::tie(it, inserted) = data_parts_indexes.insert(res.part);
    }

    /// Remove duplicate parts with the same checksum.
    if (!inserted)
    {
        if ((*it)->checksums.getTotalChecksumHex() == res.part->checksums.getTotalChecksumHex())
        {
            LOG_ERROR(log, "Remove duplicate part {}", data_part_storage->getFullPath());
            res.part->is_duplicate = true;
            return res;
        }
        else
            throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists but with different checksums", res.part->name);
    }

    // if (to_state == DataPartState::Active)
    //     addPartContributionToDataVolume(res.part);

    if (res.part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    // without it "test mergetree optimize partitioned by one low card column" will log ERROR
    resetColumnSizes();
    calculateColumnAndSecondaryIndexSizesImpl();

    LOG_TRACE(log, "Finished loading {} part {} on disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());
    return res;
}

void SparkStorageMergeTree::removePartFromMemory(const MergeTreeData::DataPart & part_to_detach)
{
    auto lock = lockParts();
    bool removed_active_part = false;
    bool restored_active_part = false;

    auto it_part = data_parts_by_info.find(part_to_detach.info);
    if (it_part == data_parts_by_info.end())
    {
        LOG_DEBUG(log, "No such data part {}", part_to_detach.getNameWithState());
        return;
    }

    /// What if part_to_detach is a reference to *it_part? Make a new owner just in case.
    /// Important to own part pointer here (not const reference), because it will be removed from data_parts_indexes
    /// few lines below.
    DataPartPtr part = *it_part; // NOLINT

    if (part->getState() == DataPartState::Active)
    {
        removePartContributionToColumnAndSecondaryIndexSizes(part);
        removed_active_part = true;
    }

    modifyPartState(it_part, DataPartState::Deleting);
    LOG_TEST(log, "removePartFromMemory: removing {} from data_parts_indexes", part->getNameWithState());
    data_parts_indexes.erase(it_part);

    if (removed_active_part || restored_active_part)
        resetObjectColumnsFromActiveParts(lock);
}

void SparkStorageMergeTree::dropPartNoWaitNoThrow(const String & /*part_name*/)
{
    throw std::runtime_error("not implement");
}
void SparkStorageMergeTree::dropPart(const String & /*part_name*/, bool /*detach*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void SparkStorageMergeTree::dropPartition(const ASTPtr & /*partition*/, bool /*detach*/, ContextPtr /*context*/)
{
}
PartitionCommandsResultInfo SparkStorageMergeTree::attachPartition(
    const ASTPtr & /*partition*/, const StorageMetadataPtr & /*metadata_snapshot*/, bool /*part*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void SparkStorageMergeTree::replacePartitionFrom(
    const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void SparkStorageMergeTree::movePartitionToTable(const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
bool SparkStorageMergeTree::partIsAssignedToBackgroundOperation(const MergeTreeData::DataPartPtr & /*part*/) const
{
    throw std::runtime_error("not implement");
}

std::string SparkStorageMergeTree::getName() const
{
    throw std::runtime_error("not implement");
}
std::vector<MergeTreeMutationStatus> SparkStorageMergeTree::getMutationsStatus() const
{
    throw std::runtime_error("not implement");
}
bool SparkStorageMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & /*executor*/)
{
    throw std::runtime_error("not implement");
}
void SparkStorageMergeTree::startBackgroundMovesIfNeeded()
{
    throw std::runtime_error("not implement");
}
std::unique_ptr<MergeTreeSettings> SparkStorageMergeTree::getDefaultSettings() const
{
    throw std::runtime_error("not implement");
}
std::map<std::string, MutationCommands> SparkStorageMergeTree::getUnfinishedMutationCommands() const
{
    throw std::runtime_error("not implement");
}

MergeTreeTemporaryPartPtr SparkMergeTreeDataWriter::writeTempPart(
    BlockWithPartition & block_with_partition,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const std::string & part_dir) const
{
    auto temp_part = std::make_unique<MergeTreeTemporaryPart>();

    Block & block = block_with_partition.block;

    auto columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());

    for (auto & column : columns)
        if (column.type->hasDynamicSubcolumns())
            column.type = block.getByName(column.name).type;

    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    minmax_idx->update(block, MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));

    MergeTreePartition partition(block_with_partition.partition);

    MergeTreePartInfo new_part_info(partition.getID(metadata_snapshot->getPartitionKey().sample_block), 1, 1, 0);

    temp_part->temporary_directory_lock = data.getTemporaryPartDirectoryHolder(part_dir);

    auto indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
        data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, indices)->execute(block);

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(sort_columns[i], 1, 1);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        if (!isAlreadySorted(block, sort_description))
        {
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
    }

    Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();

    /// If optimize_on_insert is true, block may become empty after merge.
    /// There is no need to create empty part.
    if (expected_size == 0)
        return temp_part;

    VolumePtr volume = data.getStoragePolicy()->getVolume(0);
    VolumePtr data_part_volume = std::make_shared<SingleDiskVolume>(volume->getName(), volume->getDisk(), volume->max_data_part_size);
    auto new_data_part = data.getDataPartBuilder(part_dir, data_part_volume, part_dir, context->getReadSettings())
                             .withPartFormat(data.choosePartFormat(expected_size, block.rows()))
                             .withPartInfo(new_part_info)
                             .build();

    auto data_part_storage = new_data_part->getDataPartStoragePtr();


    const MergeTreeSettings & data_settings = *data.getSettings();

    SerializationInfo::Settings settings{data_settings[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization], true};
    SerializationInfoByName infos(columns, settings);
    infos.add(block);

    new_data_part->setColumns(columns, infos, metadata_snapshot->getMetadataVersion());
    new_data_part->rows_count = block.rows();
    new_data_part->partition = std::move(partition);
    new_data_part->minmax_idx = std::move(minmax_idx);

    data_part_storage->beginTransaction();

    if (data_settings[MergeTreeSetting::assign_part_uuids])
        new_data_part->uuid = UUIDHelpers::generateV4();

    SyncGuardPtr sync_guard;

    /// The name could be non-unique in case of stale files from previous runs.
    String full_path = new_data_part->getDataPartStorage().getFullPath();

    if (new_data_part->getDataPartStorage().exists())
    {
        LOG_WARNING(log, "Removing old temporary directory {}", full_path);
        data_part_storage->removeRecursive();
    }

    data_part_storage->createDirectories();

    if ((*data.getSettings())[MergeTreeSetting::fsync_part_directory])
    {
        const auto disk = data_part_volume->getDisk();
        sync_guard = disk->getDirectorySyncGuard(full_path);
    }

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = data.getContext()->chooseCompressionCodec(0, 0);
    auto txn = context->getCurrentTransaction();
    auto index_granularity_ptr = createMergeTreeIndexGranularity(
        block.rows(),
        block.bytes(),
        *data.getSettings(),
        new_data_part->index_granularity_info,
        /*blocks_are_granules=*/false);
    auto out = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        columns,
        indices,
        MergeTreeStatisticsFactory::instance().getMany(metadata_snapshot->getColumns()),
        compression_codec,
        index_granularity_ptr,
        txn ? txn->tid : Tx::PrehistoricTID,
        block.bytes(),
        false,
        false,
        context->getWriteSettings());

    out->writeWithPermutation(block, perm_ptr);
    auto finalizer = out->finalizePartAsync(new_data_part, data_settings[MergeTreeSetting::fsync_after_insert], nullptr, nullptr);

    temp_part->part = new_data_part;
    temp_part->streams.emplace_back(MergeTreeTemporaryPart::Stream{.stream = std::move(out), .finalizer = std::move(finalizer)});
    temp_part->finalize();
    data_part_storage->commitTransaction();
    return temp_part;
}

std::unique_ptr<MergeTreeSettings>
SparkWriteStorageMergeTree::buildMergeTreeSettings(const ContextMutablePtr & context, const MergeTreeTableSettings & config)
{
    //TODO: set settings though ASTStorage
    auto settings = std::make_unique<DB::MergeTreeSettings>();

    settings->set("allow_nullable_key", Field(true));
    if (!config.storage_policy.empty())
        settings->set("storage_policy", Field(config.storage_policy));

    if (settingsEqual(context->getSettingsRef(), "merge_tree.assign_part_uuids", "true"))
        settings->set("assign_part_uuids", Field(true));

    if (String min_rows_for_wide_part; tryGetString(context->getSettingsRef(), "merge_tree.min_rows_for_wide_part", min_rows_for_wide_part))
        settings->set("min_rows_for_wide_part", Field(std::strtoll(min_rows_for_wide_part.c_str(), nullptr, 10)));

    if (settingsEqual(context->getSettingsRef(), "merge_tree.write_marks_for_substreams_in_compact_parts", "true"))
        settings->set("write_marks_for_substreams_in_compact_parts", Field(true));

    return settings;
}

SinkToStoragePtr SparkWriteStorageMergeTree::write(
    const ASTPtr &, const StorageMetadataPtr & /*storage_in_memory_metadata*/, ContextPtr context, bool /*async_insert*/)
{
#ifndef NDEBUG
    auto dest_storage = table.getStorage(getContext());
    assert(dest_storage.get() == this);
#endif
    return SparkMergeTreeSink::create(table, SparkMergeTreeWriteSettings{context}, getContext());
}

}

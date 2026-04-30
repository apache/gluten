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

#include "GraceAggregatingTransform.h"
#include <Processors/Port.h>
#include <Common/BitHelpers.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/CurrentThread.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>
#include <Common/formatReadable.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine
{
GraceAggregatingTransform::GraceAggregatingTransform(
    const DB::SharedHeader & header_,
    DB::AggregatingTransformParamsPtr params_,
    DB::ContextPtr context_,
    bool no_pre_aggregated_,
    bool final_output_)
    : IProcessor({header_}, {params_->getHeader()})
    , header(header_)
    , params(params_)
    , context(context_)
    , key_columns(params_->params.keys_size)
    , aggregate_columns(params_->params.aggregates_size)
    , no_pre_aggregated(no_pre_aggregated_)
    , final_output(final_output_)
    , tmp_data_disk(context_->getTempDataOnDisk())
{
    output_header = params->getHeader();
    auto config = GraceMergingAggregateConfig::loadFromContext(context);
    max_buckets = config.max_grace_aggregate_merging_buckets;
    throw_on_overflow_buckets = config.throw_on_overflow_grace_aggregate_merging_buckets;
    aggregated_keys_before_extend_buckets = config.aggregated_keys_before_extend_grace_aggregate_merging_buckets;
    aggregated_keys_before_extend_buckets = PODArrayUtil::adjustMemoryEfficientSize(aggregated_keys_before_extend_buckets);
    max_pending_flush_blocks_per_bucket = config.max_pending_flush_blocks_per_grace_aggregate_merging_bucket;
    max_allowed_memory_usage_ratio = config.max_allowed_memory_usage_ratio_for_aggregate_merging;
    // bucket 0 is for in-memory data, it's just a placeholder.
    buckets.emplace(0, BufferFileStream());
    enable_spill_test = config.enable_spill_test;
    if (enable_spill_test)
        buckets.emplace(1, BufferFileStream());
    current_data_variants = std::make_shared<DB::AggregatedDataVariants>();

    // IProcessor::spillable, MemorySpillScheduler will trigger the spill by enable this flag.
    spillable = true;
}

GraceAggregatingTransform::~GraceAggregatingTransform()
{
    LOG_INFO(
        logger,
        "Metrics. total_input_blocks: {}, total_input_rows: {}, total_output_blocks: {}, total_output_rows: {}, total_spill_disk_bytes: "
        "{}, total_spill_disk_time: {}, total_read_disk_time: {}, total_scatter_time: {}",
        total_input_blocks,
        total_input_rows,
        total_output_blocks,
        total_output_rows,
        total_spill_disk_bytes,
        total_spill_disk_time,
        total_read_disk_time,
        total_scatter_time);
}

GraceAggregatingTransform::Status GraceAggregatingTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (output.isFinished() || isCancelled())
    {
        input.close();
        return Status::Finished;
    }
    if (has_output)
    {
        if (output.canPush())
        {
            LOG_DEBUG(
                logger,
                "Output one chunk. rows: {}, bytes: {}, current memory usage: {}",
                output_chunk.getNumRows(),
                ReadableSize(output_chunk.bytes()),
                ReadableSize(currentThreadGroupMemoryUsage()));
            total_output_rows += output_chunk.getNumRows();
            total_output_blocks++;
            output.push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;


    if (!input_finished)
    {
        if (input.isFinished())
        {
            input_finished = true;
            return Status::Ready;
        }
        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;
        input_chunk = input.pull(true);
        LOG_DEBUG(
            logger,
            "Input one new chunk. rows: {}, bytes: {}, current memory usage: {}",
            input_chunk.getNumRows(),
            ReadableSize(input_chunk.bytes()),
            ReadableSize(currentThreadGroupMemoryUsage()));
        total_input_rows += input_chunk.getNumRows();
        total_input_blocks++;
        has_input = true;
        return Status::Ready;
    }

    if (current_bucket_index >= getBucketsNum() && (!block_converter || !block_converter->hasNext()))
    {
        output.finish();
        return Status::Finished;
    }
    return Status::Ready;
}

void GraceAggregatingTransform::work()
{
    if (has_input)
    {
        assert(!input_finished);
        auto block = header->cloneWithColumns(input_chunk.detachColumns());
        mergeOneBlock(block, true);
        has_input = false;
    }
    else
    {
        assert(input_finished);
        if (!block_converter || !block_converter->hasNext())
        {
            block_converter = nullptr;
            while (current_bucket_index < getBucketsNum())
            {
                block_converter = prepareBucketOutputBlocks(current_bucket_index);
                if (block_converter)
                    break;
                current_bucket_index++;
            }
        }
        if (!block_converter)
        {
            return;
        }

        while (block_converter->hasNext())
        {
            auto block = block_converter->next();
            if (!block.rows())
                continue;
            output_chunk = DB::Chunk(block.getColumns(), block.rows());
            has_output = true;
            break;
        }

        if (!block_converter->hasNext())
        {
            block_converter = nullptr;
            current_bucket_index++;
        }
    }
}

bool GraceAggregatingTransform::extendBuckets()
{
    if (!current_data_variants || current_data_variants->size() < aggregated_keys_before_extend_buckets)
        return false;

    auto current_size = getBucketsNum();
    auto next_size = current_size * 2;
    /// We have a soft limit on the number of buckets. When throw_on_overflow_buckets = false, we just
    /// continue to run with the current number of buckets until the executor is killed by spark scheduler.
    if (next_size > max_buckets)
    {
        if (throw_on_overflow_buckets)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Too many buckets, limit is {}. Please consider increate offhead size or partitoin number",
                max_buckets);
        else
            return false;
    }
    LOG_DEBUG(logger, "Extend buckets num from {} to {}", current_size, next_size);
    for (size_t i = current_size; i < next_size; ++i)
        buckets.emplace(i, BufferFileStream());
    return true;
}

void GraceAggregatingTransform::rehashDataVariants()
{
    auto before_memoery_usage = currentThreadGroupMemoryUsage();

    auto converter = currentDataVariantToBlockConverter(false);
    checkAndSetupCurrentDataVariants();
    size_t block_rows = 0;
    size_t block_memory_usage = 0;
    no_more_keys = false;

    size_t bucket_n = 0;
    while (converter->hasNext())
    {
        auto block = converter->next();
        if (!block.rows())
            continue;
        block_rows += block.rows();
        block_memory_usage += block.allocatedBytes();
        auto scattered_blocks = scatterBlock(block);
        block = {};
        /// the new scattered blocks from block will alway belongs to the buckets with index >= current_bucket_index
        for (size_t i = 0; i < current_bucket_index; ++i)
        {
            if (scattered_blocks[i].rows())
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "Scattered blocks should not belong to buckets with index({}) < current_bucket_index({})",
                    i,
                    current_bucket_index);
        }
        for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        {
            addBlockIntoFileBucket(i, scattered_blocks[i], false);
            scattered_blocks[i] = {};
        }

        params->aggregator.mergeOnBlock(scattered_blocks[current_bucket_index], *current_data_variants, no_more_keys, is_cancelled);
    }
    if (block_rows)
        per_key_memory_usage = block_memory_usage * 1.0 / block_rows;

    LOG_INFO(
        logger,
        "Rehash data variants. current_bucket_index: {}, buckets num: {}, memory usage change, from {} to {}",
        current_bucket_index,
        getBucketsNum(),
        ReadableSize(before_memoery_usage),
        ReadableSize(currentThreadGroupMemoryUsage()));
};

DB::Blocks GraceAggregatingTransform::scatterBlock(const DB::Block & block)
{
    if (!block.rows())
        return {};
    Stopwatch watch;
    size_t bucket_num = getBucketsNum();
    auto blocks = DB::JoinCommon::scatterBlockByHash(params->params.keys, block, bucket_num);
    for (auto & new_block : blocks)
    {
        new_block.info.bucket_num = static_cast<Int32>(bucket_num);
    }
    total_scatter_time += watch.elapsedMilliseconds();
    return blocks;
}

void GraceAggregatingTransform::addBlockIntoFileBucket(size_t bucket_index, const DB::Block & block, bool is_original_block)
{
    if (!block.rows())
        return;
    if (roundUpToPowerOfTwoOrZero(bucket_index + 1) > static_cast<size_t>(block.info.bucket_num))
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Add invalid block with bucket_num {} into bucket {}", block.info.bucket_num, bucket_index);
    }
    auto & file_stream = buckets[bucket_index];
    file_stream.pending_bytes += block.allocatedBytes();
    if (is_original_block && no_pre_aggregated)
        file_stream.original_blocks.push_back(block);
    else
        file_stream.intermediate_blocks.push_back(block);
    if (file_stream.pending_bytes > max_pending_flush_blocks_per_bucket || (file_stream.pending_bytes && enable_spill_test))
    {
        flushBucket(bucket_index);
        file_stream.pending_bytes = 0;
    }
}

void GraceAggregatingTransform::flushBuckets()
{
    for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        flushBucket(i);
}

static size_t flushBlocksInfoDisk(std::optional<DB::TemporaryBlockStreamHolder> & file_stream, std::list<DB::Block> & blocks)
{
    size_t flush_bytes = 0;
    DB::Blocks tmp_blocks;
    if (!file_stream)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "file_stream is empty");
    auto & tmp_stream = file_stream.value();
    while (!blocks.empty())
    {
        while (!blocks.empty())
        {
            if (!tmp_blocks.empty() && tmp_blocks.back().info.bucket_num != blocks.front().info.bucket_num)
                break;
            tmp_blocks.push_back(blocks.front());
            blocks.pop_front();
        }
        auto bucket = tmp_blocks.front().info.bucket_num;
        auto merged_block = BlockUtil::concatenateBlocksMemoryEfficiently(std::move(tmp_blocks));
        merged_block.info.bucket_num = bucket;
        tmp_blocks.clear();
        flush_bytes += merged_block.bytes();
        if (merged_block.rows())
        {
            tmp_stream->write(merged_block);
        }
    }
    if (flush_bytes)
        tmp_stream->flush();
    return flush_bytes;
}

size_t GraceAggregatingTransform::flushBucket(size_t bucket_index)
{
    Stopwatch watch;
    auto & file_stream = buckets[bucket_index];
    size_t flush_bytes = 0;
    if (!file_stream.original_blocks.empty())
    {
        if (!file_stream.original_file_stream)
            file_stream.original_file_stream = DB::TemporaryBlockStreamHolder(header, tmp_data_disk);
        flush_bytes += flushBlocksInfoDisk(file_stream.original_file_stream, file_stream.original_blocks);
    }
    if (!file_stream.intermediate_blocks.empty())
    {
        if (!file_stream.intermediate_file_stream)
        {
            auto intermediate_header = toShared(params->aggregator.getHeader(false));
            file_stream.intermediate_file_stream = DB::TemporaryBlockStreamHolder(intermediate_header, tmp_data_disk);
        }
        flush_bytes += flushBlocksInfoDisk(file_stream.intermediate_file_stream, file_stream.intermediate_blocks);
    }
    total_spill_disk_bytes += flush_bytes;
    total_spill_disk_time += watch.elapsedMilliseconds();
    return flush_bytes;
}

std::unique_ptr<AggregateDataBlockConverter> GraceAggregatingTransform::prepareBucketOutputBlocks(size_t bucket_index)
{
    auto & buffer_file_stream = buckets[bucket_index];
    if (!current_data_variants && !buffer_file_stream.intermediate_file_stream && buffer_file_stream.intermediate_blocks.empty()
        && !buffer_file_stream.original_file_stream && buffer_file_stream.original_blocks.empty())
    {
        return nullptr;
    }

    size_t read_bytes = 0;
    size_t read_rows = 0;
    Stopwatch watch;

    checkAndSetupCurrentDataVariants();

    if (buffer_file_stream.intermediate_file_stream)
    {
        buffer_file_stream.intermediate_file_stream->finishWriting();
        auto reader = buffer_file_stream.intermediate_file_stream->getReadStream();
        while (true)
        {
            auto block = reader->read();
            if (!block.rows())
                break;
            read_bytes += block.bytes();
            read_rows += block.rows();
            mergeOneBlock(block, false);
            block = {};
        }
        buffer_file_stream.intermediate_file_stream.reset();
        total_read_disk_time += watch.elapsedMilliseconds();
    }
    if (!buffer_file_stream.intermediate_blocks.empty())
    {
        for (auto & block : buffer_file_stream.intermediate_blocks)
        {
            mergeOneBlock(block, false);
            block = {};
        }
    }

    if (buffer_file_stream.original_file_stream)
    {
        buffer_file_stream.original_file_stream->finishWriting();
        auto reader = buffer_file_stream.original_file_stream->getReadStream();
        while (true)
        {
            auto block = reader->read();
            if (!block.rows())
                break;
            read_bytes += block.bytes();
            read_rows += block.rows();
            mergeOneBlock(block, true);
            block = {};
        }
        buffer_file_stream.original_file_stream.reset();
        total_read_disk_time += watch.elapsedMilliseconds();
    }
    if (!buffer_file_stream.original_blocks.empty())
    {
        for (auto & block : buffer_file_stream.original_blocks)
        {
            mergeOneBlock(block, true);
            block = {};
        }
    }

    auto last_data_variants_size = current_data_variants->size();
    auto converter = currentDataVariantToBlockConverter(final_output);
    LOG_INFO(
        logger,
        "prepare to output bucket {}, aggregated result keys: {}, keys size: {}, read bytes from disk: {}, read rows: {}, time: {} ms",
        bucket_index,
        last_data_variants_size,
        params->params.keys_size,
        ReadableSize(read_bytes),
        read_rows,
        watch.elapsedMilliseconds());
    return std::move(converter);
}

std::unique_ptr<AggregateDataBlockConverter> GraceAggregatingTransform::currentDataVariantToBlockConverter(bool final)
{
    if (!current_data_variants)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "current data variants is null");
    }
    auto converter = std::make_unique<AggregateDataBlockConverter>(params->aggregator, current_data_variants, final);
    current_data_variants = nullptr;
    return std::move(converter);
}

void GraceAggregatingTransform::checkAndSetupCurrentDataVariants()
{
    if (!current_data_variants)
    {
        current_data_variants = std::make_shared<DB::AggregatedDataVariants>();
        no_more_keys = false;
    }
}

void GraceAggregatingTransform::mergeOneBlock(const DB::Block & block, bool is_original_block)
{
    if (!block.rows())
        return;

    checkAndSetupCurrentDataVariants();

    // first to flush pending bytes into disk.
    if (isMemoryOverflow())
        flushBuckets();
    // then try to extend buckets.
    if (isMemoryOverflow() && extendBuckets())
    {
        rehashDataVariants();
    }
    // reset the flag
    force_spill = false;

    LOG_DEBUG(
        logger,
        "merge on block, rows: {}, bytes:{}, bucket: {}. current bucket: {}, total bucket: {}, mem used: {}",
        block.rows(),
        ReadableSize(block.bytes()),
        block.info.bucket_num,
        current_bucket_index,
        getBucketsNum(),
        ReadableSize(currentThreadGroupMemoryUsage()));

    /// the block could be one read from disk. block.info.bucket_num stores the number of buckets when it was scattered.
    /// so if the buckets number is not changed since it was scattered, we don't need to scatter it again.
    if (block.info.bucket_num == static_cast<Int32>(getBucketsNum()) || getBucketsNum() == 1)
    {
        if (is_original_block && no_pre_aggregated)
            params->aggregator.executeOnBlock(block, *current_data_variants, key_columns, aggregate_columns, no_more_keys);
        else
            params->aggregator.mergeOnBlock(block, *current_data_variants, no_more_keys, is_cancelled);
    }
    else
    {
        auto bucket_num = block.info.bucket_num;
        auto scattered_blocks = scatterBlock(block);
        for (size_t i = 0; i < current_bucket_index; ++i)
        {
            if (scattered_blocks[i].rows())
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "Scattered blocks should not belong to buckets with index({}) < current_bucket_index({}). bucket_num:{}. "
                    "scattered_blocks.size: {}, total buckets: {}",
                    i,
                    current_bucket_index,
                    bucket_num,
                    scattered_blocks.size(),
                    getBucketsNum());
            }
        }
        for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        {
            addBlockIntoFileBucket(i, scattered_blocks[i], is_original_block);
        }

        if (is_original_block && no_pre_aggregated)
        {
            params->aggregator.executeOnBlock(
                scattered_blocks[current_bucket_index], *current_data_variants, key_columns, aggregate_columns, no_more_keys);
        }
        else
        {
            params->aggregator.mergeOnBlock(scattered_blocks[current_bucket_index], *current_data_variants, no_more_keys, is_cancelled);
        }
    }
}

bool GraceAggregatingTransform::isMemoryOverflow()
{
    if (force_spill)
    {
        auto stats = getMemoryStats();
        if (stats.spillable_memory_bytes > force_spill_on_bytes * 0.8)
            return true;
    }

    /// More greedy memory usage strategy.
    if (!current_data_variants)
        return false;

    auto memory_soft_limit = DB::CurrentThread::getGroup()->memory_tracker.getSoftLimit();
    if (!memory_soft_limit)
        return false;
    auto max_mem_used = static_cast<size_t>(memory_soft_limit * max_allowed_memory_usage_ratio);
    auto current_result_rows = current_data_variants->size();
    auto current_mem_used = currentThreadGroupMemoryUsage();
    if (per_key_memory_usage > 0)
    {
        if (current_mem_used + per_key_memory_usage * current_result_rows >= max_mem_used)
        {
            LOG_INFO(
                logger,
                "Memory is overflow. current_mem_used: {}, max_mem_used: {}, per_key_memory_usage: {}, aggregator keys: {}, buckets: {}, "
                "hash table type: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                ReadableSize(per_key_memory_usage),
                current_result_rows,
                getBucketsNum(),
                current_data_variants->type);
            return true;
        }
    }
    else
    {
        if (current_mem_used * 2 >= max_mem_used)
        {
            LOG_INFO(
                logger,
                "Memory is overflow on half of max usage. current_mem_used: {}, max_mem_used: {}, aggregator keys: {}, buckets: {}, hash "
                "table type: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                current_result_rows,
                getBucketsNum(),
                current_data_variants->type);
            return true;
        }
    }
    return false;
}

DB::ProcessorMemoryStats GraceAggregatingTransform::getMemoryStats()
{
    DB::ProcessorMemoryStats stats;
    if (!current_data_variants)
        return stats;

    stats.need_reserved_memory_bytes = current_data_variants->aggregates_pool->allocatedBytes();
    for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
    {
        auto & file_stream = buckets[i];
        stats.spillable_memory_bytes += file_stream.pending_bytes;
    }

    if (per_key_memory_usage > 0)
    {
        auto current_result_rows = current_data_variants->size();
        stats.need_reserved_memory_bytes += current_result_rows * per_key_memory_usage;
        stats.spillable_memory_bytes += current_result_rows * per_key_memory_usage;
    }
    else
    {
        // This is a rough estimation, we don't know the exact memory usage for each key.
        stats.spillable_memory_bytes += current_data_variants->aggregates_pool->allocatedBytes();
    }
    return stats;
}

bool GraceAggregatingTransform::spillOnSize(size_t bytes)
{
    auto stats = getMemoryStats();
    if (stats.spillable_memory_bytes < bytes * 0.8)
        return false;
    force_spill = true;
    force_spill_on_bytes = bytes;
    return true;
}

}

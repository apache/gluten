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

#include "AggregateUtil.h"
#include <Core/Settings.h>
#include <Poco/Logger.h>
#include <Common/AggregateUtil.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

namespace Setting
{
extern const SettingsDouble max_bytes_ratio_before_external_group_by;
extern const SettingsUInt64 max_bytes_before_external_group_by;
extern const SettingsBool optimize_group_by_constant_keys;
extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
extern const SettingsMaxThreads max_threads;
extern const SettingsBool empty_result_for_aggregation_by_empty_set;
extern const SettingsUInt64 group_by_two_level_threshold_bytes;
extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
extern const SettingsUInt64 max_rows_to_group_by;
extern const SettingsBool enable_memory_bound_merging_of_aggregation_results;
extern const SettingsUInt64 aggregation_in_order_max_block_bytes;
extern const SettingsUInt64 group_by_two_level_threshold;
extern const SettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
extern const SettingsUInt64 max_block_size;
extern const SettingsBool compile_aggregate_expressions;
extern const SettingsUInt64 min_count_to_compile_aggregate_expression;
extern const SettingsBool enable_software_prefetch_in_aggregation;
extern const SettingsBool enable_producing_buckets_out_of_order_in_aggregation;
extern const SettingsBool serialize_string_in_memory_with_zero_byte;
}

template <typename Method>
static Int32 extractMethodBucketsNum(Method & /*method*/)
{
    return Method::Data::NUM_BUCKETS;
}

Int32 GlutenAggregatorUtil::getBucketsNum(AggregatedDataVariants & data_variants)
{
    if (!data_variants.isTwoLevel())
        return 0;

    Int32 buckets_num = 0;
#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) buckets_num = extractMethodBucketsNum(*data_variants.NAME);

    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant");
    return buckets_num;
}

std::optional<Block> GlutenAggregatorUtil::safeConvertOneBucketToBlock(
    Aggregator & aggregator, AggregatedDataVariants & variants, Arena * arena, bool final, Int32 bucket)
{
    if (!variants.isTwoLevel())
        return {};
    if (bucket >= getBucketsNum(variants))
        return {};
    return aggregator.convertOneBucketToBlock(variants, arena, final, bucket);
}

template <typename Method>
static void releaseOneBucket(Method & method, Int32 bucket)
{
    method.data.impls[bucket].clearAndShrink();
}

void GlutenAggregatorUtil::safeReleaseOneBucket(AggregatedDataVariants & variants, Int32 bucket)
{
    if (!variants.isTwoLevel())
        return;
    if (bucket >= getBucketsNum(variants))
        return;
#define M(NAME) else if (variants.type == AggregatedDataVariants::Type::NAME) releaseOneBucket(*variants.NAME, bucket);

    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant");
}

}

namespace local_engine
{
AggregateDataBlockConverter::AggregateDataBlockConverter(
    DB::Aggregator & aggregator_, DB::AggregatedDataVariantsPtr data_variants_, bool final_)
    : aggregator(aggregator_), data_variants(std::move(data_variants_)), final(final_)
{
    if (data_variants->isTwoLevel())
        buckets_num = DB::GlutenAggregatorUtil::getBucketsNum(*data_variants);
    else if (data_variants->size())
        buckets_num = 1;
    else
        buckets_num = 0;
}

bool AggregateDataBlockConverter::hasNext()
{
    while (current_bucket < buckets_num && output_blocks.empty())
    {
        if (data_variants->isTwoLevel())
        {
            Stopwatch watch;
            auto optional_block = DB::GlutenAggregatorUtil::safeConvertOneBucketToBlock(
                aggregator, *data_variants, data_variants->aggregates_pool, final, current_bucket);
            if (!optional_block)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bucket number {} for two-level aggregation", current_bucket);
            auto & block = *optional_block;
            LOG_DEBUG(
                &Poco::Logger::get("AggregateDataBlockConverter"),
                "Convert bucket {} into one block, rows: {}, cols: {}, bytes:{}, total bucket: {}, total rows: {}, time: {}",
                current_bucket,
                block.rows(),
                block.columns(),
                ReadableSize(block.allocatedBytes()),
                buckets_num,
                data_variants->size(),
                watch.elapsedMilliseconds());
            DB::GlutenAggregatorUtil::safeReleaseOneBucket(*data_variants, current_bucket);
            if (block.rows())
                output_blocks.emplace_back(std::move(block));
        }
        else
        {
            size_t keys = data_variants->size();
            auto blocks = aggregator.convertToBlocks(*data_variants, final);
            size_t total_allocated_bytes = 0;
            size_t total_bytes = 0;
            while (!blocks.empty())
            {
                if (blocks.front().rows())
                {
                    total_allocated_bytes += blocks.front().allocatedBytes();
                    total_bytes += blocks.front().bytes();
                    output_blocks.emplace_back(std::move(blocks.front()));
                }
                blocks.pop_front();
            }
            LOG_DEBUG(
                &Poco::Logger::get("AggregateDataBlockConverter"),
                "Convert single level hash table into blocks. blocks: {}, total bytes: {}, total allocated bytes: {}, total rows: {}",
                output_blocks.size(),
                ReadableSize(total_bytes),
                ReadableSize(total_allocated_bytes),
                keys);
            data_variants = nullptr;
        }
        ++current_bucket;
    }
    return !output_blocks.empty();
}

DB::Block AggregateDataBlockConverter::next()
{
    auto block = output_blocks.front();
    output_blocks.pop_front();
    return block;
}

DB::Aggregator::Params AggregatorParamsHelper::buildParams(
    const DB::ContextPtr & context,
    const DB::Names & grouping_keys,
    const DB::AggregateDescriptions & agg_descriptions,
    Mode mode,
    Algorithm algorithm)
{
    const auto & settings = context->getSettingsRef();
    size_t max_rows_to_group_by = mode == Mode::PARTIAL_TO_FINISHED ? 0 : static_cast<size_t>(settings[DB::Setting::max_rows_to_group_by]);

    size_t group_by_two_level_threshold
        = algorithm == Algorithm::GlutenGraceAggregate ? static_cast<size_t>(settings[DB::Setting::group_by_two_level_threshold]) : 0;
    size_t group_by_two_level_threshold_bytes = algorithm == Algorithm::GlutenGraceAggregate
        ? 0
        : (mode == Mode::PARTIAL_TO_FINISHED ? 0 : static_cast<size_t>(settings[DB::Setting::group_by_two_level_threshold_bytes]));
    double max_bytes_ratio_before_external_group_by = algorithm == Algorithm::GlutenGraceAggregate
        ? 0.0
        : (mode == Mode::PARTIAL_TO_FINISHED ? 0.0 : settings[DB::Setting::max_bytes_ratio_before_external_group_by]);
    size_t max_bytes_before_external_group_by = algorithm == Algorithm::GlutenGraceAggregate
        ? 0
        : (mode == Mode::PARTIAL_TO_FINISHED ? 0 : static_cast<size_t>(settings[DB::Setting::max_bytes_before_external_group_by]));
    bool empty_result_for_aggregation_by_empty_set = algorithm == Algorithm::GlutenGraceAggregate
        ? false
        : (mode == Mode::PARTIAL_TO_FINISHED ? false : static_cast<bool>(settings[DB::Setting::empty_result_for_aggregation_by_empty_set]));
    DB::TemporaryDataOnDiskScopePtr tmp_data_scope = algorithm == Algorithm::GlutenGraceAggregate ? nullptr : context->getTempDataOnDisk();

    size_t min_free_disk_space = algorithm == Algorithm::GlutenGraceAggregate
        ? 0
        : static_cast<size_t>(settings[DB::Setting::min_free_disk_space_for_temporary_data]);
    bool compile_aggregate_expressions = mode == Mode::PARTIAL_TO_FINISHED ? false : settings[DB::Setting::compile_aggregate_expressions];
    size_t min_count_to_compile_aggregate_expression
        = mode == Mode::PARTIAL_TO_FINISHED ? 0 : settings[DB::Setting::min_count_to_compile_aggregate_expression];
    size_t max_block_size = PODArrayUtil::adjustMemoryEfficientSize(settings[DB::Setting::max_block_size]);
    bool enable_prefetch = mode != Mode::PARTIAL_TO_FINISHED;
    bool only_merge = mode == Mode::PARTIAL_TO_FINISHED;
    bool optimize_group_by_constant_keys
        = mode == Mode::PARTIAL_TO_FINISHED ? false : settings[DB::Setting::optimize_group_by_constant_keys];

    DB::Settings aggregate_settings{settings};
    aggregate_settings[DB::Setting::max_rows_to_group_by] = max_rows_to_group_by;
    aggregate_settings[DB::Setting::max_bytes_ratio_before_external_group_by] = max_bytes_ratio_before_external_group_by;
    aggregate_settings[DB::Setting::max_bytes_before_external_group_by] = max_bytes_before_external_group_by;
    aggregate_settings[DB::Setting::min_free_disk_space_for_temporary_data] = min_free_disk_space;
    aggregate_settings[DB::Setting::compile_aggregate_expressions] = compile_aggregate_expressions;
    aggregate_settings[DB::Setting::min_count_to_compile_aggregate_expression] = min_count_to_compile_aggregate_expression;
    aggregate_settings[DB::Setting::max_block_size] = max_block_size;
    aggregate_settings[DB::Setting::enable_software_prefetch_in_aggregation] = enable_prefetch;
    aggregate_settings[DB::Setting::optimize_group_by_constant_keys] = optimize_group_by_constant_keys;
    return DB::Aggregator::Params{
        grouping_keys,
        agg_descriptions,
        /*overflow_row*/ false,
        aggregate_settings[DB::Setting::max_rows_to_group_by],
        aggregate_settings[DB::Setting::group_by_overflow_mode],
        group_by_two_level_threshold,
        group_by_two_level_threshold_bytes,
        DB::Aggregator::Params::getMaxBytesBeforeExternalGroupBy(
            aggregate_settings[DB::Setting::max_bytes_before_external_group_by],
            aggregate_settings[DB::Setting::max_bytes_ratio_before_external_group_by]),
        empty_result_for_aggregation_by_empty_set,
        tmp_data_scope,
        aggregate_settings[DB::Setting::max_threads],
        aggregate_settings[DB::Setting::min_free_disk_space_for_temporary_data],
        aggregate_settings[DB::Setting::compile_aggregate_expressions],
        aggregate_settings[DB::Setting::min_count_to_compile_aggregate_expression],
        aggregate_settings[DB::Setting::max_block_size],
        aggregate_settings[DB::Setting::enable_software_prefetch_in_aggregation],
        only_merge,
        aggregate_settings[DB::Setting::optimize_group_by_constant_keys],
        aggregate_settings[DB::Setting::min_hit_rate_to_use_consecutive_keys_optimization],
        {},
        settings[DB::Setting::enable_producing_buckets_out_of_order_in_aggregation],
        settings[DB::Setting::serialize_string_in_memory_with_zero_byte]};
}


#define COMPARE_FIELD(field) \
    if (lhs.field != rhs.field) \
    { \
        LOG_ERROR(getLogger("AggregatorParamsHelper"), "Aggregator::Params field " #field " is not equal. {}/{}", lhs.field, rhs.field); \
        return false; \
    }
bool AggregatorParamsHelper::compare(const DB::Aggregator::Params & lhs, const DB::Aggregator::Params & rhs)
{
    COMPARE_FIELD(overflow_row);
    COMPARE_FIELD(max_rows_to_group_by);
    COMPARE_FIELD(group_by_overflow_mode);
    COMPARE_FIELD(group_by_two_level_threshold);
    COMPARE_FIELD(group_by_two_level_threshold_bytes);
    COMPARE_FIELD(max_bytes_before_external_group_by);
    COMPARE_FIELD(empty_result_for_aggregation_by_empty_set);
    COMPARE_FIELD(max_threads);
    COMPARE_FIELD(min_free_disk_space);
    COMPARE_FIELD(compile_aggregate_expressions);
    if ((lhs.tmp_data_scope == nullptr) != (rhs.tmp_data_scope == nullptr))
    {
        LOG_ERROR(getLogger("AggregatorParamsHelper"), "Aggregator::Params field tmp_data_scope is not equal.");
        return false;
    }
    COMPARE_FIELD(min_count_to_compile_aggregate_expression);
    COMPARE_FIELD(enable_prefetch);
    COMPARE_FIELD(only_merge);
    COMPARE_FIELD(optimize_group_by_constant_keys);
    COMPARE_FIELD(min_hit_rate_to_use_consecutive_keys_optimization);
    return true;
}
}

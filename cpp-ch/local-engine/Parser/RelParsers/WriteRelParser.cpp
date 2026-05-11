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

#include "WriteRelParser.h"

#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parser/TypeParser.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Storages/Output/NormalFileWriter.h>
#include <google/protobuf/wrappers.pb.h>
#include <substrait/algebra.pb.h>
#include <substrait/type.pb.h>
#include <write_optimization.pb.h>
#include <Common/GlutenSettings.h>

namespace DB::Setting
{
extern const SettingsUInt64 min_insert_block_size_rows;
extern const SettingsUInt64 min_insert_block_size_bytes;
}

using namespace local_engine;
using namespace DB;

namespace
{
DB::ProcessorPtr make_sink(
    const DB::ContextPtr & context,
    const DB::Names & partition_by,
    const DB::SharedHeader & input_header,
    const DB::SharedHeader & output_header,
    const std::string & base_path,
    const FileNameGenerator & generator,
    const std::string & format_hint,
    const std::shared_ptr<WriteStats> & stats)
{
    bool no_bucketed = !SparkPartitionedBaseSink::isBucketedWrite(*input_header);
    if (partition_by.empty() && no_bucketed)
    {
        return std::make_shared<SubstraitFileSink>(
            context, base_path, "", false, generator.generate(), format_hint, input_header, stats, DeltaStats{input_header->columns()});
    }

    return std::make_shared<SubstraitPartitionedFileSink>(
        context, partition_by, input_header, output_header, base_path, generator, format_hint, stats);
}

DB::ExpressionActionsPtr create_rename_action(const DB::Block & input, const DB::Block & output)
{
    ActionsDAG actions_dag{blockToRowType(input)};
    actions_dag.project(buildNamesWithAliases(input, output));
    return std::make_shared<DB::ExpressionActions>(std::move(actions_dag));
}

DB::ExpressionActionsPtr create_project_action(const DB::Block & input, const DB::Block & output, const DB::ContextPtr context)
{
    DB::ColumnsWithTypeAndName final_cols;
    std::ranges::transform(
        output,
        std::back_inserter(final_cols),
        [](const DB::ColumnWithTypeAndName & out_ocl)
        {
            const auto out_type = out_ocl.type;
            return DB::ColumnWithTypeAndName(out_type->createColumn(), out_type, out_ocl.name);
        });
    assert(final_cols.size() == output.columns());

    const auto & original_cols = input.getColumnsWithTypeAndName();
    ActionsDAG final_project = ActionsDAG::makeConvertingActions(original_cols, final_cols, ActionsDAG::MatchColumnsMode::Position, context);
    return std::make_shared<DB::ExpressionActions>(std::move(final_project));
}

void adjust_output(const DB::QueryPipelineBuilderPtr & builder, const DB::Block & output, const ContextPtr context)
{
    const auto input = builder->getHeader();
    if (input.columns() != output.columns())
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Mismatch result columns size, input size is {}, but output size is {}",
            input.columns(),
            output.columns());
    }
    DB::ExpressionActionsPtr convert_action;
    if (sameType(input, output))
    {
        if (!sameName(input, output))
            convert_action = create_rename_action(input, output); // name_is_different
    }
    else
        convert_action = create_project_action(input, output, context); // type_is_different

    if (!convert_action)
        return;

    builder->addSimpleTransform(
        [&](const DB::SharedHeader & cur_header, const DB::QueryPipelineBuilder::StreamType stream_type) -> DB::ProcessorPtr
        {
            if (stream_type != DB::QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return std::make_shared<DB::ConvertingTransform>(cur_header, convert_action);
        });
}

void addMergeTreeSinkTransform(
    const DB::ContextPtr & context,
    const DB::QueryPipelineBuilderPtr & builder,
    const MergeTreeTable & merge_tree_table,
    const DB::SharedHeader & header,
    const DB::Names & partition_by)
{
    Chain chain;
    //
    auto stats = MergeTreeStats::create(header, partition_by);
    chain.addSink(stats);
    //

    SparkMergeTreeWriteSettings write_settings{context};

    auto sink = partition_by.empty()
        ? SparkMergeTreeSink::create(merge_tree_table, write_settings, context->getQueryContext(), DeltaStats{header->columns()}, {stats})
        : std::make_shared<SparkMergeTreePartitionedFileSink>(header, partition_by, merge_tree_table, write_settings, context, stats);

    chain.addSource(sink);
    builder->addChain(std::move(chain));
}

void addNormalFileWriterSinkTransform(
    const DB::ContextPtr & context,
    const DB::QueryPipelineBuilderPtr & builder,
    const std::string & format_hint,
    const DB::SharedHeader & output,
    const DB::Names & partition_by)
{
    GlutenWriteSettings write_settings = GlutenWriteSettings::get(context);

    if (write_settings.task_write_tmp_dir.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Write Pipeline need inject temp directory.");

    if (write_settings.task_write_filename_pattern.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Write Pipeline need inject file pattern.");

    FileNameGenerator generator(write_settings.task_write_filename_pattern);

    auto stats = WriteStats::create(output, partition_by);

    builder->addSimpleTransform(
        [&](const SharedHeader & cur_header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return make_sink(context, partition_by, cur_header, output, write_settings.task_write_tmp_dir, generator, format_hint, stats);
        });
    builder->addSimpleTransform(
        [&](const SharedHeader &, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return stats;
        });
}
}

namespace local_engine
{


IMPLEMENT_GLUTEN_SETTINGS(GlutenWriteSettings, WRITE_RELATED_SETTINGS)

void addSinkTransform(const DB::ContextPtr & context, const substrait::WriteRel & write_rel, const DB::QueryPipelineBuilderPtr & builder)
{
    assert(write_rel.has_named_table());
    const substrait::NamedObjectWrite & named_table = write_rel.named_table();

    local_engine::Write write;
    if (!named_table.advanced_extension().optimization().UnpackTo(&write))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to unpack write optimization with local_engine::Write.");
    assert(write.has_common());
    const substrait::NamedStruct & table_schema = write_rel.table_schema();
    auto partition_indexes = write.common().partition_col_index();
    if (write.has_mergetree())
    {
        MergeTreeTable merge_tree_table(write, table_schema);
        auto output = toShared(TypeParser::buildBlockFromNamedStruct(table_schema, merge_tree_table.low_card_key));
        adjust_output(builder, *output, context);

        builder->addSimpleTransform(
            [&](const SharedHeader & in_header) -> ProcessorPtr { return std::make_shared<MaterializingTransform>(in_header, false); });

        const auto partition_by = collect_partition_cols(*output, table_schema, partition_indexes);

        GlutenWriteSettings write_settings = GlutenWriteSettings::get(context);
        if (write_settings.task_write_tmp_dir.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "MergeTree Write Pipeline need inject relative path.");
        if (!merge_tree_table.relative_path.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Non empty relative path for MergeTree table in pipeline mode.");

        merge_tree_table.relative_path = write_settings.task_write_tmp_dir;
        addMergeTreeSinkTransform(context, builder, merge_tree_table, output, partition_by);
    }
    else
    {
        auto output = toShared(TypeParser::buildBlockFromNamedStruct(table_schema));
        adjust_output(builder, *output, context);
        const auto partition_by = collect_partition_cols(*output, table_schema, partition_indexes);
        addNormalFileWriterSinkTransform(context, builder, write.common().format(), output, partition_by);
    }
}
DB::Names collect_partition_cols(const DB::Block & header, const substrait::NamedStruct & struct_, const PartitionIndexes & partition_by)
{
    if (partition_by.empty())
    {
        assert(std::ranges::all_of(
            struct_.column_types(), [](const int32_t type) { return type != ::substrait::NamedStruct::PARTITION_COL; }));
        return {};
    }
    assert(struct_.column_types_size() == header.columns());
    assert(struct_.column_types_size() == struct_.struct_().types_size());

    DB::Names result;
    result.reserve(partition_by.size());
    for (auto idx : partition_by)
    {
        assert(idx >= 0 && idx < header.columns());
        assert(struct_.column_types(idx) == ::substrait::NamedStruct::PARTITION_COL);
        result.emplace_back(header.getByPosition(idx).name);
    }
    return result;
}

}

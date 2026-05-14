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

#include "ReadRelParser.h"
#include <memory>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Operator/BlocksBufferPoolTransform.h>
#include <Parser/RelParsers/MergeTreeRelParser.h>
#include <Parser/RelParsers/StreamKafkaRelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/SourceFromJavaIter.h>
#include <Storages/SourceFromRange.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <google/protobuf/wrappers.pb.h>
#include <rapidjson/document.h>
#include <Common/BlockTypeUtils.h>
#include <Common/DebugUtils.h>

namespace DB
{
namespace Setting
{
extern const SettingsMaxThreads max_threads;
extern const SettingsUInt64 max_block_size;
}
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}
namespace local_engine
{
using namespace DB;
DB::QueryPlanPtr
ReadRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    if (query_plan)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Source node's input plan should be null");

    const auto & read = rel.read();
    if (isReadRelFromMergeTree(read))
    {
        substrait::ReadRel::ExtensionTable extension_table;
        if (read.has_extension_table())
            extension_table = read.extension_table();
        else
        {
            extension_table = BinaryToMessage<substrait::ReadRel::ExtensionTable>(split_info);
            debug::dumpMessage(extension_table, "extension_table");
        }

        MergeTreeRelParser merge_tree_parser(parser_context, getContext());
        query_plan = merge_tree_parser.parseReadRel(std::make_unique<DB::QueryPlan>(), read, extension_table);
        steps = merge_tree_parser.getSteps();
    }
    else if (isReadRelFromLocalFile(read) || isReadRelFromJavaIter(read) || isReadRelFromRange(read))
    {
        chassert(read.has_base_schema());
        DB::QueryPlanStepPtr read_step;

        if (isReadRelFromJavaIter(read))
            read_step = parseReadRelWithJavaIter(read);
        else if (isReadRelFromRange(read))
            read_step = parseReadRelWithRange(read);
        else if (isReadRelFromLocalFile(read))
            read_step = parseReadRelWithLocalFile(read);

        query_plan = std::make_unique<DB::QueryPlan>();
        steps.emplace_back(read_step.get());
        query_plan->addStep(std::move(read_step));

        if (getContext()->getSettingsRef()[Setting::max_threads] > 1)
        {
            auto buffer_step = std::make_unique<BlocksBufferPoolStep>(query_plan->getCurrentHeader());
            steps.emplace_back(buffer_step.get());
            query_plan->addStep(std::move(buffer_step));
        }
    }
    else if (isReadFromStreamKafka(read))
    {
        StreamKafkaRelParser kafka_parser(parser_context, getContext());
        kafka_parser.setSplitInfo(split_info);
        query_plan = kafka_parser.parse(std::make_unique<DB::QueryPlan>(), rel, rel_stack);
        steps = kafka_parser.getSteps();
    }
    else
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown read rel:{}", read.ShortDebugString());

    return query_plan;
}

bool ReadRelParser::isReadRelFromJavaIter(const substrait::ReadRel & rel)
{
    return rel.has_local_files() && rel.local_files().items().size() == 1
        && rel.local_files().items().at(0).uri_file().starts_with("iterator");
}

bool ReadRelParser::isReadRelFromLocalFile(const substrait::ReadRel & rel)
{
    if (rel.has_local_files())
        return !isReadRelFromJavaIter(rel);
    else
        return !rel.has_extension_table() && !isReadRelFromMergeTree(rel) && !isReadRelFromRange(rel) && !isReadFromStreamKafka(rel);
}

bool ReadRelParser::isReadRelFromMergeTree(const substrait::ReadRel & rel)
{
    if (!rel.has_advanced_extension())
        return false;

    google::protobuf::StringValue optimization;
    optimization.ParseFromString(rel.advanced_extension().optimization().value());
    ReadBufferFromString in(optimization.value());
    if (!checkString("isMergeTree=", in))
        return false;

    bool is_merge_tree = false;
    readBoolText(is_merge_tree, in);
    assertChar('\n', in);
    return is_merge_tree;
}

bool ReadRelParser::isReadRelFromRange(const substrait::ReadRel & rel)
{
    if (!rel.has_advanced_extension())
        return false;

    google::protobuf::StringValue optimization;
    optimization.ParseFromString(rel.advanced_extension().optimization().value());
    ReadBufferFromString in(optimization.value());
    if (!checkString("isRange=", in))
        return false;

    bool is_range = false;
    readBoolText(is_range, in);
    assertChar('\n', in);
    return is_range;
}

bool ReadRelParser::isReadFromStreamKafka(const substrait::ReadRel & rel)
{
    return rel.has_stream_kafka() && rel.stream_kafka();
}

DB::QueryPlanStepPtr ReadRelParser::parseReadRelWithJavaIter(const substrait::ReadRel & rel)
{
    GET_JNIENV(env)
    SCOPE_EXIT({CLEAN_JNIENV});
    auto first_block = SourceFromJavaIter::peekBlock(env, input_iter);

    /// Try to decide header from the first block read from Java iterator. Thus AggregateFunction with parameters has more precise types.
    auto header = first_block.has_value() ? first_block->cloneEmpty() : TypeParser::buildBlockFromNamedStruct(rel.base_schema());
    auto source = std::make_shared<SourceFromJavaIter>(
        getContext(), std::move(header), input_iter, is_input_iter_materialize, std::move(first_block));

    QueryPlanStepPtr source_step = std::make_unique<ReadFromPreparedSource>(Pipe(source));
    source_step->setStepDescription("Read From Java Iter");
    return source_step;
}

QueryPlanStepPtr ReadRelParser::parseReadRelWithLocalFile(const substrait::ReadRel & rel)
{
    auto header = TypeParser::buildBlockFromNamedStruct(rel.base_schema());

    auto format_settings = getFormatSettings(getContext());
    // TODO: check with the Delta DV
    bool readRowIndex = ParquetVirtualMeta::hasMetaColumns(header);
    bool onlyFlatType = onlyHasFlatType(header);

    substrait::ReadRel::LocalFiles local_files;
    if (rel.has_local_files())
        local_files = rel.local_files();
    else
    {
        local_files = BinaryToMessage<substrait::ReadRel::LocalFiles>(split_info);
        debug::dumpMessage(local_files, "local_files");
    }

    auto source = std::make_shared<SubstraitFileSource>(getContext(), header, local_files);
    auto source_pipe = Pipe(source);
    auto source_step = std::make_unique<SubstraitFileSourceStep>(getContext(), std::move(source_pipe), "substrait local files");
    if (format_settings.parquet.use_native_reader_v3 && !readRowIndex && onlyFlatType)
        source_step->setStepDescription("ParquetReaderV3");
    else
        source_step->setStepDescription("ParquetReader");

    if (rel.has_filter())
    {
        DB::ActionsDAG actions_dag{blockToRowType(header)};
        const DB::ActionsDAG::Node * filter_node = parseExpression(actions_dag, rel.filter());
        actions_dag.addOrReplaceInOutputs(*filter_node);
        assert(filter_node == &(actions_dag.findInOutputs(filter_node->result_name)));
        source_step->addFilter(std::move(actions_dag), filter_node->result_name);
    }
    return source_step;
}

QueryPlanStepPtr ReadRelParser::parseReadRelWithRange(const substrait::ReadRel & rel)
{
    substrait::ReadRel::ExtensionTable extension_table;
    if (rel.has_extension_table())
        extension_table = rel.extension_table();
    else
    {
        extension_table = BinaryToMessage<substrait::ReadRel::ExtensionTable>(split_info);
        debug::dumpMessage(extension_table, "extension_table");
    }

    chassert(extension_table.has_detail());
    std::string str_range_info = toString(extension_table.detail());
    // std::cout << "range_info:" << str_range_info << std::endl;

    rapidjson::Document document;
    document.Parse(str_range_info.c_str());
    if (!document.HasMember("start") || !document.HasMember("end") || !document.HasMember("step") || !document.HasMember("numSlices")
        || !document.HasMember("sliceIndex"))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Missing required fields in range info");

    Int64 start = document["start"].GetInt64();
    Int64 end = document["end"].GetInt64();
    Int64 step = document["step"].GetInt64();
    Int32 num_slices = document["numSlices"].GetInt();
    Int32 slice_index = document["sliceIndex"].GetInt();

    auto header = toShared(TypeParser::buildBlockFromNamedStruct(rel.base_schema()));
    size_t max_block_size = getContext()->getSettingsRef()[Setting::max_block_size];
    auto source = std::make_shared<SourceFromRange>(header, start, end, step, num_slices, slice_index, max_block_size);
    QueryPlanStepPtr source_step = std::make_unique<ReadFromPreparedSource>(Pipe(source));
    source_step->setStepDescription("Read From Range Exec");
    return source_step;
}

void registerReadRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_unique<ReadRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kRead, builder);
}
}

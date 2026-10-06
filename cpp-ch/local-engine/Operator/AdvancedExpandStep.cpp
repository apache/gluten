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

#include "AdvancedExpandStep.h"

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>
#include <Operator/StreamingAggregatingStep.h>
#include <Processors/ResizeProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/Logger.h>
#include <Common/AggregateUtil.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/logger_useful.h>

namespace local_engine
{

static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

AdvancedExpandStep::AdvancedExpandStep(
    DB::ContextPtr context_,
    const DB::SharedHeader & input_header_,
    size_t grouping_keys_,
    const DB::AggregateDescriptions & aggregate_descriptions_,
    const ExpandField & project_set_exprs_)
    : DB::ITransformingStep(input_header_, toShared(buildOutputHeader(*input_header_, project_set_exprs_)), getTraits())
    , context(context_)
    , grouping_keys(grouping_keys_)
    , aggregate_descriptions(aggregate_descriptions_)
    , project_set_exprs(project_set_exprs_)
{
}

DB::Block AdvancedExpandStep::buildOutputHeader(const DB::Block &, const ExpandField & project_set_exprs_)
{
    DB::ColumnsWithTypeAndName cols;
    const auto & types = project_set_exprs_.getTypes();
    const auto & names = project_set_exprs_.getNames();

    chassert(names.size() == types.size());

    for (size_t i = 0; i < project_set_exprs_.getExpandCols(); ++i)
        cols.emplace_back(DB::ColumnWithTypeAndName(types[i], names[i]));

    return DB::Block(std::move(cols));
}

void AdvancedExpandStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & pipeline_settings)
{
    const auto & settings = context->getSettingsRef();
    DB::Names aggregate_grouping_keys;
    for (size_t i = 0; i < output_header->columns(); ++i)
    {
        const auto & col = output_header->getByPosition(i);
        if (typeid_cast<const DB::ColumnAggregateFunction *>(col.column.get()))
            break;
        aggregate_grouping_keys.push_back(col.name);
    }
    // partial to partial aggregate
    auto params = AggregatorParamsHelper::buildParams(
        context, aggregate_grouping_keys, aggregate_descriptions, AggregatorParamsHelper::Mode::PARTIAL_TO_PARTIAL);

    auto input_header = input_headers.front();
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto expand_processor
                = std::make_shared<AdvancedExpandTransform>(input_header, *output_header, grouping_keys, project_set_exprs);
            DB::connect(*output, expand_processor->getInputs().front());
            new_processors.push_back(expand_processor);

            auto expand_output_header = expand_processor->getOutputs().front().getSharedHeader();

            auto transform_params = std::make_shared<DB::AggregatingTransformParams>(expand_output_header, params, false);
            auto aggregate_processor = std::make_shared<StreamingAggregatingTransform>(context, expand_output_header, transform_params);
            DB::connect(expand_processor->getOutputs().back(), aggregate_processor->getInputs().front());
            new_processors.push_back(aggregate_processor);
            auto aggregate_output_header = aggregate_processor->getOutputs().front().getHeader();

            auto resize_processor = std::make_shared<DB::ResizeProcessor>(expand_output_header, 2, 1);
            DB::connect(aggregate_processor->getOutputs().front(), resize_processor->getInputs().front());
            DB::connect(expand_processor->getOutputs().front(), resize_processor->getInputs().back());
            new_processors.push_back(resize_processor);
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
}

void AdvancedExpandStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void AdvancedExpandStep::updateOutputHeader()
{
    output_header = toShared(buildOutputHeader(*input_headers.front(), project_set_exprs));
}

/// It has two output ports. The 1st output port is for high cardinality data, the 2nd output port is for
/// low cardinality data.
AdvancedExpandTransform::AdvancedExpandTransform(
    const DB::SharedHeader & input_header_, const DB::Block & output_header_, size_t grouping_keys_, const ExpandField & project_set_exprs_)
    : DB::IProcessor({input_header_}, {output_header_, output_header_})
    , grouping_keys(grouping_keys_)
    , project_set_exprs(project_set_exprs_)
    , input_header(input_header_)
{
    for (size_t i = 0; i < project_set_exprs.getExpandRows(); ++i)
    {
        const auto & kinds = project_set_exprs.getKinds()[i];
        size_t n = 0;
        for (size_t k = 0; k < grouping_keys; ++k)
            if (kinds[k] == EXPAND_FIELD_KIND_SELECTION)
                n += 1;
        need_to_aggregate.push_back((n != grouping_keys));
    }

    for (auto & port : outputs)
        output_ports.push_back(&port);
}

DB::IProcessor::Status AdvancedExpandTransform::prepare()
{
    auto & input = inputs.front();

    if (isCancelled() || output_ports[0]->isFinished() || output_ports[1]->isFinished())
    {
        input.close();
        output_ports[0]->finish();
        output_ports[1]->finish();
        return Status::Finished;
    }

    if (has_output)
    {
        auto & output_port = *output_ports[need_to_aggregate[expand_expr_iterator - 1]];
        if (output_port.canPush())
        {
            output_blocks[need_to_aggregate[expand_expr_iterator - 1]] += 1;
            output_rows[need_to_aggregate[expand_expr_iterator - 1]] += output_chunk.getNumRows();
            output_port.push(std::move(output_chunk));
            has_output = false;
            auto status = expand_expr_iterator >= project_set_exprs.getExpandRows() ? Status::NeedData : Status::Ready;
            return status;
        }
        else
        {
            return Status::PortFull;
        }
    }

    if (!has_input)
    {
        if (input.isFinished())
        {
            LOG_DEBUG(
                getLogger("AdvancedExpandTransform"),
                "Input rows/blocks={}/{}. output rows/blocks=[{}/{}, {}/{}]",
                input_rows,
                input_blocks,
                output_rows[0],
                output_blocks[0],
                output_rows[1],
                output_blocks[1]);
            output_ports[0]->finish();
            output_ports[1]->finish();
            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;
        input_chunk = input.pull(true);
        has_input = true;
        expand_expr_iterator = 0;
        input_blocks += 1;
        input_rows += input_chunk.getNumRows();
    }

    return Status::Ready;
}

void AdvancedExpandTransform::work()
{
    expandInputChunk();
}

void AdvancedExpandTransform::expandInputChunk()
{
    const auto & input_columns = input_chunk.getColumns();
    const auto & types = project_set_exprs.getTypes();
    const auto & kinds = project_set_exprs.getKinds()[expand_expr_iterator];
    const auto & fields = project_set_exprs.getFields()[expand_expr_iterator];
    size_t rows = input_chunk.getNumRows();

    DB::Columns columns(types.size());
    for (size_t col_i = 0; col_i < types.size(); ++col_i)
    {
        const auto & type = types[col_i];
        const auto & kind = kinds[col_i];
        const auto & field = fields[col_i];

        if (kind == EXPAND_FIELD_KIND_SELECTION)
        {
            auto index = field.safeGet<Int32>();
            const auto & input_column = input_columns[index];

            DB::ColumnWithTypeAndName input_arg;
            input_arg.column = input_column;
            input_arg.type = input_header->getByPosition(index).type;
            /// input_column maybe non-Nullable
            columns[col_i] = DB::castColumn(input_arg, type)->convertToFullColumnIfConst();
        }
        else if (kind == EXPAND_FIELD_KIND_LITERAL)
        {
            /// Add const column with field value
            auto column = type->createColumnConst(rows, field)->convertToFullColumnIfConst();
            columns[col_i] = std::move(column);
        }
        else
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown ExpandFieldKind {}", magic_enum::enum_name(kind));
    }

    output_chunk = DB::Chunk(std::move(columns), rows);
    has_output = true;

    ++expand_expr_iterator;
    has_input = expand_expr_iterator < project_set_exprs.getExpandRows();
}
}

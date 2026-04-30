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
#include "DebugUtils.h"
#include <iostream>
#include <sstream>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/join.hpp>
#include <google/protobuf/json/json.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/PlanUtil.h>
#include <Common/QueryContext.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace pb_util = google::protobuf::util;

namespace debug
{
namespace Utils
{

/**
 * Return the number of half-widths in a given string. Note that a full width character
 * occupies two half widths.
 *
 * For a string consisting of 1 million characters, the execution of this method requires
 * about 50ms.
 */
static size_t stringHalfWidth(const std::string & str)
{
    //TODO: Implement this method
    return str.size();
}

/**
 * <p>Left pad a String with spaces (' ').</p>
 *
 * <p>The String is padded to the size of {@code size}.</p>
 *
 * <pre>
 * StringUtils.leftPad(null, *)   = null
 * StringUtils.leftPad("", 3)     = "   "
 * StringUtils.leftPad("bat", 3)  = "bat"
 * StringUtils.leftPad("bat", 5)  = "  bat"
 * StringUtils.leftPad("bat", 1)  = "bat"
 * StringUtils.leftPad("bat", -1) = "bat"
 * </pre>
 *
 * @param str  the String to pad out, may be null
 * @param size  the size to pad to
 * @return left padded String or original String if no padding is necessary,
 *  {@code null} if null String input
 */
static std::string leftPad(const std::string & str, int totalWidth)
{
    std::stringstream ss;
    ss << std::setw(totalWidth) << std::setfill(' ') << str;
    return ss.str();
}

/**
 * <p>Right pad a String with spaces (' ').</p>
 *
 * <p>The String is padded to the size of {@code size}.</p>
 *
 * <pre>
 * StringUtils.rightPad(null, *)   = null
 * StringUtils.rightPad("", 3)     = "   "
 * StringUtils.rightPad("bat", 3)  = "bat"
 * StringUtils.rightPad("bat", 5)  = "bat  "
 * StringUtils.rightPad("bat", 1)  = "bat"
 * StringUtils.rightPad("bat", -1) = "bat"
 * </pre>
 *
 * @param str  the String to pad out, may be null
 * @param totalWidth the size to pad to
 * @param padChar  the character to pad with
 * @param size  the size to pad to
 * @return right padded String or original String if no padding is necessary,
 *  {@code null} if null String input
 */
static std::string rightPad(const std::string & str, int totalWidth, char padChar = ' ')
{
    std::stringstream ss;
    ss << str << std::setw(totalWidth - str.size()) << std::setfill(padChar) << "";
    return ss.str();
}

static std::string truncate(const std::string & str, size_t width)
{
    if (str.size() <= width)
        return str;
    return str.substr(0, width - 3) + "...";
}

using NameAndColumn = std::pair<std::string, DB::ColumnPtr>;
using NameAndColumns = std::vector<NameAndColumn>;

template <typename T>
const T & toAggType(DB::ConstAggregateDataPtr data)
{
    return *reinterpret_cast<const T *>(data);
}

std::string get(const DB::ColumnAggregateFunction & agg, size_t row)
{
    auto funcName = agg.getAggregateFunction()->getName();

    if (funcName == "count")
    {
        DB::ConstAggregateDataPtr data = agg.getData()[row];
        return std::to_string(toAggType<DB::AggregateFunctionCountData>(data).count);
    }
    return "Nan";
}


static std::string toString(const DB::IColumn * const col, size_t row, size_t width)
{
    assert(col != nullptr);
    auto getDataType = [](const DB::IColumn * col)
    {
        if (const auto * column_nullable = DB::checkAndGetColumn<DB::ColumnNullable>(col))
            return column_nullable->getNestedColumn().getDataType();
        return col->getDataType();
    };
    DB::WhichDataType which(getDataType(col));
    if (which.isAggregateFunction())
        return get(static_cast<const DB::ColumnAggregateFunction &>(*col), row);

    if (col->isNullAt(row))
        return "null";

    std::string str = DB::fieldToString((*col)[row]);
    if (str.size() <= width)
        return str;
    return str.substr(0, width - 3) + "...";
}

/**
 * Get rows represented in Sequence by specific truncate and vertical requirement.
 *
 * @param block Columns to show
 * @param numRows Number of rows to return
 * @param truncate If set to more than 0, truncates strings to `truncate` characters and
 *                   all cells will be aligned right.
 */
static std::vector<std::vector<std::string>> getRows(const NameAndColumns & block, size_t numRows, size_t truncate)
{
    std::vector<std::vector<std::string>> results;
    results.reserve(numRows);
    results.emplace_back(std::vector<std::string>());
    auto & headRow = results.back();

    for (const auto & column : block)
    {
        const auto & name = column.first;
        headRow.emplace_back(debug::Utils::truncate(name, truncate));
    }

    for (size_t row = 0; row < numRows - 1; ++row)
    {
        results.emplace_back(std::vector<std::string>());
        auto & currentRow = results.back();
        currentRow.reserve(block.size());

        for (const auto & column : block)
            currentRow.emplace_back(toString(column.second.get(), row, truncate));
    }
    return results;
}

static std::string showString(const NameAndColumns & block, size_t numRows, size_t truncate, bool vertical)
{
    numRows = std::min(numRows, block[0].second->size());
    bool hasMoreData = block[0].second->size() > numRows;
    // Get rows represented by vector[vector[String]], we may get one more line if it has more data.
    std::vector<std::vector<std::string>> rows = getRows(block, numRows + 1, truncate);

    size_t numCols = block.size();
    // We set a minimum column width at '3'
    constexpr size_t minimumColWidth = 3;

    std::stringstream sb;

    if (!vertical)
    {
        // Initialise the width of each column to a minimum value
        std::vector<size_t> colWidths(numCols, minimumColWidth);

        // Compute the width of each column
        for (const auto & row : rows)
            for (size_t i = 0; i < row.size(); ++i)
                colWidths[i] = std::max(colWidths[i], stringHalfWidth(row[i]));

        std::vector<std::vector<std::string>> paddedRows;
        for (const auto & row : rows)
        {
            std::vector<std::string> paddedRow;
            for (size_t i = 0; i < row.size(); ++i)
                if (truncate > 0)
                    paddedRow.push_back(leftPad(row[i], colWidths[i] - stringHalfWidth(row[i]) + row[i].size()));
                else
                    paddedRow.push_back(rightPad(row[i], colWidths[i] - stringHalfWidth(row[i]) + row[i].size()));
            paddedRows.push_back(paddedRow);
        }

        // Create SeparateLine
        std::stringstream sep;
        for (int width : colWidths)
            sep << "+" << std::string(width, '-');
        sep << "+\n";

        // column names
        sb << sep.str();
        for (const auto & cell : paddedRows[0])
            sb << "|" << cell;
        sb << "|\n" << sep.str();

        // data
        for (size_t i = 1; i < paddedRows.size(); ++i)
        {
            for (const auto & cell : paddedRows[i])
                sb << "|" << cell;
            sb << "|\n";
        }
        sb << sep.str();
    }
    else
    {
        // Extended display mode enabled
        const std::vector<std::string> & fieldNames = rows[0];
        auto dataRowsBegin = [&]() { return rows.begin() + 1; };

        // Compute the width of field name and data columns
        size_t fieldNameColWidth = minimumColWidth;
        for (const auto & fieldName : fieldNames)
            fieldNameColWidth = std::max(fieldNameColWidth, Utils::stringHalfWidth(fieldName));

        size_t dataColWidth = minimumColWidth;


        for (auto dataRowIter = dataRowsBegin(); dataRowIter != rows.end(); ++dataRowIter)
        {
            const auto & row = *dataRowIter;
            size_t maxWidth = 0;
            for (const auto & cell : row)
                maxWidth = std::max(maxWidth, stringHalfWidth(cell));
            dataColWidth = std::max(dataColWidth, maxWidth);
        }

        //
        for (auto dataRowIter = dataRowsBegin(); dataRowIter != rows.end(); ++dataRowIter)
        {
            // create row header
            std::string rowHeader = "-RECORD " + std::to_string(rows.end() - dataRowIter);
            rowHeader = rightPad(rowHeader, fieldNameColWidth + dataColWidth + 5, '-');
            sb << rowHeader << "\n";

            // process each cell in the row
            const auto & row = *dataRowIter;
            for (size_t j = 0; j < row.size(); j++)
            {
                const std::string & cell = row[j];
                const std::string & fieldName = fieldNames[j];
                std::string paddedFieldName = rightPad(fieldName, fieldNameColWidth - stringHalfWidth(fieldName) + fieldName.length());
                std::string paddedData = rightPad(cell, dataColWidth - stringHalfWidth(cell) + cell.length());
                sb << " " << paddedFieldName << " | " << paddedData << " \n";
            }
            sb << "\n";
        }
    }

    // Print a footer
    if (vertical && block[0].second->empty())
    {
        // In a vertical mode, print an empty row set explicitly
        sb << "(0 rows)" << std::endl;
    }
    else if (hasMoreData)
    {
        // For Data that has more than "numRows" records
        const char * rowsString = (numRows == 1) ? "row" : "rows";
        sb << "only showing top " << numRows << " " << rowsString << std::endl;
    }
    return sb.str();
}
} // namespace Utils


///

void dumpMemoryUsage(const char * type)
{
    auto logger = getLogger("QueryContextManager");
    if (!logger)
        return;
    auto task_id = local_engine::QueryContext::instance().currentTaskIdOrEmpty();
    task_id = task_id.empty() ? "" : "(" + task_id + ")";
    auto usage = local_engine::currentThreadGroupMemoryUsage();
    LOG_ERROR(logger, "{}{} Memory Usage {}", type, task_id, formatReadableSizeWithBinarySuffix(usage));
}

void dumpPlan(DB::QueryPlan & plan, const char * type, bool force, LoggerPtr logger)
{
    if (!logger)
    {
        logger = getLogger("SerializedPlanParser");
        if (!logger)
            return;
    }

    if (!force && !logger->debug())
        return;

    auto out = local_engine::PlanUtil::explainPlan(plan);
    auto task_id = local_engine::QueryContext::instance().currentTaskIdOrEmpty();
    task_id = task_id.empty() ? "" : "(" + task_id + ")";
    if (force) // force
        LOG_ERROR(logger, "{}{} =>\n{}", type, task_id, out);
    else
        LOG_DEBUG(logger, "{}{} =>\n{}", type, task_id, out);
}

void dumpMessage(const google::protobuf::Message & message, const char * type, bool force, LoggerPtr logger)
{
    if (!logger)
    {
        logger = getLogger("SubstraitPlan");
        if (!logger)
            return;
    }

    if (!force && !logger->debug())
        return;
    pb_util::JsonPrintOptions options;
    std::string json;
    if (auto s = MessageToJsonString(message, &json, options); !s.ok())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Can not convert {} to Json", type);

    auto task_id = local_engine::QueryContext::instance().currentTaskIdOrEmpty();
    task_id = task_id.empty() ? "" : "(" + task_id + ")";
    if (force) // force
        LOG_ERROR(logger, "{}{} =>\n{}", type, task_id, json);
    else
        LOG_DEBUG(logger, "{}{} =>\n{}", type, task_id, json);
}

void headBlock(const DB::Block & block, size_t count)
{
    std::cerr << showString(block, count) << std::endl;
}

void printBlockHeader(const DB::Block & block, const std::string & prefix)
{
    auto nameColumn = local_engine::STRING()->createColumn();
    auto typeColumn = local_engine::STRING()->createColumn();

    for (const auto & column : block.getColumnsWithTypeAndName())
    {
        nameColumn->insert(column.name);
        typeColumn->insert(column.type->getName());
    }

    if (!prefix.empty())
        std::cerr << prefix << std::endl;

    std::cerr << Utils::showString({{"[Name]", nameColumn->getPtr()}, {"[type]", typeColumn->getPtr()}}, nameColumn->size(), 100, false)
              << std::endl;
}

void headColumn(const DB::ColumnPtr & column, size_t count)
{
    std::cerr << Utils::showString({{"Column", column}}, count, 20, false) << std::endl;
}

/**
 * Compose the string representing rows for output
 *
 * @param block Block to show
 * @param numRows Number of rows to show
 * @param truncate If set to more than 0, truncates strings to `truncate` characters and
 *                   all cells will be aligned right.
 * @param vertical If set to true, prints output rows vertically (one line per column value).
 */

std::string showString(const DB::Block & block, size_t numRows, size_t truncate, bool vertical)
{
    std::vector<DB::ColumnWithTypeAndName> columns = block.getColumnsWithTypeAndName();
    Utils::NameAndColumns name_and_columns;
    name_and_columns.reserve(columns.size());
    std::ranges::transform(
        columns,
        std::back_inserter(name_and_columns),
        [](const DB::ColumnWithTypeAndName & col) { return std::make_pair(col.name, col.column); });
    return Utils::showString(name_and_columns, numRows, truncate, vertical);
}

std::string showString(const DB::ColumnPtr & column, size_t numRows, size_t truncate, bool vertical)
{
    return Utils::showString({{"Column", column}}, numRows, truncate, vertical);
}

std::string dumpColumn(const std::string & name, const DB::ColumnPtr & column)
{
    //TODO: ColumnSet

    if (isColumnConst(*column))
        return DB::fieldToString(assert_cast<const DB::ColumnConst &>(*column).getField());

    size_t size = std::min(static_cast<size_t>(10), column->size());
    std::vector<std::string> results;
    results.reserve(size);
    for (int row = 0; row < size; ++row)
        results.push_back(Utils::toString(column.get(), row, 20));
    return fmt::format("{}:[{}]", name, boost::algorithm::join(results, ", "));
}

std::string dumpActionsDAG(const DB::ActionsDAG & dag)
{
    std::stringstream ss;
    ss << "digraph ActionsDAG {\n";
    ss << "  rankdir=BT;\n"; // Invert the vertical direction
    ss << "  nodesep=0.1;\n"; // Reduce space between nodes
    ss << "  ranksep=0.1;\n"; // Reduce space between ranks
    ss << "  margin=0.1;\n"; // Reduce graph margin

    std::unordered_map<const DB::ActionsDAG::Node *, size_t> node_to_id;
    size_t id = 0;
    for (const auto & node : dag.getNodes())
        node_to_id[&node] = id++;

    std::unordered_set<const DB::ActionsDAG::Node *> output_nodes(dag.getOutputs().begin(), dag.getOutputs().end());

    for (const auto & node : dag.getNodes())
    {
        ss << "  n" << node_to_id[&node] << " [label=\"";
        ss << "id:" << node_to_id[&node] << "\\l";
        switch (node.type)
        {
            case DB::ActionsDAG::ActionType::COLUMN:
                ss << "Literal = " << (node.column ? dumpColumn(node.result_name, node.column) : "null") << "\\l";
                break;
            case DB::ActionsDAG::ActionType::ALIAS:
                ss << "alias" << "\\l";
                break;
            case DB::ActionsDAG::ActionType::FUNCTION:
                ss << "function: " << (node.function_base ? node.function_base->getName() : "null");
                if (node.is_function_compiled)
                    ss << " [compiled]";
                ss << "\\l";
                break;
            case DB::ActionsDAG::ActionType::ARRAY_JOIN:
                ss << "array join" << "\\l";
                break;
            case DB::ActionsDAG::ActionType::INPUT:
                ss << "Input Column:" << node.result_name << "\\l";
                break;
        }

        ss << "result type: " << (node.result_type ? node.result_type->getName() : "null") << "\\l";

        ss << "children:";
        for (const auto * child : node.children)
            ss << " " << node_to_id[child];
        ss << "\\l";

        ss << "\"";
        if (output_nodes.contains(&node))
            ss << ", shape=doublecircle";

        ss << "];\n";
    }

    for (const auto & node : dag.getNodes())
        for (const auto * child : node.children)
            ss << "  n" << node_to_id[child] << " -> n" << node_to_id[&node] << ";\n";

    ss << "}\n";
    return ss.str();
}

}
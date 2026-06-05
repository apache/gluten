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
#include "SparkPartitionEscape.h"
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <sstream>
#include <iomanip>
#include <string>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

const std::vector<char> SparkPartitionEscape::ESCAPE_CHAR_LIST = {
    '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
    '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
    '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
    '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
    '{', '[', ']', '^'
};

const std::bitset<128> SparkPartitionEscape::ESCAPE_BITSET = []()
{
    std::bitset<128> bitset;
    for (char c : SparkPartitionEscape::ESCAPE_CHAR_LIST)
    {
        bitset.set(c);
    }
#ifdef _WIN32
    bitset.set(' ');
    bitset.set('<');
    bitset.set('>');
    bitset.set('|');
#endif
    return bitset;
}();

static bool needsEscaping(char c) {
    return c >= 0 && c < SparkPartitionEscape::ESCAPE_BITSET.size()
        && SparkPartitionEscape::ESCAPE_BITSET.test(c);
}

static std::string escapePathName(const std::string & path) {
    std::ostringstream builder;
    for (char c : path) {
        if (needsEscaping(c)) {
            builder << '%' << std::uppercase << std::setw(2) << std::setfill('0') << std::hex << (int)c;
        } else {
            builder << c;
        }
    }

    return builder.str();
}

DB::DataTypePtr SparkPartitionEscape::getReturnTypeImpl(const DB::DataTypes & arguments) const
{
    if (arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} argument size must be 1", name);
    
    if (!isString(arguments[0]))
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must be String", getName());

    return std::make_shared<DataTypeString>();
}

DB::ColumnPtr SparkPartitionEscape::executeImpl(
   const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const
{
   auto result = result_type->createColumn();
   result->reserve(input_rows_count);

   for (size_t i = 0; i < input_rows_count; ++i)
   {
       const auto escaped_name_data = arguments[0].column->getDataAt(i);
       auto escaped_name = escapePathName(std::string(escaped_name_data.data(), escaped_name_data.size()));
       result->insertData(escaped_name.c_str(), escaped_name.size());
   }
   return result;
}

REGISTER_FUNCTION(SparkPartitionEscape)
{
   factory.registerFunction<SparkPartitionEscape>();
}
}

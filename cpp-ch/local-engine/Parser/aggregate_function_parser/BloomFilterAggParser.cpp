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
#include <cmath>
#include <string>
#include <Interpreters/ActionsDAG.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/aggregate_function_parser/BloomFilterAggParser.h>
#include "substrait/algebra.pb.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
using namespace DB;
// This is copied from Spark, org.apache.spark.util.sketch.BloomFilter#optimalNumOfHashFunctions,
// which in return learned from https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions.
Int64 optimalNumOfHashFunctions(Int64 n, Int64 m)
{
    // (m / n) * log(2), but avoid truncation due to division!
    return std::max(1, static_cast<int>(std::round(static_cast<double>(m) / n * std::log(2))));
}

DB::Array get_parameters(Int64 insert_num, Int64 bits_num)
{
    DB::Array parameters;
    Int64 hash_num = optimalNumOfHashFunctions(insert_num, bits_num);
    parameters.push_back(Field((bits_num + 7) / 8));
    parameters.push_back(Field(hash_num));
    parameters.push_back(Field(0)); // Using 0 as seed.
    return parameters;
}

DB::Array AggregateFunctionParserBloomFilterAgg::parseFunctionParameters(
    const CommonFunctionInfo & func_info, DB::ActionsDAG::NodeRawConstPtrs & arg_nodes, DB::ActionsDAG & /*actions_dag*/) const
{
    if (func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE || func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT)
    {
        auto get_parameter_field = [](const DB::ActionsDAG::Node * node, size_t /*parameter_index*/) -> DB::Field
        {
            Field ret;
            node->column->get(0, ret);
            return ret;
        };
        Int64 insert_num = get_parameter_field(arg_nodes[1], 1).safeGet<Int64>();
        Int64 bits_num = get_parameter_field(arg_nodes[2], 2).safeGet<Int64>();

        // Delete all args except the first arg.
        arg_nodes.resize(1);

        return get_parameters(insert_num, bits_num);
    }
    else
    {
        return getDefaultFunctionParameters();
    }
}

static const AggregateFunctionParserRegister<AggregateFunctionParserBloomFilterAgg> register_bloom_filter_agg;
}

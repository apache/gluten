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
#include <incbin.h>
#include <testConfig.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parser/LocalExecutor.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <gtest/gtest.h>
#include <tests/utils/gluten_test_util.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

using namespace local_engine;
using namespace DB;


INCBIN(_readcsv_plan, SOURCE_DIR "/utils/extern-local-engine/tests/json/read_student_option_schema.csv.json");
TEST(LocalExecutor, ReadCSV)
{
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"56","text":{"fieldDelimiter":",","maxBlockSize":"8192","headerLinesToSkip":"1"},"schema":{"names":["id","name","language"],"struct":{"types":[{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}}]}},"metadataColumns":[{}]}]})";
    const std::string split = replaceLocalFilesWildcards(
        split_template, GLUTEN_SOURCE_URI("/backends-velox/src/test/resources/datasource/csv/student_option_schema.csv"));
    auto plan = local_engine::JsonStringToMessage<substrait::Plan>(EMBEDDED_PLAN(_readcsv_plan));
    auto parser_context = ParserContext::build(QueryContext::globalContext(), plan);
    SerializedPlanParser parser(parser_context);
    parser.addSplitInfo(local_engine::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));

    auto query_plan = parser.parse(plan);
    auto pipeline = parser.buildQueryPipeline(*query_plan);
    LocalExecutor local_executor{std::move(query_plan), std::move(pipeline)};

    EXPECT_TRUE(local_executor.hasNext());
    const Block & x = *local_executor.nextColumnar();
    EXPECT_EQ(4, x.rows());
}

size_t count(const substrait::Type_Struct & type)
{
    size_t ret = 0;
    for (const auto & t : type.types())
        if (t.has_struct_())
            ret += 1 + count(t.struct_());
        else
            ret++;
    return ret;
}

TEST(TypeParser, SchemaTest)
{
    const std::string scheam_str = R"({
  "names": [
    "count#16#Partial#count",
    "anonymousfield0"
  ],
  "struct": {
    "types": [
      {
        "struct": {
          "types": [
            {
              "i64": {
                "nullability": "NULLABILITY_REQUIRED"
              }
            }
          ],
          "nullability": "NULLABILITY_REQUIRED",
          "names": [
            "anonymousField0"
          ]
        }
      }
    ]
  }
})";

    const auto schema = local_engine::JsonStringToMessage<substrait::NamedStruct>(scheam_str);
    EXPECT_EQ(schema.names_size(), count(schema.struct_()));
    const auto block = TypeParser::buildBlockFromNamedStruct(schema);
    EXPECT_EQ(1, block.columns());
    debug::headBlock(block);
}

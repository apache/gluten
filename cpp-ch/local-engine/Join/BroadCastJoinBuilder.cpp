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
#include "BroadCastJoinBuilder.h"

#include <Compression/CompressedReadBuffer.h>
#include <Interpreters/TableJoin.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parser/RelParsers/JoinRelParser.h>
#include <Parser/TypeParser.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Shuffle/ShuffleReader.h>
#include <jni/SharedPointerWrapper.h>
#include <jni/jni_common.h>
#include <Poco/StringTokenizer.h>
#include <Common/CHUtil.h>
#include <Common/JNIUtils.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
namespace BroadCastJoinBuilder
{
using namespace DB;
static jclass Java_CHBroadcastBuildSideCache = nullptr;
static jmethodID Java_get = nullptr;
jlong callJavaGet(const std::string & id)
{
    GET_JNIENV(env)
    const jstring s = charTojstring(env, id.c_str());
    const auto result = safeCallStaticLongMethod(env, Java_CHBroadcastBuildSideCache, Java_get, s);
    CLEAN_JNIENV

    return result;
}

DB::Block resetBuildTableBlockName(Block & block, bool only_one = false)
{
    DB::ColumnsWithTypeAndName new_cols;
    std::set<std::string> names;
    int32_t seq = 0;
    for (const auto & col : block)
    {
      // Add a prefix to avoid column name conflicts with left table.
      std::stringstream new_name;
      // add a sequence to avoid duplicate name in some rare cases
      if (names.find(col.name) == names.end())
      {
         new_name << BlockUtil::RIGHT_COLUMN_PREFIX << col.name;
         names.insert(col.name);
      }
      else
      {
        new_name << BlockUtil::RIGHT_COLUMN_PREFIX  << (seq++) << "_" << col.name;
      }
      new_cols.emplace_back(col.column, col.type, new_name.str());

      if (only_one)
        break;
    }
    return DB::Block(new_cols);
}

void cleanBuildHashTable(const std::string & hash_table_id, jlong instance)
{
    auto clean_join = [&]
    {
        SharedPointerWrapper<StorageJoinFromReadBuffer>::dispose(instance);
    };
    /// Record memory usage in Total Memory Tracker
    ThreadFromGlobalPoolNoTracingContextPropagation thread(clean_join);
    thread.join();
    LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Broadcast hash table {} is cleaned", hash_table_id);
}

std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & key)
{
    const jlong result = callJavaGet(key);

    if (unlikely(result == 0))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found in cache.", key);

    auto wrapper = SharedPointerWrapper<StorageJoinFromReadBuffer>::sharedPtr(result);
    if (unlikely(!wrapper))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found, cache value is invalidated.", key);

    return wrapper;
}

// A join in cross rel.
static bool isCrossRelJoin(const std::string & key)
{
    return key.starts_with("BuiltBNLJBroadcastTable-");
}

static void collectBlocksForCountingRows(NativeReader & block_stream, Block & header, Blocks & result)
{
    ProfileInfo profile;
    Block block = block_stream.read();
    while (!block.empty())
    {
        const auto & col = block.getByPosition(0);
        auto counting_col = BlockUtil::buildRowCountBlock(col.column->size()).getColumnsWithTypeAndName()[0];
        DB::ColumnsWithTypeAndName columns;
        columns.emplace_back(counting_col.column->convertToFullColumnIfConst(), counting_col.type, counting_col.name);
        DB::Block new_block(columns);
        profile.update(new_block);
        result.emplace_back(std::move(new_block));
        block = block_stream.read();
    }
    header = BlockUtil::buildRowCountHeader();
}

static void collectBlocksForJoinRel(NativeReader & reader, Block & header, Blocks & result)
{
    ProfileInfo profile;
    Block block = reader.read();
    while (!block.empty())
    {
        DB::ColumnsWithTypeAndName columns;
        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & column = block.getByPosition(i);
            columns.emplace_back(BlockUtil::convertColumnAsNecessary(column, header.getByPosition(i)));
        }

        DB::Block final_block(columns);
        profile.update(final_block);
        result.emplace_back(std::move(final_block));

        block = reader.read();
    }
}

std::shared_ptr<StorageJoinFromReadBuffer> buildJoin(
    const std::string & key,
    DB::ReadBuffer & input,
    jlong row_count,
    const std::string & join_keys,
    jint join_type,
    bool has_mixed_join_condition,
    bool is_existence_join,
    const std::string & named_struct,
    bool is_null_aware_anti_join,
    bool has_null_key_values)
{
    auto join_key_list = Poco::StringTokenizer(join_keys, ",");
    Names key_names;
    for (const auto & key_name : join_key_list)
        key_names.emplace_back(BlockUtil::RIGHT_COLUMN_PREFIX + key_name);

    DB::JoinKind kind;
    DB::JoinStrictness strictness;
    bool is_cross_rel_join = isCrossRelJoin(key);
    if (is_cross_rel_join) assert(key_names.empty()); // cross rel join should not have join keys

    if (is_cross_rel_join)
        std::tie(kind, strictness) = JoinUtil::getCrossJoinKindAndStrictness(static_cast<substrait::CrossRel_JoinType>(join_type));
    else
        std::tie(kind, strictness) = JoinUtil::getJoinKindAndStrictness(static_cast<substrait::JoinRel_JoinType>(join_type), is_existence_join);


    substrait::NamedStruct substrait_struct;
    substrait_struct.ParseFromString(named_struct);
    Block header = TypeParser::buildBlockFromNamedStruct(substrait_struct);
    header = resetBuildTableBlockName(header);

    bool only_one_column = header.getNamesAndTypesList().empty();
    if (only_one_column)
        header = BlockUtil::buildRowCountBlock(0).getColumnsWithTypeAndName();

    Blocks data;
    auto collect_data = [&]()
    {
        NativeReader block_stream(input);
        if (only_one_column)
            collectBlocksForCountingRows(block_stream, header, data);
        else
            collectBlocksForJoinRel(block_stream, header, data);

        // For not cross join, we need to add a constant join key column
        // to make it behavior like a normal join.
        if (is_cross_rel_join && kind != JoinKind::Cross)
        {
            auto data_type_u8 = std::make_shared<DataTypeUInt8>();
            UInt8 const_key_val = 0;
            String const_key_name = JoinUtil::CROSS_REL_RIGHT_CONST_KEY_COLUMN;
            Blocks new_data;
            for (const auto & block : data)
            {
                auto cols = block.getColumnsWithTypeAndName();
                cols.emplace_back(data_type_u8->createColumnConst(block.rows(), const_key_val), data_type_u8, const_key_name);
                new_data.emplace_back(Block(cols));
            }
            data.swap(new_data);
            key_names.emplace_back(const_key_name);
            auto cols = header.getColumnsWithTypeAndName();
            cols.emplace_back(data_type_u8->createColumnConst(0, const_key_val), data_type_u8, const_key_name);
            header = Block(cols);
        }
    };

    /// Record memory usage in Total Memory Tracker
    ThreadFromGlobalPoolNoTracingContextPropagation thread(collect_data);
    thread.join();

    ColumnsDescription columns_description(header.getNamesAndTypesList());

    return make_shared<StorageJoinFromReadBuffer>(
        data,
        row_count,
        key_names,
        true,
        kind,
        strictness,
        has_mixed_join_condition,
        columns_description,
        ConstraintsDescription(),
        key,
        true,
        is_null_aware_anti_join,
        has_null_key_values);
}

void init(JNIEnv * env)
{
    /**
     * Scala object will be compiled into two classes, one is with '$' suffix which is normal class,
     * and one is utility class which only has static method.
     *
     * Here, we use utility class.
     */

    const char * classSig = "Lorg/apache/gluten/execution/CHBroadcastBuildSideCache;";
    Java_CHBroadcastBuildSideCache = CreateGlobalClassReference(env, classSig);
    Java_get = GetStaticMethodID(env, Java_CHBroadcastBuildSideCache, "get", "(Ljava/lang/String;)J");
}

void destroy(JNIEnv * env)
{
    env->DeleteGlobalRef(Java_CHBroadcastBuildSideCache);
}

}
}

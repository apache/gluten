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
#include "SparkMergeTreeWriter.h"

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <QueryPipeline/Chain.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <jni/jni_common.h>
#include <rapidjson/prettywriter.h>
#include <Poco/StringTokenizer.h>
#include <Common/JNIUtils.h>

using namespace DB;
namespace
{
Block removeColumnSuffix(const Block & block)
{
    ColumnsWithTypeAndName columns;
    for (int i = 0; i < block.columns(); ++i)
    {
        auto name = block.getByPosition(i).name;
        Poco::StringTokenizer splits(name, "#");
        auto column = block.getByPosition(i);
        column.name = splits[0];
        columns.emplace_back(column);
    }
    return Block(columns);
}

}

namespace local_engine
{

namespace SparkMergeTreeWriterJNI
{
static jclass Java_MergeTreeCommiterHelper = nullptr;
static jmethodID Java_set = nullptr;
void init(JNIEnv * env)
{
    const char * classSig = "Lorg/apache/spark/sql/execution/datasources/v1/clickhouse/MergeTreeCommiterHelper;";
    Java_MergeTreeCommiterHelper = CreateGlobalClassReference(env, classSig);
    assert(Java_MergeTreeCommiterHelper != nullptr);

    const char * methodName = "setCurrentTaskWriteInfo";
    const char * methodSig = "(Ljava/lang/String;Ljava/lang/String;)V";
    Java_set = GetStaticMethodID(env, Java_MergeTreeCommiterHelper, methodName, methodSig);
}
void destroy(JNIEnv * env)
{
    env->DeleteGlobalRef(Java_MergeTreeCommiterHelper);
}

void setCurrentTaskWriteInfo(const std::string & jobTaskTempID, const std::string & commitInfos)
{
    GET_JNIENV(env)
    const jstring Java_jobTaskTempID = charTojstring(env, jobTaskTempID.c_str());
    const jstring Java_commitInfos = charTojstring(env, commitInfos.c_str());
    safeCallVoidMethod(env, Java_MergeTreeCommiterHelper, Java_set, Java_jobTaskTempID, Java_commitInfos);
    CLEAN_JNIENV
}
}

std::string PartInfo::toJson(const std::vector<PartInfo> & part_infos)
{
    rapidjson::StringBuffer result;
    rapidjson::Writer<rapidjson::StringBuffer> writer(result);
    writer.StartArray();
    for (const auto & item : part_infos)
    {
        writer.StartObject();
        writer.Key("part_name");
        writer.String(item.part_name.c_str());
        writer.Key("mark_count");
        writer.Uint(item.mark_count);
        writer.Key("disk_size");
        writer.Uint(item.disk_size);
        writer.Key("row_count");
        writer.Uint(item.row_count);
        writer.Key("bucket_id");
        writer.String(item.bucket_id.c_str());
        writer.Key("partition_values");
        writer.String(item.partition_values.c_str());
        writer.EndObject();
    }
    writer.EndArray();
    return result.GetString();
}

std::unique_ptr<SparkMergeTreeWriter> SparkMergeTreeWriter::create(
    const MergeTreeTable & merge_tree_table, const DB::ContextMutablePtr & context, const std::string & spark_job_id)
{
    const DB::Settings & settings = context->getSettingsRef();
    const auto dest_storage = merge_tree_table.getStorage(context);
    StorageMetadataPtr metadata_snapshot = dest_storage->getInMemoryMetadataPtr();
    Block header = metadata_snapshot->getSampleBlock();
    ASTPtr none;
    Chain chain;
    auto sink = dest_storage->write(none, metadata_snapshot, context, false);
    chain.addSink(sink);
    const SinkHelper & sink_helper = assert_cast<const SparkMergeTreeSink &>(*sink).sinkHelper();
    //
    // auto stats = std::make_shared<MergeTreeStats>(header, sink_helper);
    // chain.addSink(stats);
    return std::make_unique<SparkMergeTreeWriter>(header, sink_helper, QueryPipeline{std::move(chain)}, spark_job_id, context);
}

SparkMergeTreeWriter::SparkMergeTreeWriter(
    const DB::Block & header_, const SinkHelper & sink_helper_, DB::QueryPipeline && pipeline_, const std::string & spark_job_id_, const DB::ContextPtr & context_)
    : header{header_}, sink_helper{sink_helper_}, pipeline{std::move(pipeline_)}, executor{pipeline}, spark_job_id(spark_job_id_), context(context_)
{
}

void SparkMergeTreeWriter::write(const DB::Block & block)
{
    auto new_block = removeColumnSuffix(block);
    auto converter = ActionsDAG::makeConvertingActions(
        new_block.getColumnsWithTypeAndName(), header.getColumnsWithTypeAndName(), DB::ActionsDAG::MatchColumnsMode::Position, context);
    const ExpressionActions expression_actions{std::move(converter)};
    expression_actions.execute(new_block);
    executor.push(new_block);
}

void SparkMergeTreeWriter::close()
{
    executor.finish();
    std::string result = PartInfo::toJson(getAllPartInfo());
    if (spark_job_id != CPP_UT_JOB_ID)
        SparkMergeTreeWriterJNI::setCurrentTaskWriteInfo(spark_job_id, result);
}

std::vector<PartInfo> SparkMergeTreeWriter::getAllPartInfo() const
{
    std::vector<PartInfo> res;
    auto parts = sink_helper.unsafeGet();
    res.reserve(parts.size());

    for (const auto & part : parts)
    {
        res.emplace_back(PartInfo{
            part.data_part->name,
            part.data_part->getMarksCount(),
            part.data_part->getBytesOnDisk(),
            part.data_part->rows_count,
            sink_helper.write_settings.partition_settings.partition_dir,
            sink_helper.write_settings.partition_settings.bucket_dir});
    }
    return res;
}

}
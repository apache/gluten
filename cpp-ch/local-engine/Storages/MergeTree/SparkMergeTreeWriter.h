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
#pragma once
#include <jni.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <Storages/NativeOutputWriter.h>

namespace DB
{
struct BlockWithPartition;
class MergeTreeData;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
}

namespace local_engine
{
class SinkHelper;

struct PartInfo
{
    String part_name;
    size_t mark_count;
    size_t disk_size;
    size_t row_count;
    String partition_values;
    String bucket_id;

    bool operator<(const PartInfo & rhs) const { return disk_size < rhs.disk_size; }

    static std::string toJson(const std::vector<PartInfo> & part_infos);
};

class SparkMergeTreeWriter : public NativeOutputWriter
{
public:
    static std::unique_ptr<SparkMergeTreeWriter>
    create(const MergeTreeTable & merge_tree_table, const DB::ContextMutablePtr & context, const std::string & spark_job_id);

    SparkMergeTreeWriter(
        const DB::Block & header_, const SinkHelper & sink_helper_, DB::QueryPipeline && pipeline_, const std::string & spark_job_id_, const DB::ContextPtr & context_);

    void write(const DB::Block & block) override;
    void close() override;

    /// visible for UTs
    static constexpr auto CPP_UT_JOB_ID = "__UT_JOB_ID__";

private:
    DB::Block header;
    const SinkHelper & sink_helper;
    DB::QueryPipeline pipeline;
    DB::PushingPipelineExecutor executor;
    const std::string spark_job_id;
    const DB::ContextPtr context;

    std::vector<PartInfo> getAllPartInfo() const;
};

namespace SparkMergeTreeWriterJNI
{
void init(JNIEnv *);
void destroy(JNIEnv *);
}

}

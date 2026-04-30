/*
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
#include <unordered_set>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/Joins.h>
#include <Functions/CastOverloadResolver.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <base/types.h>
#include <substrait/algebra.pb.h>
#include <Common/CurrentThread.h>
#include <Common/GlutenConfig.h>

namespace DB
{
class QueryPipeline;
class QueryPlan;
}

namespace local_engine
{
static const String MERGETREE_INSERT_WITHOUT_LOCAL_STORAGE = "mergetree.insert_without_local_storage";
static const String MERGETREE_MERGE_AFTER_INSERT = "mergetree.merge_after_insert";
static const std::string DECIMAL_OPERATIONS_ALLOW_PREC_LOSS = "spark.sql.decimalOperations.allowPrecisionLoss";
static const std::string TIMER_PARSER_POLICY = "spark.sql.legacy.timeParserPolicy";

static const std::unordered_set<String> BOOL_VALUE_SETTINGS{
    MERGETREE_MERGE_AFTER_INSERT, MERGETREE_INSERT_WITHOUT_LOCAL_STORAGE, DECIMAL_OPERATIONS_ALLOW_PREC_LOSS};
static const std::unordered_set<String> LONG_VALUE_SETTINGS{
    "optimize.maxfilesize", "optimize.minFileSize", "mergetree.max_num_part_per_merge_task"};

class BlockUtil
{
public:
    static constexpr auto VIRTUAL_ROW_COUNT_COLUMN = "__VIRTUAL_ROW_COUNT_COLUMN__";
    static constexpr auto RIGHT_COLUMN_PREFIX = "broadcast_right_";

    // Build a header block with a virtual column which will be
    // use to indicate the number of rows in a block.
    // Commonly seen in the following quries:
    // - select count(1) from t
    // - select 1 from t
    static DB::Block buildRowCountHeader();
    static DB::Chunk buildRowCountChunk(UInt64 rows);
    static DB::Block buildRowCountBlock(UInt64 rows);


    static constexpr UInt64 FLAT_STRUCT = 1;
    static constexpr UInt64 FLAT_NESTED_TABLE = 2;
    /// If it's a struct without named fields, also force to flatten it.
    static constexpr UInt64 FLAT_STRUCT_FORCE = 4;

    // flatten the struct and array(struct) columns.
    // It's different from Nested::flattend()
    static DB::Block flattenBlock(
        const DB::Block & block,
        UInt64 flags = FLAT_STRUCT | FLAT_NESTED_TABLE,
        bool recursively = false,
        const std::unordered_set<size_t> & columns_to_skip_flatten = {});

    static DB::Block concatenateBlocksMemoryEfficiently(std::vector<DB::Block> && blocks);

    /// The column names may be different in two blocks.
    /// and the nullability also could be different, with TPCDS-Q1 as an example.
    static DB::ColumnWithTypeAndName
    convertColumnAsNecessary(const DB::ColumnWithTypeAndName & column, const DB::ColumnWithTypeAndName & sample_column);
};

class PODArrayUtil
{
public:
    /// To allocate n bytes, PODArray will allocate n + pad_left + pad_right bytes in fact. So when
    /// we want to allocate 2^k bytes, 2^(k+1) bytes are allocated. This makes the memory usage far
    /// more than we expected, and easy to cause OOM. For example, we want to limit the max block size to be
    /// 64k rows, CH will make the memory usage equal to 128k rows, and half of the reserved memory is not used.
    /// So we adjust the size by considering the padding bytes, the return value may be samller then n.
    static size_t adjustMemoryEfficientSize(size_t n);
};

/// Use this class to extract element columns from columns of nested type in a block, e.g. named Tuple.
/// It can extract a column from a multiple nested type column, e.g. named Tuple in named Tuple
/// Keeps some intermediate data to avoid rebuild them multi-times.
class NestedColumnExtractHelper
{
public:
    explicit NestedColumnExtractHelper(const DB::Block & block_, bool case_insentive_);
    std::optional<DB::ColumnWithTypeAndName> extractColumn(const String & column_name);

private:
    std::optional<DB::ColumnWithTypeAndName>
    extractColumn(const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix);
    const DB::Block & block;
    bool case_insentive;
    std::map<String, DB::BlockPtr> nested_tables;

    const DB::ColumnWithTypeAndName * findColumn(const DB::Block & block, const std::string & name) const;
};

class ActionsDAGUtil
{
public:
    static const DB::ActionsDAG::Node * convertNodeType(
        DB::ActionsDAG & actions_dag,
        const DB::ActionsDAG::Node * node_to_cast,
        const DB::DataTypePtr & cast_to_type,
        DB::ContextPtr context,
        const std::string & result_name = "",
        DB::CastType cast_type = DB::CastType::nonAccurate);

    static const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        DB::ActionsDAG & actions_dag,
        const DB::ActionsDAG::Node * node,
        const DB::DataTypePtr & dst_type,
        DB::ContextPtr context,
        const std::string & result_name = "",
        DB::CastType cast_type = DB::CastType::nonAccurate);
};

class QueryPipelineUtil
{
public:
    static String explainPipeline(DB::QueryPipeline & pipeline);
};

void registerAllFunctions();
void registerGlutenDisks();

class BackendFinalizerUtil;
class JNIUtils;
class BackendInitializerUtil
{
public:
    static DB::Field toField(const String & key, const String & value);

    /// Initialize two kinds of resources
    /// 1. global level resources like global_context/shared_context, notice that they can only be initialized once in process lifetime
    /// 2. session level resources like settings/configs, they can be initialized multiple times following the lifetime of executor/driver
    static void initBackend(const SparkConfigs::ConfigMap & spark_conf_map);
    static void initSettings(const SparkConfigs::ConfigMap & spark_conf_map, DB::Settings & settings);

    inline static const String CH_BACKEND_PREFIX = "spark.gluten.sql.columnar.backend.ch";

    inline static const String CH_RUNTIME_CONFIG = "runtime_config";
    inline static const String CH_RUNTIME_CONFIG_PREFIX = CH_BACKEND_PREFIX + "." + CH_RUNTIME_CONFIG + ".";
    inline static const String CH_RUNTIME_CONFIG_FILE = CH_RUNTIME_CONFIG_PREFIX + "config_file";

    inline static const String CH_RUNTIME_SETTINGS = "runtime_settings";
    inline static const String CH_RUNTIME_SETTINGS_PREFIX = CH_BACKEND_PREFIX + "." + CH_RUNTIME_SETTINGS + ".";

    inline static const String SETTINGS_PATH = "local_engine.settings";
    inline static const String LIBHDFS3_CONF_KEY = "hdfs.libhdfs3_conf";
    inline static const std::string HADOOP_S3_ACCESS_KEY = "fs.s3a.access.key";
    inline static const std::string HADOOP_S3_SECRET_KEY = "fs.s3a.secret.key";
    inline static const std::string HADOOP_S3_ENDPOINT = "fs.s3a.endpoint";
    inline static const std::string HADOOP_S3_ASSUMED_ROLE = "fs.s3a.assumed.role.arn";
    inline static const std::string HADOOP_S3_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    inline static const std::string HADOOP_S3_ASSUMED_SESSION_NAME = "fs.s3a.assumed.role.session.name";
    // not hadoop official
    inline static const std::string HADOOP_S3_ASSUMED_EXTERNAL_ID = "fs.s3a.assumed.role.externalId";
    // hadoop official, this is used to ignore the cached client
    inline static const std::string HADOOP_S3_CLIENT_CACHE_IGNORE = "fs.s3a.client.cached.ignore";
    inline static const std::string SPARK_HADOOP_PREFIX = "spark.hadoop.";
    inline static const std::string S3A_PREFIX = "fs.s3a.";
    inline static const std::string SPARK_DELTA_PREFIX = "spark.databricks.delta.";
    inline static const std::string SPARK_SESSION_TIME_ZONE = "spark.sql.session.timeZone";

    inline static const String GLUTEN_TASK_OFFHEAP = "spark.gluten.memory.task.offHeap.size.in.bytes";

    /// On yarn mode, native writing on hdfs cluster takes yarn container user as the user passed to libhdfs3, which
    /// will cause permission issue because yarn container user is not the owner of the hdfs dir to be written.
    /// So we need to get the spark user from env and pass it to libhdfs3.
    inline static std::optional<String> spark_user;

private:
    friend class BackendFinalizerUtil;
    friend class JNIUtils;

    static DB::Context::ConfigurationPtr initConfig(const SparkConfigs::ConfigMap & spark_conf_map);
    static String tryGetConfigFile(const SparkConfigs::ConfigMap & spark_conf_map);
    static void initLoggers(DB::Context::ConfigurationPtr config);
    static void initEnvs(DB::Context::ConfigurationPtr config);

    static void initContexts(DB::Context::ConfigurationPtr config);
    static void initCompiledExpressionCache(DB::Context::ConfigurationPtr config);
    static void registerAllFactories();
    static void applyGlobalConfigAndSettings(const DB::Context::ConfigurationPtr & config, const DB::Settings & settings);
    static std::vector<String>
    wrapDiskPathConfig(const String & path_prefix, const String & path_suffix, Poco::Util::AbstractConfiguration & config);


    inline static std::once_flag init_flag;
    inline static Poco::Logger * logger;
};

class BackendFinalizerUtil
{
public:
    /// Release global level resources like global_context/shared_context. Invoked only once in the lifetime of process when JVM is shuting down.
    static void finalizeGlobally();

    /// Release session level resources like StorageJoinBuilder. Invoked every time executor/driver shutdown.
    static void finalizeSessionally();

    static std::vector<String> paths_need_to_clean;
    static std::mutex paths_mutex;
};

// Ignore memory track, memory should free before IgnoreMemoryTracker deconstruction
class IgnoreMemoryTracker
{
public:
    explicit IgnoreMemoryTracker(size_t limit_) : limit(limit_) { DB::CurrentThread::get().untracked_memory_limit += limit; }
    ~IgnoreMemoryTracker() { DB::CurrentThread::get().untracked_memory_limit -= limit; }

private:
    size_t limit;
};

class DateTimeUtil
{
public:
    static Int64 currentTimeMillis();
    static String convertTimeZone(const String & time_zone);
};

class MemoryUtil
{
public:
    static UInt64 getMemoryRSS();
};

class JoinUtil
{
public:
    static constexpr auto CROSS_REL_LEFT_CONST_KEY_COLUMN = "__CROSS_REL_LEFT_CONST_KEY_COLUMN__";
    static constexpr auto CROSS_REL_RIGHT_CONST_KEY_COLUMN = "__CROSS_REL_RIGHT_CONST_KEY_COLUMN__";

    // Keep necessarily columns and reorder them according to cols
    static void adjustJoinOutput(DB::QueryPlan & plan, DB::Names cols);
    static std::pair<DB::JoinKind, DB::JoinStrictness>
    getJoinKindAndStrictness(substrait::JoinRel_JoinType join_type, bool is_existence_join);
    static std::pair<DB::JoinKind, DB::JoinStrictness> getCrossJoinKindAndStrictness(substrait::CrossRel_JoinType join_type);
};

}

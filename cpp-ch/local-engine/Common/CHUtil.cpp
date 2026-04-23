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

#include "CHUtil.h"

#include <filesystem>
#include <memory>
#include <optional>
#include <unistd.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Defines.h>
#include <Core/NamesAndTypes.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Disks/registerDisks.h>
#include <Disks/registerGlutenDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/registerFunctions.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Parser/RelParsers/RelParser.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/printPipeline.h>
#include <Storages/Cache/CacheManager.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <Storages/Output/WriteBufferBuilder.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <arrow/util/compression.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <sys/resource.h>
#include <Poco/Logger.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/BitHelpers.h>
#include <Common/BlockTypeUtils.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/GlutenSignalHandler.h>
#include <Common/LoggerExtend.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ServerSetting
{
extern const ServerSettingsString primary_index_cache_policy;
extern const ServerSettingsUInt64 primary_index_cache_size;
extern const ServerSettingsDouble primary_index_cache_size_ratio;
extern const ServerSettingsUInt64 max_prefixes_deserialization_thread_pool_size;
extern const ServerSettingsUInt64 max_prefixes_deserialization_thread_pool_free_size;
extern const ServerSettingsUInt64 prefixes_deserialization_thread_pool_thread_pool_queue_size;
extern const ServerSettingsUInt64 max_thread_pool_size;
extern const ServerSettingsUInt64 thread_pool_queue_size;
extern const ServerSettingsUInt64 max_io_thread_pool_size;
extern const ServerSettingsUInt64 io_thread_pool_queue_size;
extern const ServerSettingsString vector_similarity_index_cache_policy;
extern const ServerSettingsUInt64 vector_similarity_index_cache_size;
extern const ServerSettingsUInt64 vector_similarity_index_cache_max_entries;
extern const ServerSettingsDouble vector_similarity_index_cache_size_ratio;
}
namespace Setting
{
extern const SettingsMaxThreads max_threads;
extern const SettingsUInt64 prefer_external_sort_block_bytes;
extern const SettingsUInt64 max_bytes_before_external_sort;
extern const SettingsDouble max_bytes_ratio_before_external_sort;
extern const SettingsBool query_plan_merge_filters;
extern const SettingsBool compile_expressions;
extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;
extern const SettingsUInt64 output_format_compression_level;
extern const SettingsBool query_plan_optimize_lazy_materialization;
}
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TYPE;
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}

extern void registerAggregateFunctionUniqHyperLogLogPlusPlus(AggregateFunctionFactory &);
}

namespace local_engine
{
namespace fs = std::filesystem;

DB::Block BlockUtil::buildRowCountHeader()
{
    return DB::Block{createColumn<UInt8>({}, VIRTUAL_ROW_COUNT_COLUMN)};
}

DB::Chunk BlockUtil::buildRowCountChunk(UInt64 rows)
{
    return DB::Chunk{{createColumnConst<UInt8>(rows, 0)}, rows};
}

DB::Block BlockUtil::buildRowCountBlock(UInt64 rows)
{
    return DB::Block{createColumnConst<UInt8>(rows, 0, VIRTUAL_ROW_COUNT_COLUMN)};
}

/// The column names may be different in two blocks.
/// and the nullability also could be different, with TPCDS-Q1 as an example.
DB::ColumnWithTypeAndName
BlockUtil::convertColumnAsNecessary(const DB::ColumnWithTypeAndName & column, const DB::ColumnWithTypeAndName & sample_column)
{
    if (sample_column.type->equals(*column.type))
        return {column.column, column.type, sample_column.name};
    else if (sample_column.type->isNullable() && !column.type->isNullable() && DB::removeNullable(sample_column.type)->equals(*column.type))
    {
        auto nullable_column = column;
        DB::JoinCommon::convertColumnToNullable(nullable_column);
        return {nullable_column.column, sample_column.type, sample_column.name};
    }
    else
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Columns have different types. original:{} expected:{}",
            column.dumpStructure(),
            sample_column.dumpStructure());
}

/**
 * There is a special case with which we need be careful. In spark, struct/map/list are always
 * wrapped in Nullable, but this should not happen in clickhouse.
 */
DB::Block
BlockUtil::flattenBlock(const DB::Block & block, UInt64 flags, bool recursively, const std::unordered_set<size_t> & columns_to_skip_flatten)
{
    DB::Block res;

    for (size_t col_i = 0; col_i < block.columns(); ++col_i)
    {
        const auto & elem = block.getByPosition(col_i);

        if (columns_to_skip_flatten.contains(col_i))
        {
            res.insert(elem);
            continue;
        }

        DB::DataTypePtr nested_type = removeNullable(elem.type);
        DB::ColumnPtr nested_col = elem.column;
        DB::ColumnPtr null_map_col = nullptr;
        // A special case, const(Nullable(nothing))
        if (elem.type->isNullable() && typeid_cast<const DB::ColumnNullable *>(elem.column->getPtr().get()))
        {
            const auto * nullable_col = typeid_cast<const DB::ColumnNullable *>(elem.column->getPtr().get());
            nested_col = nullable_col->getNestedColumnPtr();
            null_map_col = nullable_col->getNullMapColumnPtr();
        }

        if (const DB::DataTypeArray * type_arr = typeid_cast<const DB::DataTypeArray *>(nested_type.get()))
        {
            const DB::DataTypeTuple * type_tuple = typeid_cast<const DB::DataTypeTuple *>(type_arr->getNestedType().get());
            if (type_tuple && type_tuple->hasExplicitNames() && (flags & FLAT_NESTED_TABLE))
            {
                const DB::DataTypes & element_types = type_tuple->getElements();
                const DB::Strings & names = type_tuple->getElementNames();
                size_t tuple_size = element_types.size();

                bool is_const = isColumnConst(*nested_col);
                const DB::ColumnArray * column_array;
                if (is_const)
                    column_array = typeid_cast<const DB::ColumnArray *>(&assert_cast<const DB::ColumnConst &>(*nested_col).getDataColumn());
                else
                    column_array = typeid_cast<const DB::ColumnArray *>(nested_col.get());

                const DB::ColumnPtr & column_offsets = column_array->getOffsetsPtr();

                const DB::ColumnTuple & column_tuple = typeid_cast<const DB::ColumnTuple &>(column_array->getData());
                const auto & element_columns = column_tuple.getColumns();

                for (size_t i = 0; i < tuple_size; ++i)
                {
                    String nested_name = DB::Nested::concatenateName(elem.name, names[i]);
                    DB::ColumnPtr column_array_of_element = DB::ColumnArray::create(element_columns[i], column_offsets);
                    auto named_column_array_of_element = DB::ColumnWithTypeAndName(
                        is_const ? DB::ColumnConst::create(std::move(column_array_of_element), block.rows()) : column_array_of_element,
                        std::make_shared<DB::DataTypeArray>(element_types[i]),
                        nested_name);

                    if (null_map_col)
                    {
                        // Should all field columns have the same null map ?
                        DB::DataTypePtr null_type = std::make_shared<DB::DataTypeNullable>(element_types[i]);
                        named_column_array_of_element.column
                            = DB::ColumnNullable::create(named_column_array_of_element.column, null_map_col);
                        named_column_array_of_element.type = null_type;
                    }

                    if (recursively)
                    {
                        auto flatten_one_col_block = flattenBlock({named_column_array_of_element}, flags, recursively);
                        for (const auto & named_col : flatten_one_col_block.getColumnsWithTypeAndName())
                            res.insert(named_col);
                    }
                    else
                        res.insert(named_column_array_of_element);
                }
            }
            else
                res.insert(elem);
        }
        else if (const DB::DataTypeTuple * type_tuple = typeid_cast<const DB::DataTypeTuple *>(nested_type.get()))
        {
            if ((flags & FLAT_STRUCT_FORCE) || (type_tuple->hasExplicitNames() && (flags & FLAT_STRUCT)))
            {
                const DB::DataTypes & element_types = type_tuple->getElements();
                DB::Strings element_names = type_tuple->getElementNames();
                if (element_names.empty())
                {
                    // This is a struct without named fields, we should flatten it.
                    // But we can't get the field names, so we use the field index as the field name.
                    for (size_t i = 0; i < element_types.size(); ++i)
                        element_names.push_back(elem.name + "_filed_" + std::to_string(i));
                }

                const DB::ColumnTuple * column_tuple;
                if (isColumnConst(*nested_col))
                    column_tuple = typeid_cast<const DB::ColumnTuple *>(&assert_cast<const DB::ColumnConst &>(*nested_col).getDataColumn());
                else
                    column_tuple = typeid_cast<const DB::ColumnTuple *>(nested_col.get());

                size_t tuple_size = column_tuple->tupleSize();
                for (size_t i = 0; i < tuple_size; ++i)
                {
                    const auto & element_column = column_tuple->getColumn(i);
                    String nested_name = DB::Nested::concatenateName(elem.name, element_names[i]);
                    auto new_element_col = DB::ColumnWithTypeAndName(element_column.getPtr(), element_types[i], nested_name);
                    if (null_map_col && !element_types[i]->isNullable())
                    {
                        // Should all field columns have the same null map ?
                        new_element_col.column = DB::ColumnNullable::create(new_element_col.column, null_map_col);
                        new_element_col.type = std::make_shared<DB::DataTypeNullable>(new_element_col.type);
                    }

                    if (recursively)
                    {
                        DB::Block one_col_block({new_element_col});
                        auto flatten_one_col_block = flattenBlock(one_col_block, flags, recursively);
                        for (const auto & named_col : flatten_one_col_block.getColumnsWithTypeAndName())
                            res.insert(named_col);
                    }
                    else
                        res.insert(std::move(new_element_col));
                }
            }
            else
                res.insert(elem);
        }
        else
            res.insert(elem);
    }

    return res;
}

DB::Block BlockUtil::concatenateBlocksMemoryEfficiently(std::vector<DB::Block> && blocks)
{
    if (blocks.empty())
        return {};

    size_t num_rows = 0;
    for (const auto & block : blocks)
        num_rows += block.rows();

    DB::Block out = blocks[0].cloneEmpty();
    DB::MutableColumns columns = out.mutateColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns[i]->reserve(num_rows);
        for (auto & block : blocks)
        {
            const auto & tmp_column = *block.getByPosition(0).column;
            columns[i]->insertRangeFrom(tmp_column, 0, block.rows());
            block.erase(0);
        }
    }
    blocks.clear();

    out.setColumns(std::move(columns));
    return out;
}

size_t PODArrayUtil::adjustMemoryEfficientSize(size_t n)
{
    /// According to definition of DEFUALT_BLOCK_SIZE
    size_t padding_n = 2 * DB::PADDING_FOR_SIMD - 1;
    size_t rounded_n = roundUpToPowerOfTwoOrZero(n);
    size_t padded_n = n;
    if (rounded_n > n + n / 2)
    {
        size_t smaller_rounded_n = rounded_n / 2;
        padded_n = smaller_rounded_n < padding_n ? n : smaller_rounded_n - padding_n;
    }
    else
    {
        padded_n = rounded_n - padding_n;
    }
    return padded_n;
}

NestedColumnExtractHelper::NestedColumnExtractHelper(const DB::Block & block_, bool case_insentive_)
    : block(block_), case_insentive(case_insentive_)
{
}

std::optional<DB::ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(const String & column_name)
{
    if (const auto * col = findColumn(block, column_name))
        return {*col};

    auto nested_names = DB::Nested::splitName(column_name);
    if (case_insentive)
    {
        boost::to_lower(nested_names.first);
        boost::to_lower(nested_names.second);
    }
    if (!findColumn(block, nested_names.first))
        return {};

    if (!nested_tables.contains(nested_names.first))
    {
        DB::ColumnsWithTypeAndName columns = {*findColumn(block, nested_names.first)};
        nested_tables[nested_names.first] = std::make_shared<DB::Block>(BlockUtil::flattenBlock(columns));
    }

    return extractColumn(column_name, nested_names.first, nested_names.second);
}

std::optional<DB::ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(
    const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix)
{
    auto table_iter = nested_tables.find(column_name_prefix);
    if (table_iter == nested_tables.end())
        return {};

    auto & nested_table = table_iter->second;
    auto nested_names = DB::Nested::splitName(column_name_suffix);
    auto new_column_name_prefix = DB::Nested::concatenateName(column_name_prefix, nested_names.first);
    if (nested_names.second.empty())
    {
        if (const auto * column_ref = findColumn(*nested_table, new_column_name_prefix))
        {
            DB::ColumnWithTypeAndName column = *column_ref;
            if (case_insentive)
                column.name = original_column_name;
            return {std::move(column)};
        }
        else
        {
            return {};
        }
    }

    const auto * sub_col = findColumn(*nested_table, new_column_name_prefix);
    if (!sub_col)
        return {};

    DB::ColumnsWithTypeAndName columns = {*sub_col};
    DB::Block sub_block(columns);
    nested_tables[new_column_name_prefix] = std::make_shared<DB::Block>(BlockUtil::flattenBlock(sub_block));
    return extractColumn(original_column_name, new_column_name_prefix, nested_names.second);
}

const DB::ColumnWithTypeAndName * NestedColumnExtractHelper::findColumn(const DB::Block & in_block, const std::string & name) const
{
    return in_block.findByName(name, case_insentive);
}

const DB::ActionsDAG::Node * ActionsDAGUtil::convertNodeType(
    DB::ActionsDAG & actions_dag,
    const DB::ActionsDAG::Node * node,
    const DB::DataTypePtr & cast_to_type,
    const std::string & result_name,
    DB::CastType cast_type)
{
    DB::ColumnWithTypeAndName type_name_col;
    type_name_col.name = cast_to_type->getName();
    type_name_col.column = DB::DataTypeString().createColumnConst(0, type_name_col.name);
    type_name_col.type = std::make_shared<DB::DataTypeString>();
    const auto * right_arg = &actions_dag.addColumn(std::move(type_name_col));
    const auto * left_arg = node;
    DB::CastDiagnostic diagnostic = {node->result_name, node->result_name};
    DB::ColumnWithTypeAndName left_column{nullptr, node->result_type, {}};
    DB::ActionsDAG::NodeRawConstPtrs children = {left_arg, right_arg};
    auto func_base_cast = createInternalCast(std::move(left_column), cast_to_type, cast_type, diagnostic);

    return &actions_dag.addFunction(func_base_cast, std::move(children), result_name);
}

const DB::ActionsDAG::Node * ActionsDAGUtil::convertNodeTypeIfNeeded(
    DB::ActionsDAG & actions_dag,
    const DB::ActionsDAG::Node * node,
    const DB::DataTypePtr & dst_type,
    const std::string & result_name,
    DB::CastType cast_type)
{
    if (node->result_type->equals(*dst_type))
        return node;

    return convertNodeType(actions_dag, node, dst_type, result_name, cast_type);
}

String QueryPipelineUtil::explainPipeline(DB::QueryPipeline & pipeline)
{
    DB::WriteBufferFromOwnString buf;
    const auto & processors = pipeline.getProcessors();
    DB::printPipelineCompact(processors, buf, true);
    return buf.str();
}

using namespace DB;

std::vector<String> BackendInitializerUtil::wrapDiskPathConfig(
    const String & path_prefix, const String & path_suffix, Poco::Util::AbstractConfiguration & config)
{
    std::vector<String> changed_paths;
    if (path_prefix.empty() && path_suffix.empty())
        return changed_paths;

    auto change_func = [&](String key) -> void
    {
        if (const String value = config.getString(key, ""); value != "")
        {
            const String change_value = path_prefix + value + path_suffix;
            config.setString(key, change_value);
            changed_paths.emplace_back(change_value);
            LOG_INFO(getLogger("BackendInitializerUtil"), "Change config `{}` from '{}' to {}.", key, value, change_value);
        }
    };

    Poco::Util::AbstractConfiguration::Keys disks;
    std::unordered_set<String> disk_types = {GlutenObjectStorageConfig::S3_DISK_TYPE, GlutenObjectStorageConfig::HDFS_DISK_TYPE, "cache"};
    config.keys("storage_configuration.disks", disks);

    std::ranges::for_each(
        disks,
        [&](const auto & disk_name)
        {
            String disk_prefix = "storage_configuration.disks." + disk_name;
            String disk_type = config.getString(disk_prefix + ".type", "");
            if (!disk_types.contains(disk_type))
                return;
            if (disk_type == "cache")
                change_func(disk_prefix + ".path");
            else if (disk_type == GlutenObjectStorageConfig::S3_DISK_TYPE || disk_type == GlutenObjectStorageConfig::HDFS_DISK_TYPE)
                change_func(disk_prefix + ".metadata_path");
        });

    change_func("path");
    change_func(GlutenCacheConfig::PREFIX + ".path");

    return changed_paths;
}

DB::Context::ConfigurationPtr BackendInitializerUtil::initConfig(const SparkConfigs::ConfigMap & spark_conf_map)
{
    DB::Context::ConfigurationPtr config;

    if (const String config_file = tryGetConfigFile(spark_conf_map); !config_file.empty())
    {
        if (fs::exists(config_file) && fs::is_regular_file(config_file))
        {
            ConfigProcessor config_processor(config_file, false, true);
            DB::ConfigProcessor::setConfigPath(fs::path(config_file).parent_path());
            const auto loaded_config = config_processor.loadConfig(false);
            config = loaded_config.configuration;
        }
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{} is not a valid configure file.", config_file);
    }
    else
        config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    for (const auto & [key, value] : spark_conf_map)
    {
        if (key.starts_with(CH_RUNTIME_CONFIG_PREFIX) && key != CH_RUNTIME_CONFIG_FILE)
        {
            // Apply spark.gluten.sql.columnar.backend.ch.runtime_config.* to config
            const auto name = key.substr(CH_RUNTIME_CONFIG_PREFIX.size());
            if ((name == "storage_configuration.disks.s3.metadata_path" || name == "path") && !value.ends_with("/"))
                config->setString(name, value + "/");
            else
                config->setString(name, value);
        }
    }

    if (spark_conf_map.contains(GLUTEN_TASK_OFFHEAP))
        config->setString(MemoryConfig::CH_TASK_MEMORY, spark_conf_map.at(GLUTEN_TASK_OFFHEAP));

    const bool use_current_directory_as_tmp = config->getBool(PathConfig::USE_CURRENT_DIRECTORY_AS_TMP, false);
    char buffer[PATH_MAX];
    if (use_current_directory_as_tmp && getcwd(buffer, sizeof(buffer)) != nullptr)
        wrapDiskPathConfig(String(buffer), "", *config);

    const bool reuse_disk_cache = config->getBool("reuse_disk_cache", true);

    if (!reuse_disk_cache)
    {
        String pid = std::to_string(static_cast<Int64>(getpid()));
        auto path_need_clean = wrapDiskPathConfig("", "/" + pid, *config);
        std::lock_guard lock(BackendFinalizerUtil::paths_mutex);
        BackendFinalizerUtil::paths_need_to_clean.insert(
            BackendFinalizerUtil::paths_need_to_clean.end(), path_need_clean.begin(), path_need_clean.end());
    }

    // FIXMEX: workaround for https://github.com/ClickHouse/ClickHouse/pull/75452#pullrequestreview-2625467710
    // entry in DiskSelector::initialize
    // Bug in FileCacheSettings::loadFromConfig
    auto updateCacheDiskType = [](Poco::Util::AbstractConfiguration & config)
    {
        const std::string config_prefix = "storage_configuration.disks";
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);
        for (const auto & disk_name : keys)
        {
            const auto disk_config_prefix = config_prefix + "." + disk_name;
            const auto disk_type = config.getString(disk_config_prefix + ".type", "local");
            if (disk_type == "cache")
                config.setString(disk_config_prefix, "workaround");
        }
        config.setString(GlutenCacheConfig::PREFIX, "workaround");
    };

    updateCacheDiskType(*config);

    return config;
}

String BackendInitializerUtil::tryGetConfigFile(const SparkConfigs::ConfigMap & spark_conf_map)
{
    if (spark_conf_map.contains(CH_RUNTIME_CONFIG_FILE))
        return spark_conf_map.at(CH_RUNTIME_CONFIG_FILE);

    /// Try to get config path from environment variable
    if (const char * config_path = std::getenv("CLICKHOUSE_BACKEND_CONFIG"))
        return config_path;

    return String{};
}


void BackendInitializerUtil::initLoggers(DB::Context::ConfigurationPtr config)
{
    auto level = config->getString("logger.level", "warning");
    if (config->has("logger.log"))
        local_engine::LoggerExtend::initFileLogger(*config, "ClickHouseBackend");
    else
        local_engine::LoggerExtend::initConsoleLogger(level);

    logger = &Poco::Logger::get("ClickHouseBackend");
}

void BackendInitializerUtil::initEnvs(DB::Context::ConfigurationPtr config)
{
    /// Set environment variable TZ if possible
    if (config->has("timezone"))
    {
        const std::string config_timezone = config->getString("timezone");
        const String mapped_timezone = DateTimeUtil::convertTimeZone(config_timezone);
        if (0
            != setenv(
                "TZ", mapped_timezone.data(), 1)) // NOLINT(concurrency-mt-unsafe) // ok if not called concurrently with other setenv/getenv
            throw Poco::Exception("Cannot setenv TZ variable");

        tzset();
        DateLUT::setDefaultTimezone(mapped_timezone);
    }

    /// Set environment variable LIBHDFS3_CONF if possible
    if (config->has(LIBHDFS3_CONF_KEY))
    {
        std::string libhdfs3_conf = config->getString(LIBHDFS3_CONF_KEY, "");
        setenv("LIBHDFS3_CONF", libhdfs3_conf.c_str(), true); /// NOLINT
    }

    /// Enable logging in libhdfs3, logs will be written to stderr
    setenv("HDFS_ENABLE_LOGGING", "true", true); /// NOLINT

    /// Get environment varaible SPARK_USER if possible
    if (const char * spark_user_c_str = std::getenv("SPARK_USER"))
        spark_user = spark_user_c_str;
}

DB::Field BackendInitializerUtil::toField(const String & key, const String & value)
{
    if (BOOL_VALUE_SETTINGS.contains(key))
        return DB::Field(value == "true" || value == "1");
    else if (LONG_VALUE_SETTINGS.contains(key))
        return DB::Field(std::strtoll(value.c_str(), nullptr, 10));
    else
        return DB::Field(value);
}

void BackendInitializerUtil::initSettings(const SparkConfigs::ConfigMap & spark_conf_map, DB::Settings & settings)
{
    /// Initialize default setting.
    settings.set("date_time_input_format", "best_effort");
    settings.set(MERGETREE_MERGE_AFTER_INSERT, true);
    settings.set(MERGETREE_INSERT_WITHOUT_LOCAL_STORAGE, false);
    settings.set(DECIMAL_OPERATIONS_ALLOW_PREC_LOSS, true);
    settings.set("remote_filesystem_read_prefetch", false);
    settings.set("max_parsing_threads", 1);
    settings.set("max_download_threads", 1);
    settings.set("input_format_parquet_enable_row_group_prefetch", false);
    settings.set("output_format_parquet_use_custom_encoder", false);

    //1.
    // TODO: we need set Setting::max_threads to 1 by default, but now we can't get correct metrics for the some query if we set it to 1.
    // settings[Setting::max_threads] = 1;

    /// 2. After https://github.com/ClickHouse/ClickHouse/pull/71539
    /// Set false to query_plan_merge_filters.
    /// If true, we can't get correct metrics for the query
    settings[Setting::query_plan_merge_filters] = false;

    /// 3. After https://github.com/ClickHouse/ClickHouse/pull/70598.
    /// Set false to compile_expressions to avoid dead loop.
    /// TODO: FIXME set true again.
    /// We now set BuildQueryPipelineSettings according to config.
    settings[Setting::compile_expressions] = false;
    settings[Setting::short_circuit_function_evaluation] = ShortCircuitFunctionEvaluation::DISABLE;

    /// 4. After https://github.com/ClickHouse/ClickHouse/pull/73422
    /// Since we already set max_bytes_before_external_sort, set max_bytes_ratio_before_external_sort to 0
    settings[Setting::max_bytes_ratio_before_external_sort] = 0.;

    /// 5. After https://github.com/ClickHouse/ClickHouse/pull/73651.
    /// See following settings, we always use Snappy compression for Parquet, however after https://github.com/ClickHouse/ClickHouse/pull/73651,
    /// output_format_compression_level is set to 3, which is wrong, since snappy does not support it.
    settings[Setting::output_format_compression_level] = arrow::util::kUseDefaultCompressionLevel;

    /// 6. After https://github.com/ClickHouse/ClickHouse/pull/55518
    /// We currently do not support lazy materialization.
    /// "test 'order by' two keys" will failed if we enable it.
    settings[Setting::query_plan_optimize_lazy_materialization] = false;

    for (const auto & [key, value] : spark_conf_map)
    {
        // Firstly apply spark.gluten.sql.columnar.backend.ch.runtime_config.local_engine.settings.* to settings
        if (key.starts_with(CH_RUNTIME_CONFIG_PREFIX + SETTINGS_PATH + "."))
        {
            settings.set(key.substr((CH_RUNTIME_CONFIG_PREFIX + SETTINGS_PATH + ".").size()), value);
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings from config key:{} value:{}", key, value);
        }
        else if (key.starts_with(CH_RUNTIME_SETTINGS_PREFIX))
        {
            auto k = key.substr(CH_RUNTIME_SETTINGS_PREFIX.size());
            settings.set(k, toField(k, value));
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings key:{} value:{}", key, value);
        }
        else if (key.starts_with(SPARK_HADOOP_PREFIX + S3A_PREFIX))
        {
            // Apply general S3 configs, e.g. spark.hadoop.fs.s3a.access.key -> set in fs.s3a.access.key
            // deal with per bucket S3 configs, e.g. fs.s3a.bucket.bucket_name.assumed.role.arn
            // for gluten, we require first authenticate with AK/SK(or instance profile), then assume other roles with STS
            // so only the following per-bucket configs are supported:
            // 1. fs.s3a.bucket.bucket_name.assumed.role.arn
            // 2. fs.s3a.bucket.bucket_name.assumed.role.session.name
            // 3. fs.s3a.bucket.bucket_name.endpoint
            // 4. fs.s3a.bucket.bucket_name.assumed.role.externalId (non hadoop official)
            settings.set(key.substr(SPARK_HADOOP_PREFIX.length()), value);
        }
        else if (key.starts_with(SPARK_DELTA_PREFIX))
        {
            auto k = key.substr(SPARK_DELTA_PREFIX.size());
            settings.set(k, toField(k, value));
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings key:{} value:{}", key, value);
        }
        else if (key == SPARK_SESSION_TIME_ZONE)
        {
            String time_zone_val = DateTimeUtil::convertTimeZone(value);
            settings.set("session_timezone", time_zone_val);
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings key:{} value:{}", "session_timezone", time_zone_val);
        }
        else if (key == DECIMAL_OPERATIONS_ALLOW_PREC_LOSS)
        {
            settings.set(key, toField(key, value));
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings key:{} value:{}", key, value);
        }
        else if (key == TIMER_PARSER_POLICY)
        {
            settings.set(key, value);
            LOG_DEBUG(&Poco::Logger::get("CHUtil"), "Set settings key:{} value:{}", key, value);
        }
    }

    /// Finally apply some fixed kvs to settings.
    settings.set("join_use_nulls", true);
    settings.set("input_format_orc_allow_missing_columns", true);
    settings.set("input_format_orc_case_insensitive_column_matching", true);
    settings.set("input_format_orc_import_nested", true);
    settings.set("input_format_orc_skip_columns_with_unsupported_types_in_schema_inference", true);
    settings.set("input_format_parquet_allow_missing_columns", true);
    settings.set("input_format_parquet_case_insensitive_column_matching", true);
    settings.set("input_format_parquet_import_nested", true);
    settings.set("input_format_json_read_numbers_as_strings", true);
    settings.set("input_format_json_read_bools_as_numbers", false);
    settings.set("input_format_csv_trim_whitespaces", false);
    settings.set("input_format_csv_allow_cr_end_of_line", true);
    settings.set("output_format_orc_string_as_string", true);
    settings.set("output_format_parquet_version", "1.0");
    settings.set("output_format_parquet_compression_method", "snappy");
    settings.set("output_format_parquet_string_as_string", true);
    settings.set("output_format_parquet_fixed_string_as_fixed_byte_array", false);
    settings.set("output_format_json_quote_64bit_integers", false);
    settings.set("output_format_json_quote_denormals", true);
    settings.set("output_format_json_skip_null_value_in_named_tuples", true);
    settings.set("output_format_json_escape_forward_slashes", false);
    settings.set("function_json_value_return_type_allow_complex", true);
    settings.set("function_json_value_return_type_allow_nullable", true);
    settings.set("precise_float_parsing", true);
    settings.set("enable_named_columns_in_function_tuple", false);
    settings.set("date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", true);
    settings.set("input_format_orc_dictionary_as_low_cardinality", false); //after https://github.com/ClickHouse/ClickHouse/pull/69481

    if (spark_conf_map.contains(GLUTEN_TASK_OFFHEAP))
    {
        auto task_memory = std::stoull(spark_conf_map.at(GLUTEN_TASK_OFFHEAP));
        if (!spark_conf_map.contains(CH_RUNTIME_SETTINGS_PREFIX + "max_bytes_before_external_sort"))
        {
            settings[Setting::max_bytes_before_external_sort] = static_cast<size_t>(0.8 * task_memory);
        }
        if (!spark_conf_map.contains(CH_RUNTIME_SETTINGS_PREFIX + "prefer_external_sort_block_bytes"))
        {
            auto mem_gb = task_memory / static_cast<double>(1_GiB);
            // 2.8x+5, Heuristics calculate the block size of external sort, [8,16]
            settings[Setting::prefer_external_sort_block_bytes]
                = std::max(std::min(static_cast<size_t>(2.8 * mem_gb + 5), 16ul), 8ul) * 1024 * 1024;
        }
    }
}

void BackendInitializerUtil::initContexts(DB::Context::ConfigurationPtr config)
{
    /// Make sure global_context and shared_context are constructed only once.
    if (auto global_context = QueryContext::globalMutableContext(); !global_context)
    {
        ServerSettings server_settings;
        server_settings.loadSettingsFromConfig(*config);

        auto log = getLogger("CHUtil");
        global_context = QueryContext::createGlobal();
        global_context->makeGlobalContext();
        global_context->setConfig(config);

        auto tmp_path = config->getString("tmp_path", PathConfig::DEFAULT_TEMP_FILE_PATH);
        if (config->getBool(PathConfig::USE_CURRENT_DIRECTORY_AS_TMP, false))
        {
            char buffer[PATH_MAX];
            if (getcwd(buffer, sizeof(buffer)) != nullptr)
                tmp_path = std::string(buffer) + tmp_path;
        };

        if (!fs::exists(tmp_path))
            fs::create_directories(tmp_path);
        global_context->setTemporaryStoragePath(tmp_path, 0);
        global_context->setPath(config->getString("path", "/"));

        String uncompressed_cache_policy = config->getString("uncompressed_cache_policy", DEFAULT_UNCOMPRESSED_CACHE_POLICY);
        size_t uncompressed_cache_size = config->getUInt64("uncompressed_cache_size", DEFAULT_UNCOMPRESSED_CACHE_MAX_SIZE);
        double uncompressed_cache_size_ratio = config->getDouble("uncompressed_cache_size_ratio", DEFAULT_UNCOMPRESSED_CACHE_SIZE_RATIO);
        global_context->setUncompressedCache(uncompressed_cache_policy, uncompressed_cache_size, uncompressed_cache_size_ratio);

        String mark_cache_policy = config->getString("mark_cache_policy", DEFAULT_MARK_CACHE_POLICY);
        size_t mark_cache_size = config->getUInt64("mark_cache_size", DEFAULT_MARK_CACHE_MAX_SIZE);
        double mark_cache_size_ratio = config->getDouble("mark_cache_size_ratio", DEFAULT_MARK_CACHE_SIZE_RATIO);
        if (!mark_cache_size)
            LOG_ERROR(log, "Mark cache is disabled, it will lead to severe performance degradation.");
        LOG_INFO(log, "mark cache size to {}.", formatReadableSizeWithBinarySuffix(mark_cache_size));
        global_context->setMarkCache(mark_cache_policy, mark_cache_size, mark_cache_size_ratio);

        String primary_index_cache_policy = server_settings[ServerSetting::primary_index_cache_policy];
        size_t primary_index_cache_size = server_settings[ServerSetting::primary_index_cache_size];
        double primary_index_cache_size_ratio = server_settings[ServerSetting::primary_index_cache_size_ratio];
        LOG_INFO(log, "Primary index cache size to {}.", formatReadableSizeWithBinarySuffix(primary_index_cache_size));
        global_context->setPrimaryIndexCache(primary_index_cache_policy, primary_index_cache_size, primary_index_cache_size_ratio);

        String index_uncompressed_cache_policy
            = config->getString("index_uncompressed_cache_policy", DEFAULT_INDEX_UNCOMPRESSED_CACHE_POLICY);
        size_t index_uncompressed_cache_size
            = config->getUInt64("index_uncompressed_cache_size", DEFAULT_INDEX_UNCOMPRESSED_CACHE_MAX_SIZE);
        double index_uncompressed_cache_size_ratio
            = config->getDouble("index_uncompressed_cache_size_ratio", DEFAULT_INDEX_UNCOMPRESSED_CACHE_SIZE_RATIO);
        global_context->setIndexUncompressedCache(
            index_uncompressed_cache_policy, index_uncompressed_cache_size, index_uncompressed_cache_size_ratio);

        String index_mark_cache_policy = config->getString("index_mark_cache_policy", DEFAULT_INDEX_MARK_CACHE_POLICY);
        size_t index_mark_cache_size = config->getUInt64("index_mark_cache_size", DEFAULT_INDEX_MARK_CACHE_MAX_SIZE);
        double index_mark_cache_size_ratio = config->getDouble("index_mark_cache_size_ratio", DEFAULT_INDEX_MARK_CACHE_SIZE_RATIO);
        global_context->setIndexMarkCache(index_mark_cache_policy, index_mark_cache_size, index_mark_cache_size_ratio);

        String vector_similarity_index_cache_policy = server_settings[ServerSetting::vector_similarity_index_cache_policy];
        size_t vector_similarity_index_cache_size = server_settings[ServerSetting::vector_similarity_index_cache_size];
        size_t vector_similarity_index_cache_max_count = server_settings[ServerSetting::vector_similarity_index_cache_max_entries];
        double vector_similarity_index_cache_size_ratio = server_settings[ServerSetting::vector_similarity_index_cache_size_ratio];
        LOG_INFO(log, "Lowered vector similarity index cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(vector_similarity_index_cache_size));

        global_context->setVectorSimilarityIndexCache(vector_similarity_index_cache_policy, vector_similarity_index_cache_size, vector_similarity_index_cache_max_count, vector_similarity_index_cache_size_ratio);

        getMergeTreePrefixesDeserializationThreadPool().initialize(
            server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_size],
            server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_free_size],
            server_settings[ServerSetting::prefixes_deserialization_thread_pool_thread_pool_queue_size]);

        size_t mmap_cache_size = config->getUInt64("mmap_cache_size", DEFAULT_MMAP_CACHE_MAX_SIZE);
        global_context->setMMappedFileCache(mmap_cache_size);

        /// Initialize a dummy query result cache.
        global_context->setQueryResultCache(0, 0, 0, 0);

        /// Initialize a dummy query condition cache.
        global_context->setQueryConditionCache(DEFAULT_QUERY_CONDITION_CACHE_POLICY, 0, 0);

        // We must set the application type to CLIENT to avoid ServerUUID::get() throw exception
        global_context->setApplicationType(Context::ApplicationType::CLIENT);
    }
    else
    {
        // just for ut
        global_context->updateStorageConfiguration(*config);
    }
}

void BackendInitializerUtil::applyGlobalConfigAndSettings(const DB::Context::ConfigurationPtr & config, const DB::Settings & settings)
{
    const auto global_context = QueryContext::globalMutableContext();
    global_context->setConfig(config);
    global_context->setSettings(settings);
}

extern void registerAggregateFunctionCombinatorPartialMerge(AggregateFunctionCombinatorFactory &);
extern void registerAggregateFunctionsBloomFilter(AggregateFunctionFactory &);
extern void registerAggregateFunctionSparkAvg(AggregateFunctionFactory &);
extern void registerAggregateFunctionRowNumGroup(AggregateFunctionFactory &);
extern void registerAggregateFunctionDVRoaringBitmap(AggregateFunctionFactory &);


extern void registerFunctions(FunctionFactory &);

void registerAllFunctions()
{
    DB::registerFunctions();
    DB::registerAggregateFunctions();

    auto & agg_factory = AggregateFunctionFactory::instance();
    registerAggregateFunctionsBloomFilter(agg_factory);
    registerAggregateFunctionSparkAvg(agg_factory);
    registerAggregateFunctionRowNumGroup(agg_factory);
    DB::registerAggregateFunctionUniqHyperLogLogPlusPlus(agg_factory);
    registerAggregateFunctionDVRoaringBitmap(agg_factory);

    /// register aggregate function combinators from local_engine
    auto & combinator_factory = AggregateFunctionCombinatorFactory::instance();
    registerAggregateFunctionCombinatorPartialMerge(combinator_factory);
}

void registerGlutenDisks()
{
    registerDisks(true);
    registerGlutenDisks(true);
}

void BackendInitializerUtil::registerAllFactories()
{
    registerFormats();

    registerGlutenDisks();

    registerReadBufferBuilders();
    registerWriteBufferBuilders();

    LOG_INFO(logger, "Register read buffer builders.");

    registerRelParsers();
    LOG_INFO(logger, "Register relation parsers.");

    registerAllFunctions();
    LOG_INFO(logger, "Register all functions.");
}

void BackendInitializerUtil::initCompiledExpressionCache(DB::Context::ConfigurationPtr config)
{
#if USE_EMBEDDED_COMPILER
    /// 128 MB
    constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
    size_t compiled_expression_cache_size = config->getUInt64("compiled_expression_cache_size", compiled_expression_cache_size_default);

    constexpr size_t compiled_expression_cache_elements_size_default = 10000;
    size_t compiled_expression_cache_elements_size
        = config->getUInt64("compiled_expression_cache_elements_size", compiled_expression_cache_elements_size_default);

    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size, compiled_expression_cache_elements_size);
#endif
}

void BackendInitializerUtil::initBackend(const SparkConfigs::ConfigMap & spark_conf_map)
{
    DB::Context::ConfigurationPtr config = initConfig(spark_conf_map);

    initLoggers(config);

    initEnvs(config);
    LOG_INFO(logger, "Init environment variables.");

    DB::Settings settings;
    initSettings(spark_conf_map, settings);
    LOG_INFO(logger, "Init settings.");

    initContexts(config);
    LOG_INFO(logger, "Init shared context and global context.");

    applyGlobalConfigAndSettings(config, settings);
    LOG_INFO(logger, "Apply configuration and setting for global context.");

    // clean static per_bucket_clients and shared_client before running local engine,
    // in case of running the multiple gluten ut in one process
    ReadBufferBuilderFactory::instance().clean();

    // Init the table metadata cache map
    StorageMergeTreeFactory::init_cache_map();

    JobScheduler::initialize(QueryContext::globalContext());
    CacheManager::initialize(QueryContext::globalMutableContext());

    std::call_once(
        init_flag,
        [&]
        {
            SignalHandler::instance().init();

            registerAllFactories();
            LOG_INFO(logger, "Register all factories.");

            initCompiledExpressionCache(config);
            LOG_INFO(logger, "Init compiled expressions cache factory.");

            ServerSettings server_settings;
            server_settings.loadSettingsFromConfig(*config);
            GlobalThreadPool::initialize(
                server_settings[ServerSetting::max_thread_pool_size], 0, server_settings[ServerSetting::thread_pool_queue_size]);
            getIOThreadPool().initialize(
                server_settings[ServerSetting::max_io_thread_pool_size], 0, server_settings[ServerSetting::io_thread_pool_queue_size]);

            const size_t active_parts_loading_threads = config->getUInt("max_active_parts_loading_thread_pool_size", 64);
            DB::getActivePartsLoadingThreadPool().initialize(
                active_parts_loading_threads,
                0, // We don't need any threads one all the parts will be loaded
                active_parts_loading_threads);

            const size_t cleanup_threads = config->getUInt("max_parts_cleaning_thread_pool_size", 128);
            getPartsCleaningThreadPool().initialize(
                cleanup_threads,
                0, // We don't need any threads one all the parts will be deleted
                cleanup_threads);

            // Avoid using LD_PRELOAD in child process
            unsetenv("LD_PRELOAD");
        });
}

void BackendFinalizerUtil::finalizeGlobally()
{
    // Make sure client caches release before ClientCacheRegistry
    ReadBufferBuilderFactory::instance().clean();
    StorageMergeTreeFactory::clear_cache_map();
    QueryContext::instance().reset();
    std::lock_guard lock(paths_mutex);
    std::ranges::for_each(
        paths_need_to_clean,
        [](const auto & path)
        {
            if (fs::exists(path))
                fs::remove_all(path);
        });
    paths_need_to_clean.clear();
}

void BackendFinalizerUtil::finalizeSessionally()
{
}

std::vector<String> BackendFinalizerUtil::paths_need_to_clean;

std::mutex BackendFinalizerUtil::paths_mutex;

Int64 DateTimeUtil::currentTimeMillis()
{
    return timeInMilliseconds(std::chrono::system_clock::now());
}

String DateTimeUtil::convertTimeZone(const String & time_zone)
{
    String res = time_zone;
    /// Convert timezone ID like '+08:00' to GMT+8:00
    if (time_zone.starts_with("+") || time_zone.starts_with("-"))
        res = "GMT" + time_zone;
    res = DateLUT::mappingForJavaTimezone(res);
    return res;
}

UInt64 MemoryUtil::getMemoryRSS()
{
    long rss = 0L;
    FILE * fp = NULL;
    char buf[4096];
    sprintf(buf, "/proc/%d/statm", getpid());
    if ((fp = fopen(buf, "r")) == NULL)
        return 0;
    fscanf(fp, "%*s%ld", &rss);
    fclose(fp);
    return rss * sysconf(_SC_PAGESIZE);
}

void JoinUtil::adjustJoinOutput(DB::QueryPlan & plan, DB::Names cols)
{
    auto header = plan.getCurrentHeader();
    std::unordered_map<String, const DB::ActionsDAG::Node *> name_to_node;
    ActionsDAG project;
    for (const auto & col : header->getColumnsWithTypeAndName())
    {
        const auto * node = &(project.addInput(col));
        name_to_node[col.name] = node;
    }
    for (const auto & col : cols)
    {
        const auto it = name_to_node.find(col);
        if (it == name_to_node.end())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Column {} not found in header", col);
        }
        project.addOrReplaceInOutputs(*(it->second));
    }
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(project));
    project_step->setStepDescription("Reorder Join Output");
    plan.addStep(std::move(project_step));
}

std::pair<DB::JoinKind, DB::JoinStrictness>
JoinUtil::getJoinKindAndStrictness(substrait::JoinRel_JoinType join_type, bool is_existence_join)
{
    switch (join_type)
    {
        case substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
            return {DB::JoinKind::Inner, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI: {
            if (is_existence_join)
                return {DB::JoinKind::Left, DB::JoinStrictness::Any};
            return {DB::JoinKind::Left, DB::JoinStrictness::Semi};
        }
        case substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
            return {DB::JoinKind::Right, DB::JoinStrictness::Semi};
        case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI:
            return {DB::JoinKind::Left, DB::JoinStrictness::Anti};
        case substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_ANTI:
            return {DB::JoinKind::Right, DB::JoinStrictness::Anti};
        case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
            return {DB::JoinKind::Left, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT:
            return {DB::JoinKind::Right, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_OUTER:
            return {DB::JoinKind::Full, DB::JoinStrictness::All};
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", magic_enum::enum_name(join_type));
    }
}

std::pair<DB::JoinKind, DB::JoinStrictness> JoinUtil::getCrossJoinKindAndStrictness(substrait::CrossRel_JoinType join_type)
{
    switch (join_type)
    {
        case substrait::CrossRel_JoinType_JOIN_TYPE_INNER:
            return {DB::JoinKind::Cross, DB::JoinStrictness::All};
        case substrait::CrossRel_JoinType_JOIN_TYPE_LEFT:
            return {DB::JoinKind::Left, DB::JoinStrictness::All};
        case substrait::CrossRel_JoinType_JOIN_TYPE_OUTER:
            return {DB::JoinKind::Full, DB::JoinStrictness::All};
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", magic_enum::enum_name(join_type));
    }
}

}

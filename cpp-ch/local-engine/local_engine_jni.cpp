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
#include <numeric>
#include <string>
#include <jni.h>

#include <Builder/SerializedPlanBuilder.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataTypes/DataTypeNullable.h>
#include <Join/BroadCastJoinBuilder.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/LocalExecutor.h>
#include <Parser/ParserContext.h>
#include <Parser/RelParsers/MergeTreeRelParser.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/RelParsers/WriteRelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Parser/SubstraitParserUtils.h>
#include <Shuffle/NativeSplitter.h>
#include <Shuffle/NativeWriterInMemory.h>
#include <Shuffle/PartitionWriter.h>
#include <Shuffle/ShuffleCommon.h>
#include <Shuffle/ShuffleReader.h>
#include <Shuffle/ShuffleWriter.h>
#include <Shuffle/SparkExchangeSink.h>
#include <Shuffle/WriteBufferFromJavaOutputStream.h>
#include <Storages/Cache/CacheManager.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <Storages/MergeTree/SparkMergeTreeWriteSettings.h>
#include <Storages/MergeTree/SparkMergeTreeWriter.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <Storages/Output/BlockStripeSplitter.h>
#include <Storages/Output/NormalFileWriter.h>
#include <Storages/SubstraitSource/Delta/DeltaWriter.h>
#include <jni/SharedPointerWrapper.h>
#include <jni/jni_common.h>
#include <jni/jni_error.h>
#include <write_optimization.pb.h>
#include <Poco/Logger.h>
#include <Poco/StringTokenizer.h>
#include <Common/CHUtil.h>
#include <Common/ErrorCodes.h>
#include <Common/ExceptionUtils.h>
#include <Common/JNIUtils.h>
#include <Common/QueryContext.h>

#ifdef __cplusplus
namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
extern const int UNKNOWN_EXCEPTION;
}
}
static DB::ColumnWithTypeAndName getColumnFromColumnVector(JNIEnv * /*env*/, jobject /*obj*/, jlong block_address, jint column_position)
{
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    return block->getByPosition(column_position);
}

static std::string jstring2string(JNIEnv * env, jstring string)
{
    if (string == nullptr)
        return std::string();
    const char * chars = env->GetStringUTFChars(string, nullptr);
    std::string ret(chars);
    env->ReleaseStringUTFChars(string, chars);
    return ret;
}

extern "C" {
#endif


namespace dbms
{
class LocalExecutor;
}

static jclass block_stripes_class;
static jmethodID block_stripes_constructor;

static jclass split_result_class;
static jmethodID split_result_constructor;

static jclass block_stats_class;
static jmethodID block_stats_constructor;

JNIEXPORT jint JNI_OnLoad(JavaVM * vm, void * /*reserved*/)
{
    JNIEnv * env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) != JNI_OK)
        return JNI_ERR;

    local_engine::JniErrorsGlobalState::instance().initialize(env);

    block_stripes_class = local_engine::CreateGlobalClassReference(env, "Lorg/apache/spark/sql/execution/datasources/BlockStripes;");
    block_stripes_constructor = local_engine::GetMethodID(env, block_stripes_class, "<init>", "(J[J[II)V");

    split_result_class = local_engine::CreateGlobalClassReference(env, "Lorg/apache/gluten/vectorized/CHSplitResult;");
    split_result_constructor = local_engine::GetMethodID(env, split_result_class, "<init>", "(JJJJJJ[J[JJJJJJJ)V");

    block_stats_class = local_engine::CreateGlobalClassReference(env, "Lorg/apache/gluten/vectorized/BlockStats;");
    block_stats_constructor = local_engine::GetMethodID(env, block_stats_class, "<init>", "(JZ)V");

    local_engine::ShuffleReader::shuffle_input_stream_class
        = local_engine::CreateGlobalClassReference(env, "Lorg/apache/gluten/vectorized/ShuffleInputStream;");
    local_engine::NativeSplitter::iterator_class
        = local_engine::CreateGlobalClassReference(env, "Lorg/apache/gluten/vectorized/IteratorWrapper;");
    local_engine::WriteBufferFromJavaOutputStream::output_stream_class
        = local_engine::CreateGlobalClassReference(env, "Ljava/io/OutputStream;");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class
        = local_engine::CreateGlobalClassReference(env, "Lorg/apache/gluten/execution/ColumnarNativeIterator;");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_hasNext
        = local_engine::GetMethodID(env, local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class, "hasNext", "()Z");
    local_engine::SourceFromJavaIter::serialized_record_batch_iterator_next
        = local_engine::GetMethodID(env, local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class, "next", "()[B");

    local_engine::ReadBufferFromJavaInputStream::input_stream_class
        = local_engine::CreateGlobalClassReference(env, "Ljava/io/InputStream;");
    local_engine::ReadBufferFromJavaInputStream::input_stream_read
        = local_engine::GetMethodID(env, local_engine::ReadBufferFromJavaInputStream::input_stream_class, "read", "([B)I");

    local_engine::ShuffleReader::shuffle_input_stream_read
        = local_engine::GetMethodID(env, local_engine::ShuffleReader::shuffle_input_stream_class, "read", "(JJ)J");

    local_engine::NativeSplitter::iterator_has_next
        = local_engine::GetMethodID(env, local_engine::NativeSplitter::iterator_class, "hasNext", "()Z");
    local_engine::NativeSplitter::iterator_next
        = local_engine::GetMethodID(env, local_engine::NativeSplitter::iterator_class, "next", "()J");

    local_engine::WriteBufferFromJavaOutputStream::output_stream_write
        = local_engine::GetMethodID(env, local_engine::WriteBufferFromJavaOutputStream::output_stream_class, "write", "([BII)V");
    local_engine::WriteBufferFromJavaOutputStream::output_stream_flush
        = local_engine::GetMethodID(env, local_engine::WriteBufferFromJavaOutputStream::output_stream_class, "flush", "()V");


    local_engine::SparkRowToCHColumn::spark_row_interator_class
        = local_engine::CreateGlobalClassReference(env, "Lorg/apache/gluten/execution/SparkRowIterator;");
    local_engine::SparkRowToCHColumn::spark_row_interator_hasNext
        = local_engine::GetMethodID(env, local_engine::SparkRowToCHColumn::spark_row_interator_class, "hasNext", "()Z");
    local_engine::SparkRowToCHColumn::spark_row_interator_next
        = local_engine::GetMethodID(env, local_engine::SparkRowToCHColumn::spark_row_interator_class, "next", "()[B");
    local_engine::SparkRowToCHColumn::spark_row_iterator_nextBatch = local_engine::GetMethodID(
        env, local_engine::SparkRowToCHColumn::spark_row_interator_class, "nextBatch", "()Ljava/nio/ByteBuffer;");

    local_engine::BroadCastJoinBuilder::init(env);
    local_engine::CacheManager::initJNI(env);
    local_engine::SparkMergeTreeWriterJNI::init(env);
    local_engine::SparkRowInfoJNI::init(env);

    local_engine::JNIUtils::vm = vm;

    local_engine::JniEnvStatusWatcher::instance().setActive();
    return JNI_VERSION_1_8;
}

JNIEXPORT void Java_org_apache_gluten_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(JNIEnv * env, jclass, jbyteArray conf_plan)
{
    LOCAL_ENGINE_JNI_METHOD_START
    // Ensure that current_thread is valid before initializing resource.
    DB::ThreadStatus thread_status;

    const auto conf_plan_a = local_engine::getByteArrayElementsSafe(env, conf_plan);
    const std::string::size_type plan_buf_size = conf_plan_a.length();
    local_engine::SparkConfigs::update(
        {reinterpret_cast<const char *>(conf_plan_a.elems()), plan_buf_size},
        [&](const local_engine::SparkConfigs::ConfigMap & spark_conf_map)
        { local_engine::BackendInitializerUtil::initBackend(spark_conf_map); },
        true);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_gluten_vectorized_ExpressionEvaluatorJniWrapper_nativeFinalizeNative(JNIEnv * env, jclass)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::BackendFinalizerUtil::finalizeSessionally();
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_gluten_vectorized_ExpressionEvaluatorJniWrapper_nativeDestroyNative(JNIEnv * env, jclass)
{
    LOCAL_ENGINE_JNI_DESTROY_METHOD_START
    {
        // Ensure that current_thread is valid before destroying resource to avoid
        // any future calls to CurrentThread::get() in parts of the code.
        // For example, see `MergeTreeData::getInMemoryMetadataPtr()`
        DB::ThreadStatus thread_status;
        local_engine::BackendFinalizerUtil::finalizeGlobally();

        local_engine::JniErrorsGlobalState::instance().destroy(env);
        local_engine::BroadCastJoinBuilder::destroy(env);
        local_engine::SparkMergeTreeWriterJNI::destroy(env);
        local_engine::SparkRowInfoJNI::destroy(env);

        env->DeleteGlobalRef(block_stripes_class);
        env->DeleteGlobalRef(split_result_class);
        env->DeleteGlobalRef(block_stats_class);
        env->DeleteGlobalRef(local_engine::ShuffleReader::shuffle_input_stream_class);
        env->DeleteGlobalRef(local_engine::NativeSplitter::iterator_class);
        env->DeleteGlobalRef(local_engine::WriteBufferFromJavaOutputStream::output_stream_class);
        env->DeleteGlobalRef(local_engine::SourceFromJavaIter::serialized_record_batch_iterator_class);
        env->DeleteGlobalRef(local_engine::SparkRowToCHColumn::spark_row_interator_class);
    }
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

/// Set settings for the current query. It assumes that all parameters are started with `CH_RUNTIME_SETTINGS_PREFIX` prefix,
/// and the prefix is removed by java before passing to C++.
JNIEXPORT void
Java_org_apache_gluten_vectorized_ExpressionEvaluatorJniWrapper_updateQueryRuntimeSettings(JNIEnv * env, jclass, jbyteArray settings)
{
    LOCAL_ENGINE_JNI_METHOD_START
    const auto query_context = local_engine::QueryContext::instance().currentQueryContext();

    const auto conf_plan_a = local_engine::getByteArrayElementsSafe(env, settings);
    const std::string::size_type conf_plan_size = conf_plan_a.length();
    local_engine::updateSettings(query_context, {reinterpret_cast<const char *>(conf_plan_a.elems()), conf_plan_size});

    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv * env,
    jclass,
    jbyteArray plan,
    jobjectArray split_infos,
    jobjectArray iter_arr,
    jbyteArray conf_plan,
    jboolean materialize_input,
    jint partition_index)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto query_context = local_engine::QueryContext::instance().currentQueryContext();

    // by task update new configs ( in case of dynamic config update )
    const auto conf_plan_a = local_engine::getByteArrayElementsSafe(env, conf_plan);
    const std::string::size_type conf_plan_size = conf_plan_a.length();
    local_engine::SparkConfigs::updateConfig(query_context, {reinterpret_cast<const char *>(conf_plan_a.elems()), conf_plan_size});

    const auto plan_a = local_engine::getByteArrayElementsSafe(env, plan);
    const std::string::size_type plan_size = plan_a.length();
    auto plan_pb = local_engine::BinaryToMessage<substrait::Plan>({reinterpret_cast<const char *>(plan_a.elems()), plan_size});

    auto parser_context = local_engine::ParserContext::build(query_context, plan_pb, partition_index);
    local_engine::SerializedPlanParser parser(parser_context);

    jsize iter_num = env->GetArrayLength(iter_arr);
    for (jsize i = 0; i < iter_num; i++)
    {
        jobject iter = env->GetObjectArrayElement(iter_arr, i);
        iter = env->NewGlobalRef(iter);
        parser.addInputIter(iter, materialize_input);
    }

    for (jsize i = 0, split_info_arr_size = env->GetArrayLength(split_infos); i < split_info_arr_size; i++)
    {
        jbyteArray split_info = static_cast<jbyteArray>(env->GetObjectArrayElement(split_infos, i));
        const auto split_info_a = local_engine::getByteArrayElementsSafe(env, split_info);
        const std::string::size_type split_info_size = split_info_a.length();
        parser.addSplitInfo({reinterpret_cast<const char *>(split_info_a.elems()), split_info_size});
    }

    local_engine::LocalExecutor * executor = parser.createExecutor(plan_pb).release();
    LOG_INFO(getLogger("jni"), "Construct LocalExecutor {}", reinterpret_cast<uintptr_t>(executor));
    executor->setMetric(parser.getMetric());
    executor->setExtraPlanHolder(parser.extra_plan_holder);
    local_engine::QueryContext::instance().attachLocalExecutor(executor);

    return reinterpret_cast<jlong>(executor);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

// Columnar Iterator
JNIEXPORT jboolean Java_org_apache_gluten_vectorized_BatchIterator_nativeHasNext(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_BatchIterator_nativeCHNext(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    DB::Block * column_batch = executor->nextColumnar();
    return reinterpret_cast<UInt64>(column_batch);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_BatchIterator_nativeCancel(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    executor->cancel();
    LOG_INFO(getLogger("jni"), "Cancel LocalExecutor {}", reinterpret_cast<uintptr_t>(executor));
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_gluten_vectorized_BatchIterator_nativeClose(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    LOG_INFO(getLogger("jni"), "Finalize LocalExecutor {}", reinterpret_cast<intptr_t>(executor));
    local_engine::LocalExecutor::resetCurrentExecutor();
    if (local_engine::QueryContext::instance().detachLocalExecutor(executor))
        delete executor;
    else
        LOG_WARNING(getLogger("jni"), "The LocalExecutor {} has been detached before, should not delete it again.", reinterpret_cast<intptr_t>(executor));
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jstring Java_org_apache_gluten_vectorized_BatchIterator_nativeFetchMetrics(JNIEnv * env, jobject /*obj*/, jlong executor_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    /// Collect metrics only if optimizations are disabled, otherwise coredump would happen.
    const local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    const auto metric = executor->getMetric();
    const String metrics_json = metric ? local_engine::RelMetricSerializer::serializeRelMetric(metric) : "";

    return local_engine::charTojstring(env, metrics_json.c_str());
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT jboolean
Java_org_apache_gluten_vectorized_CHColumnVector_nativeHasNull(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    if (!col.column->isNullable())
    {
        return false;
    }
    else
    {
        const auto * nullable = checkAndGetColumn<DB::ColumnNullable>(&*col.column);
        const auto & null_map_data = nullable->getNullMapData();
        return !DB::memoryIsZero(null_map_data.data(), 0, null_map_data.size());
    }
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jint
Java_org_apache_gluten_vectorized_CHColumnVector_nativeNumNulls(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    if (!col.column->isNullable())
    {
        return 0;
    }
    else
    {
        const auto * nullable = checkAndGetColumn<DB::ColumnNullable>(&*col.column);
        return std::accumulate(nullable->getNullMapData().begin(), nullable->getNullMapData().end(), 0);
    }
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jboolean Java_org_apache_gluten_vectorized_CHColumnVector_nativeIsNullAt(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    return col.column->isNullAt(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jboolean Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetBoolean(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getBool(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jbyte Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetByte(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return reinterpret_cast<const jbyte *>(nested_col->getDataAt(row_id).data())[0];
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT jshort Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetShort(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return reinterpret_cast<const jshort *>(nested_col->getDataAt(row_id).data())[0];
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jint Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetInt(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    if (col.type->getTypeId() == DB::TypeIndex::Date)
        return nested_col->getUInt(row_id);
    else
        return nested_col->getInt(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetLong(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getInt(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jfloat Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetFloat(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getFloat32(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0.0)
}

JNIEXPORT jdouble Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetDouble(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    return nested_col->getFloat64(row_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0.0)
}

JNIEXPORT jstring Java_org_apache_gluten_vectorized_CHColumnVector_nativeGetString(
    JNIEnv * env, jobject obj, jint row_id, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    DB::ColumnPtr nested_col = col.column;
    if (const auto * nullable_col = checkAndGetColumn<DB::ColumnNullable>(nested_col.get()))
        nested_col = nullable_col->getNestedColumnPtr();
    const auto * string_col = checkAndGetColumn<DB::ColumnString>(nested_col.get());
    auto result = string_col->getDataAt(row_id);
    return local_engine::charTojstring(env, result.data());
    LOCAL_ENGINE_JNI_METHOD_END(env, local_engine::charTojstring(env, ""))
}

// native block
JNIEXPORT void Java_org_apache_gluten_vectorized_CHNativeBlock_nativeClose(JNIEnv * /*env*/, jobject /*obj*/, jlong /*block_address*/)
{
}

JNIEXPORT jint Java_org_apache_gluten_vectorized_CHNativeBlock_nativeNumRows(JNIEnv * env, jobject /*obj*/, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    return block->rows();
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jint Java_org_apache_gluten_vectorized_CHNativeBlock_nativeNumColumns(JNIEnv * env, jobject /*obj*/, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    return block->columns();
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jbyteArray
Java_org_apache_gluten_vectorized_CHNativeBlock_nativeColumnType(JNIEnv * env, jobject /*obj*/, jlong block_address, jint position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    const auto & col = block->getByPosition(position);
    std::string substrait_type;
    dbms::SerializedPlanBuilder::buildType(col.type, substrait_type);
    return local_engine::stringTojbyteArray(env, substrait_type);
    LOCAL_ENGINE_JNI_METHOD_END(env, local_engine::stringTojbyteArray(env, ""))
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHNativeBlock_nativeTotalBytes(JNIEnv * env, jobject /*obj*/, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    return block->bytes();
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jobject
Java_org_apache_gluten_vectorized_CHNativeBlock_nativeBlockStats(JNIEnv * env, jobject obj, jlong block_address, jint column_position)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    auto col = getColumnFromColumnVector(env, obj, block_address, column_position);
    if (!col.column->isNullable())
    {
        jobject block_stats = env->NewObject(block_stats_class, block_stats_constructor, block->rows(), false);
        return block_stats;
    }
    else
    {
        const auto * nullable = checkAndGetColumn<DB::ColumnNullable>(&*col.column);
        const auto & null_map_data = nullable->getNullMapData();

        jobject block_stats = env->NewObject(
            block_stats_class, block_stats_constructor, block->rows(), !DB::memoryIsZero(null_map_data.data(), 0, null_map_data.size()));
        return block_stats;
    }
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT jlong
Java_org_apache_gluten_vectorized_CHNativeBlock_copyBlock(JNIEnv * env, jobject obj, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);

    auto copied_block = block->cloneWithColumns(block->getColumns());
    auto * a = new DB::Block(std::move(copied_block));
    return reinterpret_cast<jlong>(a);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong
Java_org_apache_gluten_vectorized_CHNativeBlock_nativeSlice(JNIEnv * env, jobject /* obj */, jlong block_address, jint offset, jint limit)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    DB::Block cut_block = block->cloneWithCutColumns(offset, limit);

    return reinterpret_cast<jlong>(new DB::Block(std::move(cut_block)));
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHStreamReader_createNativeShuffleReader(
    JNIEnv * env, jclass /*clazz*/, jobject input_stream, jboolean compressed, jlong max_shuffle_read_rows, jlong max_shuffle_read_bytes)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * input = env->NewGlobalRef(input_stream);
    auto read_buffer = std::make_unique<local_engine::ReadBufferFromJavaShuffleInputStream>(input);
    auto * shuffle_reader
        = new local_engine::ShuffleReader(std::move(read_buffer), compressed, max_shuffle_read_rows, max_shuffle_read_bytes);
    return reinterpret_cast<jlong>(shuffle_reader);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHStreamReader_nativeNext(JNIEnv * env, jobject /*obj*/, jlong shuffle_reader)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleReader * reader = reinterpret_cast<local_engine::ShuffleReader *>(shuffle_reader);
    DB::Block * block = reader->read();
    return reinterpret_cast<jlong>(block);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHStreamReader_directRead(
    JNIEnv * env, jclass /*clazz*/, jobject input_stream, jbyteArray buffer, jint buffer_size)
{
    LOCAL_ENGINE_JNI_METHOD_START
    // auto * input = env->NewGlobalRef(input_stream);
    auto rb = std::make_unique<local_engine::ReadBufferFromJavaInputStream>(input_stream, buffer, buffer_size);
    auto reader = std::make_unique<local_engine::NativeReader>(*rb);
    DB::Block block = reader->read();
    DB::Block * res = new DB::Block(block);
    return reinterpret_cast<jlong>(res);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_CHStreamReader_nativeClose(JNIEnv * env, jobject /*obj*/, jlong shuffle_reader)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleReader * reader = reinterpret_cast<local_engine::ShuffleReader *>(shuffle_reader);
    delete reader;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

local_engine::SplitterHolder * buildAndExecuteShuffle(
    JNIEnv * env, jobject iter, const String & name, const local_engine::SplitOptions & options, jobject rss_pusher = nullptr)
{
    auto current_executor = local_engine::LocalExecutor::getCurrentExecutor();
    local_engine::SplitterHolder * splitter = nullptr;
    // There are two modes of fallback, one is full fallback but uses columnar shuffle,
    // and the other is partial fallback that creates one or more LocalExecutor.
    // In full fallback, the current executor does not exist.
    if (!current_executor.has_value() || current_executor.value()->fallbackMode())
    {
        auto first_block = local_engine::SourceFromJavaIter::peekBlock(env, iter);
        if (first_block.has_value())
        {
            /// Try to decide header from the first block read from Java iterator.
            auto header = first_block.value().cloneEmpty();
            splitter = new local_engine::SplitterHolder{
                .exchange_manager = std::make_unique<local_engine::SparkExchangeManager>(header, name, options, rss_pusher)};
            splitter->exchange_manager->initSinks(1);
            splitter->exchange_manager->pushBlock(first_block.value());
            first_block = std::nullopt;
            // in fallback mode, spark's whole stage code gen operator uses TaskContext and needs to be executed in the task thread.
            while (auto block = local_engine::SourceFromJavaIter::peekBlock(env, iter))
                splitter->exchange_manager->pushBlock(block.value());
        }
        else
            // empty iterator
            splitter = new local_engine::SplitterHolder{
                .exchange_manager = std::make_unique<local_engine::SparkExchangeManager>(DB::Block(), name, options, rss_pusher)};
    }
    else
    {
        splitter = new local_engine::SplitterHolder{
            .exchange_manager = std::make_unique<local_engine::SparkExchangeManager>(
                current_executor.value()->getHeader().cloneEmpty(), name, options, rss_pusher)};
        // TODO support multiple sinks
        splitter->exchange_manager->initSinks(1);
        current_executor.value()->setSinks([&](auto & pipeline_builder)
                                           { splitter->exchange_manager->setSinksToPipeline(pipeline_builder); });
        // execute pipeline
        current_executor.value()->execute();
    }
    return splitter;
}

// Splitter Jni Wrapper
JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHShuffleSplitterJniWrapper_nativeMake(
    JNIEnv * env,
    jobject,
    jobject iter,
    jstring short_name,
    jint num_partitions,
    jbyteArray expr_list,
    jbyteArray out_expr_list,
    jint shuffle_id,
    jlong map_id,
    jint split_size,
    jstring codec,
    jint compress_level,
    jstring data_file,
    jstring local_dirs,
    jint num_sub_dirs,
    jlong spill_threshold,
    jstring hash_algorithm,
    jlong max_sort_buffer_size,
    jboolean force_memory_sort)
{
    LOCAL_ENGINE_JNI_METHOD_START
    std::string hash_exprs;
    std::string out_exprs;
    if (expr_list != nullptr)
    {
        const auto expr_list_a = local_engine::getByteArrayElementsSafe(env, expr_list);
        const std::string::size_type expr_list_size = expr_list_a.length();
        hash_exprs = std::string{reinterpret_cast<const char *>(expr_list_a.elems()), expr_list_size};
    }

    if (out_expr_list != nullptr)
    {
        const auto out_expr_list_a = local_engine::getByteArrayElementsSafe(env, out_expr_list);
        const std::string::size_type out_expr_list_size = out_expr_list_a.length();
        out_exprs = std::string{reinterpret_cast<const char *>(out_expr_list_a.elems()), out_expr_list_size};
    }

    Poco::StringTokenizer local_dirs_tokenizer(jstring2string(env, local_dirs), ",");
    std::vector<std::string> local_dirs_list;
    local_dirs_list.insert(local_dirs_list.end(), local_dirs_tokenizer.begin(), local_dirs_tokenizer.end());

    local_engine::SplitOptions options{
        .split_size = static_cast<size_t>(split_size),
        .io_buffer_size = DB::DBMS_DEFAULT_BUFFER_SIZE,
        .data_file = jstring2string(env, data_file),
        .local_dirs_list = std::move(local_dirs_list),
        .num_sub_dirs = num_sub_dirs,
        .shuffle_id = shuffle_id,
        .map_id = static_cast<int>(map_id),
        .partition_num = static_cast<size_t>(num_partitions),
        .hash_exprs = hash_exprs,
        .out_exprs = out_exprs,
        .compress_method = jstring2string(env, codec),
        .compress_level = compress_level < 0 ? std::nullopt : std::optional<int>(compress_level),
        .spill_threshold = static_cast<size_t>(spill_threshold),
        .hash_algorithm = jstring2string(env, hash_algorithm),
        .max_sort_buffer_size = static_cast<size_t>(max_sort_buffer_size),
        .force_memory_sort = static_cast<bool>(force_memory_sort)};
    auto name = jstring2string(env, short_name);

    return reinterpret_cast<jlong>(buildAndExecuteShuffle(env, iter, name, options));
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHShuffleSplitterJniWrapper_nativeMakeForRSS(
    JNIEnv * env,
    jobject,
    jobject iter,
    jstring short_name,
    jint num_partitions,
    jbyteArray expr_list,
    jbyteArray out_expr_list,
    jint shuffle_id,
    jlong map_id,
    jint split_size,
    jstring codec,
    jint compress_level,
    jlong spill_threshold,
    jstring hash_algorithm,
    jobject pusher,
    jboolean force_memory_sort)
{
    LOCAL_ENGINE_JNI_METHOD_START
    std::string hash_exprs;
    std::string out_exprs;
    if (expr_list != nullptr)
    {
        const auto expr_list_a = local_engine::getByteArrayElementsSafe(env, expr_list);
        const std::string::size_type expr_list_size = expr_list_a.length();
        hash_exprs = std::string{reinterpret_cast<const char *>(expr_list_a.elems()), expr_list_size};
    }

    if (out_expr_list != nullptr)
    {
        const auto out_expr_list_a = local_engine::getByteArrayElementsSafe(env, out_expr_list);
        const std::string::size_type out_expr_list_size = out_expr_list_a.length();
        out_exprs = std::string{reinterpret_cast<const char *>(out_expr_list_a.elems()), out_expr_list_size};
    }

    local_engine::SplitOptions options{
        .split_size = static_cast<size_t>(split_size),
        .io_buffer_size = DB::DBMS_DEFAULT_BUFFER_SIZE,
        .shuffle_id = shuffle_id,
        .map_id = static_cast<int>(map_id),
        .partition_num = static_cast<size_t>(num_partitions),
        .hash_exprs = hash_exprs,
        .out_exprs = out_exprs,
        .compress_method = jstring2string(env, codec),
        .compress_level = compress_level < 0 ? std::nullopt : std::optional<int>(compress_level),
        .spill_threshold = static_cast<size_t>(spill_threshold),
        .hash_algorithm = jstring2string(env, hash_algorithm),
        .force_memory_sort = static_cast<bool>(force_memory_sort)};
    auto name = jstring2string(env, short_name);
    return reinterpret_cast<jlong>(buildAndExecuteShuffle(env, iter, name, options, pusher));
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jobject Java_org_apache_gluten_vectorized_CHShuffleSplitterJniWrapper_stop(JNIEnv * env, jobject, jlong splitterId)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
    splitter->exchange_manager->finish();
    auto result = splitter->exchange_manager->getSplitResult();

    const auto & partition_lengths = result.partition_lengths;
    auto * partition_length_arr = env->NewLongArray(partition_lengths.size());
    const auto * src = reinterpret_cast<const jlong *>(partition_lengths.data());
    env->SetLongArrayRegion(partition_length_arr, 0, partition_lengths.size(), src);

    const auto & raw_partition_lengths = result.raw_partition_lengths;
    auto * raw_partition_length_arr = env->NewLongArray(raw_partition_lengths.size());
    const auto * raw_src = reinterpret_cast<const jlong *>(raw_partition_lengths.data());
    env->SetLongArrayRegion(raw_partition_length_arr, 0, raw_partition_lengths.size(), raw_src);

    // AQE has dependency on total_bytes_written, if the data is wrong, it will generate inappropriate plan
    // add a log here for remining this.
    if (result.total_rows && !result.total_bytes_written)
        LOG_WARNING(getLogger("CHShuffleSplitterJniWrapper"), "total_bytes_written is 0, something may be wrong");

    jobject split_result = env->NewObject(
        split_result_class,
        split_result_constructor,
        result.total_compute_pid_time,
        result.total_write_time,
        result.total_spill_time,
        result.total_compress_time,
        result.total_bytes_written,
        result.total_bytes_spilled,
        partition_length_arr,
        raw_partition_length_arr,
        result.total_split_time,
        result.total_io_time,
        result.total_serialize_time,
        result.total_rows,
        result.total_blocks,
        result.wall_time);

    return split_result;
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_CHShuffleSplitterJniWrapper_close(JNIEnv * env, jobject, jlong splitterId)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::SplitterHolder * splitter = reinterpret_cast<local_engine::SplitterHolder *>(splitterId);
    delete splitter;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

// CHBlockConverterJniWrapper
JNIEXPORT jobject Java_org_apache_gluten_vectorized_CHBlockConverterJniWrapper_convertColumnarToRow(
    JNIEnv * env, jclass, jlong block_address, jintArray masks)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::MaskVector mask = nullptr;
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    if (masks != nullptr)
    {
        auto safeArray = local_engine::getIntArrayElementsSafe(env, masks);
        mask = std::make_unique<std::vector<size_t>>();
        for (int j = 0; j < safeArray.length(); j++)
            mask->push_back(safeArray.elems()[j]);
    }

    local_engine::CHColumnToSparkRow converter;
    std::unique_ptr<local_engine::SparkRowInfo> spark_row_info = converter.convertCHColumnToSparkRow(*block, mask);
    return local_engine::SparkRowInfoJNI::create(env, *spark_row_info);
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_CHBlockConverterJniWrapper_freeMemory(JNIEnv * env, jclass, jlong address, jlong size)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::CHColumnToSparkRow converter;
    converter.freeMem(reinterpret_cast<char *>(address), size);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHBlockConverterJniWrapper_convertSparkRowsToCHColumn(
    JNIEnv * env, jclass, jobject java_iter, jobjectArray names, jobjectArray types)
{
    LOCAL_ENGINE_JNI_METHOD_START
    using namespace std;

    int num_columns = env->GetArrayLength(names);
    vector<string> c_names;
    vector<string> c_types;
    c_names.reserve(num_columns);
    for (int i = 0; i < num_columns; i++)
    {
        auto * name = static_cast<jstring>(env->GetObjectArrayElement(names, i));
        c_names.emplace_back(jstring2string(env, name));

        auto * type = static_cast<jbyteArray>(env->GetObjectArrayElement(types, i));
        auto type_length = env->GetArrayLength(type);
        jbyte * type_ptr = env->GetByteArrayElements(type, nullptr);
        string str_type(reinterpret_cast<const char *>(type_ptr), type_length);
        c_types.emplace_back(std::move(str_type));

        env->ReleaseByteArrayElements(type, type_ptr, JNI_ABORT);
        env->DeleteLocalRef(name);
        env->DeleteLocalRef(type);
    }
    auto * block = local_engine::SparkRowToCHColumn::convertSparkRowItrToCHColumn(java_iter, c_names, c_types);
    return reinterpret_cast<jlong>(block);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_CHBlockConverterJniWrapper_freeBlock(JNIEnv * env, jclass, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::SparkRowToCHColumn::freeBlock(reinterpret_cast<DB::Block *>(block_address));
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_CHBlockWriterJniWrapper_nativeCreateInstance(JNIEnv * env, jobject)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = new local_engine::NativeWriterInMemory();
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void
Java_org_apache_gluten_vectorized_CHBlockWriterJniWrapper_nativeWrite(JNIEnv * env, jobject, jlong instance, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    writer->write(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jint Java_org_apache_gluten_vectorized_CHBlockWriterJniWrapper_nativeResultSize(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    return static_cast<jint>(writer->collect().size());
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void
Java_org_apache_gluten_vectorized_CHBlockWriterJniWrapper_nativeCollect(JNIEnv * env, jobject, jlong instance, jbyteArray result)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    auto data = writer->collect();
    env->SetByteArrayRegion(result, 0, data.size(), reinterpret_cast<const jbyte *>(data.data()));
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_gluten_vectorized_CHBlockWriterJniWrapper_nativeClose(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeWriterInMemory *>(instance);
    delete writer;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_createFilerWriter(
    JNIEnv * env, jobject, jstring file_uri_, jbyteArray writeRel)
{
    LOCAL_ENGINE_JNI_METHOD_START

    const auto writeRelBytes = local_engine::getByteArrayElementsSafe(env, writeRel);
    substrait::WriteRel write_rel = local_engine::BinaryToMessage<substrait::WriteRel>(
        {reinterpret_cast<const char *>(writeRelBytes.elems()), static_cast<size_t>(writeRelBytes.length())});

    assert(write_rel.has_named_table());
    const substrait::NamedObjectWrite & named_table = write_rel.named_table();
    local_engine::Write write_opt;
    named_table.advanced_extension().optimization().UnpackTo(&write_opt);
    DB::Block preferred_schema = local_engine::TypeParser::buildBlockFromNamedStructWithoutDFS(write_rel.table_schema());

    const auto file_uri = jstring2string(env, file_uri_);

    // for HiveFileFormat, the file url may not end with .parquet, so we pass in the format as a hint
    const auto context = local_engine::QueryContext::instance().currentQueryContext();
    auto * writer = local_engine::NormalFileWriter::create(context, file_uri, preferred_schema, write_opt.common().format()).release();
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT jlong Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_createMergeTreeWriter(
    JNIEnv * env, jobject, jbyteArray writeRel, jbyteArray conf_plan)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto query_context = local_engine::QueryContext::instance().currentQueryContext();
    // by task update new configs (in case of dynamic config update)
    const auto conf_plan_a = local_engine::getByteArrayElementsSafe(env, conf_plan);
    local_engine::SparkConfigs::updateConfig(
        query_context, {reinterpret_cast<const char *>(conf_plan_a.elems()), static_cast<size_t>(conf_plan_a.length())});

    const auto writeRelBytes = local_engine::getByteArrayElementsSafe(env, writeRel);
    substrait::WriteRel write_rel = local_engine::BinaryToMessage<substrait::WriteRel>(
        {reinterpret_cast<const char *>(writeRelBytes.elems()), static_cast<size_t>(writeRelBytes.length())});

    assert(write_rel.has_named_table());
    const substrait::NamedObjectWrite & named_table = write_rel.named_table();
    local_engine::Write write;
    if (!named_table.advanced_extension().optimization().UnpackTo(&write))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to unpack write optimization with local_engine::Write.");
    assert(write.has_common());
    assert(write.has_mergetree());
    local_engine::MergeTreeTable merge_tree_table(write, write_rel.table_schema());
    const std::string & id = write.common().job_task_attempt_id();

    return reinterpret_cast<jlong>(local_engine::SparkMergeTreeWriter::create(merge_tree_table, query_context, id).release());
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}


JNIEXPORT jstring Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_filterRangesOnDriver(
    JNIEnv * env, jclass, jbyteArray plan_, jbyteArray read_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    // Ensure that current_thread is valid, even if this function is called on Driver.
    DB::ThreadStatus thread_status;

    const auto plan_a = local_engine::getByteArrayElementsSafe(env, plan_);
    auto plan_pb = local_engine::BinaryToMessage<substrait::Plan>(
        {reinterpret_cast<const char *>(plan_a.elems()), static_cast<size_t>(plan_a.length())});

    auto parser_context = local_engine::ParserContext::build(local_engine::QueryContext::globalContext(), plan_pb);
    local_engine::SerializedPlanParser parser(parser_context);

    const auto read_a = local_engine::getByteArrayElementsSafe(env, read_);
    auto read_pb = local_engine::BinaryToMessage<substrait::Rel>(
        {reinterpret_cast<const char *>(read_a.elems()), static_cast<size_t>(read_a.length())});

    local_engine::MergeTreeRelParser mergeTreeParser(parser_context, local_engine::QueryContext::globalContext());
    auto res = mergeTreeParser.filterRangesOnDriver(read_pb.read());

    return local_engine::charTojstring(env, res.c_str());
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT void
Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_write(JNIEnv * env, jobject, jlong instanceId, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START

    auto * writer = reinterpret_cast<local_engine::NativeOutputWriter *>(instanceId);
    auto * block = reinterpret_cast<DB::Block *>(block_address);
    writer->write(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_close(JNIEnv * env, jobject, jlong instanceId)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::NativeOutputWriter *>(instanceId);
    SCOPE_EXIT({ delete writer; });
    writer->close();
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jstring Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_nativeMergeMTParts(
    JNIEnv * env, jclass, jbyteArray split_info_, jstring partition_dir_, jstring bucket_dir_)
{
    LOCAL_ENGINE_JNI_METHOD_START

    const auto uuid_str = toString(DB::UUIDHelpers::generateV4());
    const auto partition_dir = jstring2string(env, partition_dir_);
    const auto bucket_dir = jstring2string(env, bucket_dir_);

    const auto split_info_a = local_engine::getByteArrayElementsSafe(env, split_info_);
    auto extension_table = local_engine::BinaryToMessage<substrait::ReadRel::ExtensionTable>(
        {reinterpret_cast<const char *>(split_info_a.elems()), static_cast<size_t>(split_info_a.length())});

    local_engine::MergeTreeTableInstance merge_tree_table(extension_table);
    auto context = local_engine::QueryContext::instance().currentQueryContext();
    // each task, using its own CustomStorageMergeTree, doesn't reuse
    auto temp_storage = merge_tree_table.copyToVirtualStorage(context);
    // prefetch all needed parts metadata before merge
    local_engine::restoreMetaData(temp_storage, merge_tree_table, *context);

    // to release temp CustomStorageMergeTree with RAII
    DB::StorageID storage_id = temp_storage->getStorageID();
    SCOPE_EXIT({ local_engine::StorageMergeTreeFactory::freeStorage(storage_id); });

    std::vector<DB::DataPartPtr> selected_parts
        = local_engine::StorageMergeTreeFactory::getDataPartsByNames(temp_storage->getStorageID(), "", merge_tree_table.getPartNames());

    DB::MergeTreeDataPartPtr loaded = local_engine::mergeParts(selected_parts, uuid_str, *temp_storage, partition_dir, bucket_dir);

    std::vector<local_engine::PartInfo> res;

    saveFileStatus(*temp_storage, context, loaded->name, const_cast<DB::IDataPartStorage &>(loaded->getDataPartStorage()));
    res.emplace_back(local_engine::PartInfo{
        loaded->name, loaded->getMarksCount(), loaded->getBytesOnDisk(), loaded->rows_count, partition_dir, bucket_dir});

    auto json_info = local_engine::PartInfo::toJson(res);
    return local_engine::charTojstring(env, json_info.c_str());
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}


JNIEXPORT jobject Java_org_apache_spark_sql_execution_datasources_CHDatasourceJniWrapper_splitBlockByPartitionAndBucket(
    JNIEnv * env, jclass, jlong blockAddress, jintArray partitionColIndice, jboolean hasBucket)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * block = reinterpret_cast<DB::Block *>(blockAddress);

    auto safeArray = local_engine::getIntArrayElementsSafe(env, partitionColIndice);
    std::vector<size_t> partition_col_indice_vec;
    for (int i = 0; i < safeArray.length(); ++i)
        partition_col_indice_vec.push_back(safeArray.elems()[i]);

    auto query_context = local_engine::QueryContext::instance().currentQueryContext();
    const DB::Settings & settings = query_context->getSettingsRef();

    bool reserve_ = local_engine::settingsEqual(settings, "gluten.write.reserve_partition_columns", "true");

    local_engine::BlockStripes bs = local_engine::BlockStripeSplitter::split(*block, partition_col_indice_vec, hasBucket, reserve_);

    auto * addresses = env->NewLongArray(bs.block_addresses.size());
    env->SetLongArrayRegion(addresses, 0, bs.block_addresses.size(), reinterpret_cast<const jlong *>(bs.block_addresses.data()));
    auto * indices = env->NewIntArray(bs.heading_row_indice.size());
    env->SetIntArrayRegion(indices, 0, bs.heading_row_indice.size(), bs.heading_row_indice.data());

    jobject block_stripes = env->NewObject(
        block_stripes_class, block_stripes_constructor, bs.origin_block_address, addresses, indices, bs.origin_block_num_columns);
    return block_stripes;

    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_StorageJoinBuilder_nativeBuild(
    JNIEnv * env,
    jclass,
    jstring key,
    jbyteArray in,
    jlong row_count_,
    jstring join_key_,
    jint join_type_,
    jboolean has_mixed_join_condition,
    jboolean is_existence_join,
    jbyteArray named_struct,
    jboolean is_null_aware_anti_join,
    jboolean has_null_key_values)
{
    LOCAL_ENGINE_JNI_METHOD_START
    const auto hash_table_id = jstring2string(env, key);
    const auto join_key = jstring2string(env, join_key_);
    const auto named_struct_a = local_engine::getByteArrayElementsSafe(env, named_struct);
    const std::string::size_type struct_size = named_struct_a.length();
    std::string struct_string{reinterpret_cast<const char *>(named_struct_a.elems()), struct_size};
    const jsize length = env->GetArrayLength(in);
    local_engine::ReadBufferFromByteArray read_buffer_from_java_array(in, length);
    DB::CompressedReadBuffer input(read_buffer_from_java_array);
    local_engine::configureCompressedReadBuffer(input);
    const auto * obj = make_wrapper(local_engine::BroadCastJoinBuilder::buildJoin(
        hash_table_id,
        input,
        row_count_,
        join_key,
        join_type_,
        has_mixed_join_condition,
        is_existence_join,
        struct_string,
        is_null_aware_anti_join,
        has_null_key_values));
    return obj->instance();
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_StorageJoinBuilder_nativeCloneBuildHashTable(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * cloned
        = local_engine::make_wrapper(local_engine::SharedPointerWrapper<local_engine::StorageJoinFromReadBuffer>::sharedPtr(instance));
    return cloned->instance();
    LOCAL_ENGINE_JNI_METHOD_END(env, 0)
}

JNIEXPORT void
Java_org_apache_gluten_vectorized_StorageJoinBuilder_nativeCleanBuildHashTable(JNIEnv * env, jclass, jstring hash_table_id_, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto hash_table_id = jstring2string(env, hash_table_id_);
    local_engine::BroadCastJoinBuilder::cleanBuildHashTable(hash_table_id, instance);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

// BlockSplitIterator
JNIEXPORT jlong Java_org_apache_gluten_vectorized_BlockSplitIterator_nativeCreate(
    JNIEnv * env,
    jobject,
    jobject in,
    jstring name,
    jstring expr,
    jstring schema,
    jint partition_num,
    jint buffer_size,
    jstring hash_algorithm)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Options options;
    options.partition_num = partition_num;
    options.buffer_size = buffer_size;
    auto hash_algorithm_str = jstring2string(env, hash_algorithm);
    options.hash_algorithm.swap(hash_algorithm_str);
    auto expr_str = jstring2string(env, expr);
    std::string schema_str;
    if (schema)
        schema_str = jstring2string(env, schema);
    options.exprs_buffer.swap(expr_str);
    options.schema_buffer.swap(schema_str);
    local_engine::NativeSplitter::Holder * splitter = new local_engine::NativeSplitter::Holder{
        .splitter = local_engine::NativeSplitter::create(jstring2string(env, name), options, in)};
    return reinterpret_cast<jlong>(splitter);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_BlockSplitIterator_nativeClose(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    delete splitter;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jboolean Java_org_apache_gluten_vectorized_BlockSplitIterator_nativeHasNext(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    return splitter->splitter->hasNext();
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_BlockSplitIterator_nativeNext(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    return reinterpret_cast<jlong>(splitter->splitter->next());
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jint Java_org_apache_gluten_vectorized_BlockSplitIterator_nativeNextPartitionId(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::NativeSplitter::Holder * splitter = reinterpret_cast<local_engine::NativeSplitter::Holder *>(instance);
    return reinterpret_cast<jint>(splitter->splitter->nextPartitionId());
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_BlockOutputStream_nativeCreate(
    JNIEnv * env,
    jobject,
    jobject output_stream,
    jbyteArray buffer,
    jstring codec,
    jint level,
    jboolean compressed,
    jint customize_buffer_size)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer
        = new local_engine::ShuffleWriter(output_stream, buffer, jstring2string(env, codec), level, compressed, customize_buffer_size);
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT long Java_org_apache_gluten_vectorized_BlockOutputStream_directWrite(
    JNIEnv * env,
    jclass,
    jobject output_stream,
    jbyteArray buffer,
    jint customize_buffer_size,
    jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    auto wb = std::make_shared<local_engine::WriteBufferFromJavaOutputStream>(output_stream, buffer, customize_buffer_size);
    auto native_writer = std::make_unique<local_engine::NativeWriter>(*wb, block->cloneEmpty());
    auto write_size = native_writer->write(*block);
    native_writer->flush();
    wb->finalize();
    return write_size;
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_BlockOutputStream_nativeClose(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer = reinterpret_cast<local_engine::ShuffleWriter *>(instance);
    writer->flush();
    delete writer;
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_gluten_vectorized_BlockOutputStream_nativeWrite(JNIEnv * env, jobject, jlong instance, jlong block_address)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer = reinterpret_cast<local_engine::ShuffleWriter *>(instance);
    DB::Block * block = reinterpret_cast<DB::Block *>(block_address);
    writer->write(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT void Java_org_apache_gluten_vectorized_BlockOutputStream_nativeFlush(JNIEnv * env, jobject, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::ShuffleWriter * writer = reinterpret_cast<local_engine::ShuffleWriter *>(instance);
    writer->flush();
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jlong
Java_org_apache_gluten_vectorized_SimpleExpressionEval_createNativeInstance(JNIEnv * env, jclass, jobject input, jbyteArray plan)
{
    LOCAL_ENGINE_JNI_METHOD_START
    const auto plan_a = local_engine::getByteArrayElementsSafe(env, plan);
    const std::string::size_type plan_size = plan_a.length();
    auto plan_pb = local_engine::BinaryToMessage<substrait::Plan>({reinterpret_cast<const char *>(plan_a.elems()), plan_size});

    auto parser_context = local_engine::ParserContext::build(local_engine::QueryContext::globalContext(), plan_pb);
    local_engine::SerializedPlanParser parser(parser_context);

    const jobject iter = env->NewGlobalRef(input);
    parser.addInputIter(iter, false);
    local_engine::LocalExecutor * executor = parser.createExecutor(plan_pb).release();
    local_engine::QueryContext::instance().attachLocalExecutor(executor);
    return reinterpret_cast<jlong>(executor);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT void Java_org_apache_gluten_vectorized_SimpleExpressionEval_nativeClose(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(instance);
    if (local_engine::QueryContext::instance().detachLocalExecutor(executor))
        delete executor;
    else
        LOG_WARNING(getLogger("jni"), "The LocalExecutor {} has been detached before, should not delete it again.", reinterpret_cast<intptr_t>(executor));
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

JNIEXPORT jboolean Java_org_apache_gluten_vectorized_SimpleExpressionEval_nativeHasNext(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(instance);
    return executor->hasNext();
    LOCAL_ENGINE_JNI_METHOD_END(env, false)
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_SimpleExpressionEval_nativeNext(JNIEnv * env, jclass, jlong instance)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(instance);
    return reinterpret_cast<jlong>(executor->nextColumnar());
    LOCAL_ENGINE_JNI_METHOD_END(env, -1)
}

JNIEXPORT jlong Java_org_apache_gluten_memory_CHThreadGroup_createThreadGroup(JNIEnv * env, jclass, jstring task_id_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto task_id = jstring2string(env, task_id_);
    return local_engine::QueryContext::instance().initializeQuery(task_id);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0l)
}

JNIEXPORT jlong Java_org_apache_gluten_memory_CHThreadGroup_threadGroupPeakMemory(JNIEnv * env, jclass, jlong id)
{
    LOCAL_ENGINE_JNI_METHOD_START
    return local_engine::QueryContext::instance().currentPeakMemory(id);
    LOCAL_ENGINE_JNI_METHOD_END(env, 0l)
}

JNIEXPORT void Java_org_apache_gluten_memory_CHThreadGroup_releaseThreadGroup(JNIEnv * env, jclass, jlong id)
{
    LOCAL_ENGINE_JNI_METHOD_START
    local_engine::QueryContext::instance().finalizeQuery(id);
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}

// only for UT GlutenClickHouseNativeExceptionSuite
JNIEXPORT void Java_org_apache_gluten_utils_TestExceptionUtils_generateNativeException(JNIEnv * env)
{
    LOCAL_ENGINE_JNI_METHOD_START
    throw DB::Exception(DB::ErrorCodes::UNKNOWN_EXCEPTION, "test native exception");
    LOCAL_ENGINE_JNI_METHOD_END(env, )
}


JNIEXPORT jstring Java_org_apache_gluten_execution_CHNativeCacheManager_nativeCacheParts(
    JNIEnv * env, jobject, jstring table_, jstring columns_, jboolean only_meta_cache_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto table_def = jstring2string(env, table_);
    auto columns = jstring2string(env, columns_);
    Poco::StringTokenizer tokenizer(columns, ",");
    std::unordered_set<String> column_set;
    for (const auto & col : tokenizer)
        column_set.insert(col);
    local_engine::MergeTreeTableInstance table(table_def);
    auto id = local_engine::CacheManager::instance().cacheParts(table, column_set, only_meta_cache_);
    return local_engine::charTojstring(env, id.c_str());
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr);
}

JNIEXPORT jobject Java_org_apache_gluten_execution_CHNativeCacheManager_nativeGetCacheStatus(JNIEnv * env, jobject, jstring id)
{
    LOCAL_ENGINE_JNI_METHOD_START
    return local_engine::CacheManager::instance().getCacheStatus(env, jstring2string(env, id));
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr);
}

JNIEXPORT jstring Java_org_apache_gluten_execution_CHNativeCacheManager_nativeCacheFiles(JNIEnv * env, jobject, jbyteArray files)
{
    LOCAL_ENGINE_JNI_METHOD_START
    const auto files_bytes = local_engine::getByteArrayElementsSafe(env, files);
    const std::string::size_type files_bytes_size = files_bytes.length();
    std::string_view files_view = {reinterpret_cast<const char *>(files_bytes.elems()), files_bytes_size};
    substrait::ReadRel::LocalFiles local_files = local_engine::BinaryToMessage<substrait::ReadRel::LocalFiles>(files_view);

    auto jobId = local_engine::CacheManager::instance().cacheFiles(local_files);
    return local_engine::charTojstring(env, jobId.c_str());
    LOCAL_ENGINE_JNI_METHOD_END(env, nullptr);
}

JNIEXPORT void Java_org_apache_gluten_execution_CHNativeCacheManager_removeFiles(JNIEnv * env, jobject, jstring file_, jstring cache_name_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto file = jstring2string(env, file_);
    auto cache_name = jstring2string(env, cache_name_);

    local_engine::CacheManager::removeFiles(file, cache_name);
    LOCAL_ENGINE_JNI_METHOD_END(env, );
}

JNIEXPORT jlong Java_org_apache_gluten_vectorized_DeltaWriterJNIWrapper_createDeletionVectorWriter(
    JNIEnv * env, jclass, jstring table_path_, jint prefix_length_, jlong packingTargetSize_, jstring dv_file_name_prefix_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto table_path = jstring2string(env, table_path_);
    auto dv_file_name_prefix = jstring2string(env, dv_file_name_prefix_);

    const auto query_context = local_engine::QueryContext::instance().currentQueryContext();
    auto writer = new local_engine::delta::DeltaWriter(query_context, table_path, prefix_length_, packingTargetSize_, dv_file_name_prefix);
    return reinterpret_cast<jlong>(writer);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1);
}

JNIEXPORT void Java_org_apache_gluten_vectorized_DeltaWriterJNIWrapper_deletionVectorWrite(
    JNIEnv * env, jclass, jlong writer_address_, jlong blockAddress)
{
    LOCAL_ENGINE_JNI_METHOD_START
    const auto * block = reinterpret_cast<DB::Block *>(blockAddress);
    auto * writer = reinterpret_cast<local_engine::delta::DeltaWriter *>(writer_address_);
    writer->writeDeletionVector(*block);
    LOCAL_ENGINE_JNI_METHOD_END(env, );
}

JNIEXPORT jlong
Java_org_apache_gluten_vectorized_DeltaWriterJNIWrapper_deletionVectorWriteFinalize(JNIEnv * env, jclass, jlong writer_address_)
{
    LOCAL_ENGINE_JNI_METHOD_START
    auto * writer = reinterpret_cast<local_engine::delta::DeltaWriter *>(writer_address_);
    auto * column_batch = writer->finalize();
    delete writer;
    return reinterpret_cast<UInt64>(column_batch);
    LOCAL_ENGINE_JNI_METHOD_END(env, -1);
}

#ifdef __cplusplus
}

#endif

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

#include <jni.h>
#include "compute/NativeExecutor.h"
#include "compute/VeloxExpressionEvaluator.h"
#include "compute/VeloxRuntime.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"
#include "utils/ConfigExtractor.h"
#include "utils/ObjectStore.h"

using namespace gluten;
using namespace facebook;

extern "C" JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ExpressionBenchmarkJni_compile(
    JNIEnv* env,
    jobject wrapper,
    jobjectArray stagePlans,
    jint batchSize,
    jboolean hashPartitioned,
    jboolean resizeShuffleInput,
    jboolean resizeShuffleOutput,
    jint minRows,
    jint maxRows,
    jlong preferredBytes,
    jboolean copyRanges,
    jstring shuffleDir) {
  JNI_METHOD_START
  auto* runtime = dynamic_cast<VeloxRuntime*>(getRuntime(env, wrapper));
  GLUTEN_CHECK(runtime != nullptr && batchSize > 0, "Invalid benchmark runtime");
  GLUTEN_CHECK(stagePlans != nullptr, "Missing benchmark stages");
  const auto stageCount = env->GetArrayLength(stagePlans);
  GLUTEN_CHECK(stageCount == 1 || stageCount == 2, "Expected one or two benchmark stages");
  std::vector<std::shared_ptr<VeloxPlanEvaluator>> stages;
  stages.reserve(stageCount);
  for (int i = 0; i < stageCount; ++i) {
    auto plan = static_cast<jbyteArray>(env->GetObjectArrayElement(stagePlans, i));
    GLUTEN_CHECK(plan != nullptr, "Missing benchmark stage plan");
    auto bytes = getByteArrayElementsSafe(env, plan);
    env->DeleteLocalRef(plan);
    stages.push_back(
        std::make_shared<VeloxPlanEvaluator>(runtime, reinterpret_cast<const uint8_t*>(bytes.elems()), bytes.length()));
  }
  return runtime->saveObject(std::make_shared<NativeExecutor>(
      runtime,
      std::move(stages),
      batchSize,
      hashPartitioned,
      resizeShuffleInput,
      resizeShuffleOutput,
      minRows,
      maxRows,
      preferredBytes,
      copyRanges,
      jStringToCString(env, shuffleDir)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

extern "C" JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ExpressionBenchmarkJni_execute(
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jobjectArray inputs,
    jlongArray metrics) {
  JNI_METHOD_START
  GLUTEN_CHECK(metrics != nullptr && env->GetArrayLength(metrics) == 2, "Expected two metrics");
  auto executor = ObjectStore::retrieve<NativeExecutor>(handle);
  GLUTEN_CHECK(inputs != nullptr, "Missing benchmark inputs");
  std::vector<std::vector<std::shared_ptr<ColumnarBatch>>> batches;
  for (int i = 0; i < env->GetArrayLength(inputs); ++i) {
    auto handles = static_cast<jlongArray>(env->GetObjectArrayElement(inputs, i));
    GLUTEN_CHECK(handles != nullptr, "Missing benchmark input partition");
    auto values = getLongArrayElementsSafe(env, handles);
    const auto count = values.length();
    auto& partition = batches.emplace_back();
    partition.reserve(count);
    for (int j = 0; j < count; ++j) {
      partition.push_back(ObjectStore::retrieve<ColumnarBatch>(values.elems()[j]));
    }
    env->DeleteLocalRef(handles);
  }
  int64_t shuffleBytes;
  auto result = executor->execute(std::move(batches), shuffleBytes);
  const jlong values[] = {0, shuffleBytes};
  env->SetLongArrayRegion(metrics, 0, 2, values);
  return getRuntime(env, wrapper)->saveObject(std::move(result));
  JNI_METHOD_END(kInvalidObjectHandle)
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_gluten_vectorized_ExpressionBenchmarkJni_close(JNIEnv* env, jobject, jlong handle) {
  JNI_METHOD_START
  ObjectStore::release(handle);
  JNI_METHOD_END()
}

extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_ExpressionBenchmarkJni_usedBytes(JNIEnv* env, jobject wrapper) {
  JNI_METHOD_START
  auto* runtime = dynamic_cast<VeloxRuntime*>(getRuntime(env, wrapper));
  GLUTEN_CHECK(runtime != nullptr, "Expected Velox runtime");
  return runtime->memoryManager()->getAggregateMemoryPool()->usedBytes();
  JNI_METHOD_END(0)
}

extern "C" {
JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_VeloxExpressionEvaluatorJniWrapper_compile( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray extendedExpression) {
  JNI_METHOD_START
  GLUTEN_CHECK(extendedExpression != nullptr, "ExtendedExpression must not be null");
  auto* runtime = dynamic_cast<VeloxRuntime*>(getRuntime(env, wrapper));
  GLUTEN_CHECK(runtime != nullptr, "Expression evaluation requires a Velox runtime");
  const auto bytes = getByteArrayElementsSafe(env, extendedExpression);
  auto* memoryManager = runtime->memoryManager();
  const auto partitionId = runtime->getSparkTaskInfo().value_or(SparkTaskInfo{}).partitionId;
  auto queryCtx = velox::core::QueryCtx::create(
      nullptr,
      velox::core::QueryConfig{getQueryContextConf(runtime->veloxCfg().get(), partitionId)},
      {},
      nullptr,
      memoryManager->getAggregateMemoryPool());
  auto evaluator = std::make_shared<VeloxExpressionEvaluator>(
      memoryManager->getLeafMemoryPool(), std::move(queryCtx), bytes.elems(), bytes.length());
  return runtime->saveObject(std::move(evaluator));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_VeloxExpressionEvaluatorJniWrapper_evaluate( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong evaluatorHandle,
    jlong inputBatchHandle) {
  JNI_METHOD_START
  auto* runtime = dynamic_cast<VeloxRuntime*>(getRuntime(env, wrapper));
  GLUTEN_CHECK(runtime != nullptr, "Expression evaluation requires a Velox runtime");
  auto evaluator = ObjectStore::retrieve<VeloxExpressionEvaluator>(evaluatorHandle);
  auto input = ObjectStore::retrieve<ColumnarBatch>(inputBatchHandle);
  return runtime->saveObject(evaluator->evaluate(input));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_VeloxExpressionEvaluatorJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject,
    jlong evaluatorHandle) {
  JNI_METHOD_START
  ObjectStore::release(evaluatorHandle);
  JNI_METHOD_END()
}

} // extern "C"

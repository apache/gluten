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

#include <stdexcept>
#include <unordered_map>

#include "substrait/algebra.pb.h"
#include "substrait/extensions/extensions.pb.h"

#include "compute/ProtobufUtils.h"
#include "compute/VeloxRuntime.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"
#include "substrait/SubstraitParser.h"
#include "substrait/SubstraitToVeloxExpr.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "compute/delta/DeltaRowIndexFinder.h"
#include "velox/core/Expressions.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace gluten::delta;

namespace {

std::string jbyteArrayToString(JNIEnv* env, jbyteArray array) {
  jsize length = env->GetArrayLength(array);
  jbyte* bytes = env->GetByteArrayElements(array, nullptr);
  std::string result(reinterpret_cast<char*>(bytes), length);
  env->ReleaseByteArrayElements(array, bytes, JNI_ABORT);
  return result;
}

jlongArray vectorToJlongArray(JNIEnv* env, const std::vector<uint64_t>& vec) {
  jlongArray result = env->NewLongArray(vec.size());
  if (result == nullptr) {
    return nullptr;
  }

  jlong* elements = env->GetLongArrayElements(result, nullptr);
  for (size_t i = 0; i < vec.size(); ++i) {
    elements[i] = static_cast<jlong>(vec[i]);
  }
  env->ReleaseLongArrayElements(result, elements, 0);
  return result;
}

std::shared_ptr<const RowType> deserializeSchema(const std::string& serialized) {
  ::substrait::NamedStruct namedStruct;
  VELOX_CHECK(
      gluten::parseProtobuf(
          reinterpret_cast<const uint8_t*>(serialized.data()),
          serialized.size(),
          &namedStruct),
      "Failed to parse serialized schema");

  auto types = gluten::SubstraitParser::parseNamedStruct(namedStruct);
  std::vector<std::string> names;
  names.reserve(namedStruct.names_size());
  for (const auto& name : namedStruct.names()) {
    names.emplace_back(name);
  }
  return ROW(std::move(names), std::move(types));
}

std::unordered_map<uint64_t, std::string> deserializeFunctionMappings(
    JNIEnv* env,
    jobjectArray mappings) {
  std::unordered_map<uint64_t, std::string> functionMappings;
  if (mappings == nullptr) {
    return functionMappings;
  }

  const auto mappingCount = env->GetArrayLength(mappings);
  functionMappings.reserve(mappingCount);
  for (jsize i = 0; i < mappingCount; ++i) {
    auto mapping = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(mappings, i));
    std::string serializedMapping = jbyteArrayToString(env, mapping);
    ::substrait::extensions::SimpleExtensionDeclaration mappingDecl;
    VELOX_CHECK(
        gluten::parseProtobuf(
            reinterpret_cast<const uint8_t*>(serializedMapping.data()),
            serializedMapping.size(),
            &mappingDecl),
        "Failed to parse serialized function mapping");

    const auto& function = mappingDecl.extension_function();
    functionMappings.emplace(function.function_anchor(), function.name());
    env->DeleteLocalRef(mapping);
  }
  return functionMappings;
}

std::shared_ptr<const core::ITypedExpr> deserializeExpression(
    const std::string& serialized,
    const std::shared_ptr<const RowType>& inputType,
    const std::unordered_map<uint64_t, std::string>& functionMappings) {
  if (serialized.empty()) {
    return nullptr;
  }

  ::substrait::Expression expression;
  VELOX_CHECK(
      gluten::parseProtobuf(
          reinterpret_cast<const uint8_t*>(serialized.data()),
          serialized.size(),
          &expression),
      "Failed to parse serialized filter expression");

  gluten::SubstraitVeloxExprConverter converter(
      gluten::defaultLeafVeloxMemoryPool().get(),
      functionMappings);
  return converter.toVeloxExpr(expression, inputType);
}

std::shared_ptr<const core::ITypedExpr> deserializeExpression(
    const std::string& serialized,
    const std::shared_ptr<const RowType>& inputType) {
  return deserializeExpression(serialized, inputType, {});
}

} // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_VeloxRowIndexFinderJni_00024_nativeCreateFinder(
    JNIEnv* env,
    jclass,
    jlong memoryPoolHandle) {
  JNI_METHOD_START

  auto* pool = reinterpret_cast<memory::MemoryPool*>(memoryPoolHandle);
  if (pool == nullptr) {
    pool = gluten::defaultLeafVeloxMemoryPool().get();
  }

  DeltaRowIndexFinder::Config config;
  config.batchSize = 10000;
  config.numThreads = 1;
  config.memoryLimit = 1ULL << 30;

  auto* finder = new DeltaRowIndexFinder(pool, config);
  return reinterpret_cast<jlong>(finder);

  JNI_METHOD_END(0)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_VeloxRowIndexFinderJni_00024_nativeCreateFinderWithConfig(
    JNIEnv* env,
    jclass,
    jlong memoryPoolHandle,
    jint batchSize,
    jint numThreads,
    jint memoryLimitMB) {
  JNI_METHOD_START

  auto* pool = reinterpret_cast<memory::MemoryPool*>(memoryPoolHandle);
  if (pool == nullptr) {
    pool = gluten::defaultLeafVeloxMemoryPool().get();
  }

  DeltaRowIndexFinder::Config config;
  config.batchSize = static_cast<uint32_t>(batchSize);
  config.numThreads = static_cast<uint32_t>(numThreads);
  config.memoryLimit = static_cast<uint64_t>(memoryLimitMB) * 1024 * 1024;

  auto* finder = new DeltaRowIndexFinder(pool, config);
  return reinterpret_cast<jlong>(finder);

  JNI_METHOD_END(0)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_vectorized_VeloxRowIndexFinderJni_00024_nativeFindMatchingRows(
    JNIEnv* env,
    jclass,
    jlong finderHandle,
    jstring filePath,
    jbyteArray filterExpr,
    jbyteArray schema,
    jobjectArray functionMappings) {
  JNI_METHOD_START

  auto* finder = reinterpret_cast<DeltaRowIndexFinder*>(finderHandle);
  if (finder == nullptr) {
    throw std::invalid_argument("Finder handle is null");
  }

  const char* filePathCStr = env->GetStringUTFChars(filePath, nullptr);
  std::string filePathStr(filePathCStr);
  env->ReleaseStringUTFChars(filePath, filePathCStr);

  if (schema == nullptr) {
    throw std::invalid_argument("Schema cannot be null");
  }
  std::string serializedSchema = jbyteArrayToString(env, schema);
  auto rowType = deserializeSchema(serializedSchema);

  auto nativeFunctionMappings =
      deserializeFunctionMappings(env, functionMappings);

  std::shared_ptr<const core::ITypedExpr> filter = nullptr;
  if (filterExpr != nullptr) {
    std::string serializedFilter = jbyteArrayToString(env, filterExpr);
    filter = deserializeExpression(
        serializedFilter,
        rowType,
        nativeFunctionMappings);
  }

  auto indices = finder->findMatchingRows(filePathStr, filter, rowType);
  return vectorToJlongArray(env, indices);

  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobjectArray JNICALL
Java_org_apache_gluten_vectorized_VeloxRowIndexFinderJni_00024_nativeFindMatchingRowsBatch(
    JNIEnv* env,
    jclass,
    jlong finderHandle,
    jobjectArray filePaths,
    jobjectArray filterExprs,
    jobjectArray schemas) {
  JNI_METHOD_START

  auto* finder = reinterpret_cast<DeltaRowIndexFinder*>(finderHandle);
  if (finder == nullptr) {
    throw std::invalid_argument("Finder handle is null");
  }

  jsize numFiles = env->GetArrayLength(filePaths);
  std::vector<std::tuple<
      std::string,
      std::shared_ptr<const core::ITypedExpr>,
      std::shared_ptr<const RowType>>> files;
  files.reserve(numFiles);

  for (jsize i = 0; i < numFiles; ++i) {
    auto jFilePath = reinterpret_cast<jstring>(env->GetObjectArrayElement(filePaths, i));
    const char* filePathCStr = env->GetStringUTFChars(jFilePath, nullptr);
    std::string filePath(filePathCStr);
    env->ReleaseStringUTFChars(jFilePath, filePathCStr);

    auto jSchema = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(schemas, i));
    if (jSchema == nullptr) {
      throw std::invalid_argument("Schema cannot be null");
    }
    std::string serializedSchema = jbyteArrayToString(env, jSchema);
    auto rowType = deserializeSchema(serializedSchema);

    auto jFilter = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(filterExprs, i));
    std::shared_ptr<const core::ITypedExpr> filter = nullptr;
    if (jFilter != nullptr) {
      std::string serializedFilter = jbyteArrayToString(env, jFilter);
      filter = deserializeExpression(serializedFilter, rowType);
    }

    files.emplace_back(filePath, filter, rowType);
    env->DeleteLocalRef(jFilePath);
    env->DeleteLocalRef(jFilter);
    env->DeleteLocalRef(jSchema);
  }

  auto results = finder->findMatchingRowsBatch(files);

  jclass longArrayClass = env->FindClass("[J");
  jobjectArray resultArray =
      env->NewObjectArray(numFiles, longArrayClass, nullptr);

  for (jsize i = 0; i < numFiles; ++i) {
    auto jFilePath = reinterpret_cast<jstring>(env->GetObjectArrayElement(filePaths, i));
    const char* filePathCStr = env->GetStringUTFChars(jFilePath, nullptr);
    std::string filePath(filePathCStr);
    env->ReleaseStringUTFChars(jFilePath, filePathCStr);

    auto it = results.find(filePath);
    if (it != results.end()) {
      jlongArray indices = vectorToJlongArray(env, it->second);
      env->SetObjectArrayElement(resultArray, i, indices);
      env->DeleteLocalRef(indices);
    }
    env->DeleteLocalRef(jFilePath);
  }

  return resultArray;

  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_vectorized_VeloxRowIndexFinderJni_00024_nativeGetStats(
    JNIEnv* env,
    jclass,
    jlong finderHandle) {
  JNI_METHOD_START

  auto* finder = reinterpret_cast<DeltaRowIndexFinder*>(finderHandle);
  if (finder == nullptr) {
    throw std::invalid_argument("Finder handle is null");
  }

  auto stats = finder->getStats();

  jlongArray result = env->NewLongArray(4);
  jlong statsArray[4] = {
      static_cast<jlong>(stats.rowsScanned),
      static_cast<jlong>(stats.rowsMatched),
      static_cast<jlong>(stats.bytesRead),
      static_cast<jlong>(stats.executionTimeMs)};
  env->SetLongArrayRegion(result, 0, 4, statsArray);
  return result;

  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_org_apache_gluten_vectorized_VeloxRowIndexFinderJni_00024_nativeReleaseFinder(
    JNIEnv* env,
    jclass,
    jlong finderHandle) {
  JNI_METHOD_START

  auto* finder = reinterpret_cast<DeltaRowIndexFinder*>(finderHandle);
  if (finder != nullptr) {
    delete finder;
  }

  JNI_METHOD_END()
}

} // extern "C"

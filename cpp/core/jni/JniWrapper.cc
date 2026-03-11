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
#include <algorithm>
#include <cstdint>
#include <filesystem>

#include "compute/Runtime.h"
#include "config/GlutenConfig.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"

#include <arrow/c/bridge.h>
#include <google/protobuf/stubs/common.h>
#include <optional>
#include <string>
#include "memory/AllocationListener.h"
#include "memory/SplitAwareColumnarBatchIterator.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/Partitioning.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/Utils.h"
#include "utils/ArrowStatus.h"
#include "utils/StringUtil.h"

using namespace gluten;

namespace {

jclass byteArrayClass;

jclass jniUnsafeByteBufferClass;
jmethodID jniUnsafeByteBufferAllocate;
jmethodID jniUnsafeByteBufferAddress;
jmethodID jniUnsafeByteBufferSize;

jclass jniByteInputStreamClass;
jmethodID jniByteInputStreamRead;
jmethodID jniByteInputStreamTell;
jmethodID jniByteInputStreamClose;

jclass splitResultClass;
jmethodID splitResultConstructor;

jclass metricsBuilderClass;
jmethodID metricsBuilderConstructor;
jclass nativeColumnarToRowInfoClass;
jmethodID nativeColumnarToRowInfoConstructor;

jclass shuffleReaderMetricsClass;
jmethodID shuffleReaderMetricsSetDecompressTime;
jmethodID shuffleReaderMetricsSetDeserializeTime;

jclass shuffleStreamReaderClass;
jmethodID shuffleStreamReaderNextStream;

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, arrow::MemoryPool* pool, jobject jniIn) : pool_(pool) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw GlutenException(errorMessage);
    }
    jniIn_ = env->NewGlobalRef(jniIn);
  }

  ~JavaInputStreamAdaptor() override {
    try {
      auto status = JavaInputStreamAdaptor::Close();
      if (!status.ok()) {
        LOG(WARNING) << __func__ << " call JavaInputStreamAdaptor::Close() failed, status:" << status.ToString();
      }
    } catch (std::exception& e) {
      LOG(WARNING) << __func__ << " call JavaInputStreamAdaptor::Close() got exception:" << e.what();
    }
  }

  // not thread safe
  arrow::Status Close() override {
    if (closed_) {
      return arrow::Status::OK();
    }
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->CallVoidMethod(jniIn_, jniByteInputStreamClose);
    checkException(env);
    env->DeleteGlobalRef(jniIn_);
    vm_->DetachCurrentThread();
    closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong told = env->CallLongMethod(jniIn_, jniByteInputStreamTell);
    checkException(env);
    return told;
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong read = env->CallLongMethod(jniIn_, jniByteInputStreamRead, reinterpret_cast<jlong>(out), nbytes);
    checkException(env);
    return read;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    GLUTEN_ASSIGN_OR_THROW(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_))
    GLUTEN_ASSIGN_OR_THROW(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  arrow::MemoryPool* pool_;
  JavaVM* vm_;
  jobject jniIn_;
  bool closed_ = false;
};

/// Internal backend consists of empty implementations of Runtime API and MemoryManager API.
/// The backend is used for saving contextual objects only.
///
/// It's also possible to extend the implementation for handling Arrow-based requests either in the future.
inline static const std::string kInternalBackendKind{"internal"};

class InternalMemoryManager : public MemoryManager {
 public:
  InternalMemoryManager(const std::string& kind) : MemoryManager(kind) {}

  arrow::MemoryPool* defaultArrowMemoryPool() override {
    throw GlutenException("Not implemented");
  }

  std::shared_ptr<arrow::MemoryPool> getOrCreateArrowMemoryPool(const std::string& name) override {
    throw GlutenException("Not yet implemented");
  }

  const MemoryUsageStats collectMemoryUsageStats() const override {
    return MemoryUsageStats();
  }

  const int64_t shrink(int64_t size) override {
    return 0;
  }

  void hold() override {}
};

class InternalRuntime : public Runtime {
 public:
  InternalRuntime(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& confMap)
      : Runtime(kind, memoryManager, confMap) {}
};

MemoryManager* internalMemoryManagerFactory(const std::string& kind, std::unique_ptr<AllocationListener> listener) {
  return new InternalMemoryManager(kind);
}

void internalMemoryManagerReleaser(MemoryManager* memoryManager) {
  delete memoryManager;
}

Runtime* internalRuntimeFactory(
    const std::string& kind,
    MemoryManager* memoryManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  return new InternalRuntime(kind, memoryManager, sessionConf);
}

void internalRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}

class ShuffleStreamReader : public StreamReader {
 public:
  ShuffleStreamReader(JNIEnv* env, jobject reader) {
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      throw GlutenException("Unable to get JavaVM instance");
    }
    ref_ = env->NewGlobalRef(reader);
  }

  ~ShuffleStreamReader() override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->DeleteGlobalRef(ref_);
  }

  std::shared_ptr<arrow::io::InputStream> readNextStream(arrow::MemoryPool* pool) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);

    jobject jniIn = env->CallObjectMethod(ref_, shuffleStreamReaderNextStream);
    checkException(env);
    if (jniIn == nullptr) {
      return nullptr; // No more streams to read
    }
    std::shared_ptr<arrow::io::InputStream> in = std::make_shared<JavaInputStreamAdaptor>(env, pool, jniIn);
    return in;
  }

 private:
  JavaVM* vm_;
  jobject ref_;
};

} // namespace

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }
  getJniCommonState()->ensureInitialized(env);
  getJniErrorState()->ensureInitialized(env);

  MemoryManager::registerFactory(kInternalBackendKind, internalMemoryManagerFactory, internalMemoryManagerReleaser);
  Runtime::registerFactory(kInternalBackendKind, internalRuntimeFactory, internalRuntimeReleaser);

  byteArrayClass = createGlobalClassReferenceOrError(env, "[B");

  jniUnsafeByteBufferClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/sql/execution/unsafe/JniUnsafeByteBuffer;");
  jniUnsafeByteBufferAllocate = env->GetStaticMethodID(
      jniUnsafeByteBufferClass, "allocate", "(J)Lorg/apache/spark/sql/execution/unsafe/JniUnsafeByteBuffer;");
  jniUnsafeByteBufferAddress = env->GetMethodID(jniUnsafeByteBufferClass, "address", "()J");
  jniUnsafeByteBufferSize = env->GetMethodID(jniUnsafeByteBufferClass, "size", "()J");

  jniByteInputStreamClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/JniByteInputStream;");
  jniByteInputStreamRead = getMethodIdOrError(env, jniByteInputStreamClass, "read", "(JJ)J");
  jniByteInputStreamTell = getMethodIdOrError(env, jniByteInputStreamClass, "tell", "()J");
  jniByteInputStreamClose = getMethodIdOrError(env, jniByteInputStreamClass, "close", "()V");

  splitResultClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/GlutenSplitResult;");
  splitResultConstructor = getMethodIdOrError(env, splitResultClass, "<init>", "(JJJJJJJJJJDJ[J[J)V");

  metricsBuilderClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/metrics/Metrics;");

  metricsBuilderConstructor = getMethodIdOrError(
      env,
      metricsBuilderClass,
      "<init>",
      "([J[J[J[J[J[J[J[J[J[JJ[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[JLjava/lang/String;)V");

  nativeColumnarToRowInfoClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/NativeColumnarToRowInfo;");
  nativeColumnarToRowInfoConstructor = getMethodIdOrError(env, nativeColumnarToRowInfoClass, "<init>", "([I[IJ)V");

  shuffleReaderMetricsClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ShuffleReaderMetrics;");
  shuffleReaderMetricsSetDecompressTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDecompressTime", "(J)V");
  shuffleReaderMetricsSetDeserializeTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDeserializeTime", "(J)V");

  shuffleStreamReaderClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ShuffleStreamReader;");
  shuffleStreamReaderNextStream = getMethodIdOrError(
      env, shuffleStreamReaderClass, "nextStream", "()Lorg/apache/gluten/vectorized/JniByteInputStream;");

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
  env->DeleteGlobalRef(jniByteInputStreamClass);
  env->DeleteGlobalRef(splitResultClass);
  env->DeleteGlobalRef(nativeColumnarToRowInfoClass);
  env->DeleteGlobalRef(byteArrayClass);
  env->DeleteGlobalRef(jniUnsafeByteBufferClass);
  env->DeleteGlobalRef(shuffleReaderMetricsClass);

  getJniErrorState()->close();
  getJniCommonState()->close();

  google::protobuf::ShutdownProtobufLibrary();
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_runtime_RuntimeJniWrapper_createRuntime( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jBackendType,
    jlong nmmHandle,
    jbyteArray sessionConf) {
  JNI_METHOD_START
  MemoryManager* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  auto safeArray = getByteArrayElementsSafe(env, sessionConf);
  auto sparkConf = parseConfMap(env, safeArray.elems(), safeArray.length());
  auto backendType = jStringToCString(env, jBackendType);

  auto runtime = Runtime::create(backendType, memoryManager, sparkConf);
  return reinterpret_cast<jlong>(runtime);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_runtime_RuntimeJniWrapper_releaseRuntime( // NOLINT
    JNIEnv* env,
    jclass,
    jlong ctxHandle) {
  JNI_METHOD_START
  auto runtime = jniCastOrThrow<Runtime>(ctxHandle);

  Runtime::release(runtime);
  JNI_METHOD_END()
}

namespace {
const std::string kBacktraceAllocation = "spark.gluten.memory.backtrace.allocation";
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_create( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jBackendType,
    jobject jListener,
    jbyteArray sessionConf) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }
  auto backendType = jStringToCString(env, jBackendType);
  auto safeArray = getByteArrayElementsSafe(env, sessionConf);
  auto sparkConf = parseConfMap(env, safeArray.elems(), safeArray.length());
  std::unique_ptr<AllocationListener> listener = std::make_unique<SparkAllocationListener>(vm, jListener);
  bool backtrace = sparkConf.at(kBacktraceAllocation) == "true";
  if (backtrace) {
    listener = std::make_unique<BacktraceAllocationListener>(std::move(listener));
  }
  MemoryManager* mm = MemoryManager::create(backendType, std::move(listener));
  return reinterpret_cast<jlong>(mm);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_collectUsage( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);

  const MemoryUsageStats& stats = memoryManager->collectMemoryUsageStats();
  auto size = stats.ByteSizeLong();
  jbyteArray out = env->NewByteArray(size);
  std::vector<uint8_t> buffer(size);
  GLUTEN_CHECK(
      stats.SerializeToArray(reinterpret_cast<void*>(buffer.data()), size),
      "Serialization failed when collecting memory usage stats");
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer.data()));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_shrink( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle,
    jlong size) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  return memoryManager->shrink(static_cast<int64_t>(size));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_hold( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  memoryManager->hold();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_release( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  MemoryManager::release(memoryManager);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativePlanString( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArray,
    jboolean details) {
  JNI_METHOD_START

  auto safeArray = getByteArrayElementsSafe(env, planArray);
  auto planData = safeArray.elems();
  auto planSize = env->GetArrayLength(planArray);
  auto ctx = getRuntime(env, wrapper);
  ctx->parsePlan(planData, planSize);
  auto& conf = ctx->getConfMap();
  auto planString = ctx->planString(details, conf);
  return env->NewStringUTF(planString.c_str());

  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_injectWriteFilesTempPath( // NOLINT
    JNIEnv* env,
    jclass,
    jbyteArray path,
    jbyteArray fileName) {
  JNI_METHOD_START
  auto len = env->GetArrayLength(path);
  auto safeArray = getByteArrayElementsSafe(env, path);
  std::string pathStr(reinterpret_cast<char*>(safeArray.elems()), len);
  *Runtime::localWriteFilesTempPath() = pathStr;

  len = env->GetArrayLength(fileName);
  auto fileNameArray = getByteArrayElementsSafe(env, fileName);
  std::string fileNameStr(reinterpret_cast<char*>(fileNameArray.elems()), len);
  *Runtime::localWriteFileName() = fileNameStr;
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeCreateKernelWithIterator( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArr,
    jobjectArray splitInfosArr,
    jobjectArray batchItrArray,
    jint stageId,
    jint partitionId,
    jlong taskId,
    jboolean enableDumping,
    jstring spillDir) {
  JNI_METHOD_START

  auto ctx = getRuntime(env, wrapper);

  ctx->setSparkTaskInfo({stageId, partitionId, taskId});

  if (enableDumping) {
    ctx->enableDumping();
  }

  auto spillDirStr = jStringToCString(env, spillDir);

  auto safePlanArray = getByteArrayElementsSafe(env, planArr);
  auto planSize = env->GetArrayLength(planArr);

  ctx->parsePlan(safePlanArray.elems(), planSize);

  if (splitInfosArr != nullptr) {
    for (jsize i = 0, splitInfoArraySize = env->GetArrayLength(splitInfosArr); i < splitInfoArraySize; i++) {
      jbyteArray splitInfoArray = static_cast<jbyteArray>(env->GetObjectArrayElement(splitInfosArr, i));
      jsize splitInfoSize = env->GetArrayLength(splitInfoArray);
      auto safeSplitArray = getByteArrayElementsSafe(env, splitInfoArray);
      auto splitInfoData = safeSplitArray.elems();

      ctx->parseSplitInfo(splitInfoData, splitInfoSize, i);
    }
  }

  // Handle the Java iters
  std::vector<std::shared_ptr<ResultIterator>> inputIters;
  if (batchItrArray != nullptr) {
    jsize itersLen = env->GetArrayLength(batchItrArray);
    inputIters.reserve(itersLen);
    for (int idx = 0; idx < itersLen; idx++) {
      jobject iter = env->GetObjectArrayElement(batchItrArray, idx);
      auto arrayIter = std::make_unique<JniColumnarBatchIterator>(env, iter, ctx, idx);
      auto resultIter = std::make_shared<ResultIterator>(std::move(arrayIter));
      inputIters.push_back(std::move(resultIter));
    }
  }

  return ctx->saveObject(ctx->createResultIterator(spillDirStr, inputIters));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeHasNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (iter == nullptr) {
    std::string errorMessage =
        "When hasNext() is called on a closed iterator, an exception is thrown. To prevent this, consider using the protectInvocationFlow() method when creating the iterator in scala side. This will allow the hasNext() method to be called multiple times without issue.";
    throw GlutenException(errorMessage);
  }
  return iter->hasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (!iter->hasNext()) {
    return kInvalidObjectHandle;
  }

  std::shared_ptr<ColumnarBatch> batch = iter->next();
  auto batchHandle = ctx->saveObject(batch);

  iter->setExportNanos(batch->getExportNanos());
  return batchHandle;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_metrics_IteratorMetricsJniWrapper_nativeFetchMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  auto metrics = iter->getMetrics();
  unsigned int numMetrics = 0;
  if (metrics) {
    numMetrics = metrics->numMetrics;
  }

  jlongArray longArray[Metrics::kNum];
  for (auto i = static_cast<int>(Metrics::kBegin); i != static_cast<int>(Metrics::kEnd); ++i) {
    longArray[i] = env->NewLongArray(numMetrics);
    if (metrics) {
      env->SetLongArrayRegion(longArray[i], 0, numMetrics, metrics->get((Metrics::TYPE)i));
    }
  }

  return env->NewObject(
      metricsBuilderClass,
      metricsBuilderConstructor,
      longArray[Metrics::kInputRows],
      longArray[Metrics::kInputVectors],
      longArray[Metrics::kInputBytes],
      longArray[Metrics::kRawInputRows],
      longArray[Metrics::kRawInputBytes],
      longArray[Metrics::kOutputRows],
      longArray[Metrics::kOutputVectors],
      longArray[Metrics::kOutputBytes],
      longArray[Metrics::kCpuCount],
      longArray[Metrics::kWallNanos],
      metrics ? metrics->veloxToArrow : -1,
      longArray[Metrics::kPeakMemoryBytes],
      longArray[Metrics::kNumMemoryAllocations],
      longArray[Metrics::kSpilledInputBytes],
      longArray[Metrics::kSpilledBytes],
      longArray[Metrics::kSpilledRows],
      longArray[Metrics::kSpilledPartitions],
      longArray[Metrics::kSpilledFiles],
      longArray[Metrics::kNumDynamicFiltersProduced],
      longArray[Metrics::kNumDynamicFiltersAccepted],
      longArray[Metrics::kNumReplacedWithDynamicFilterRows],
      longArray[Metrics::kFlushRowCount],
      longArray[Metrics::kLoadedToValueHook],
      longArray[Metrics::kBloomFilterBlocksByteSize],
      longArray[Metrics::kScanTime],
      longArray[Metrics::kSkippedSplits],
      longArray[Metrics::kProcessedSplits],
      longArray[Metrics::kSkippedStrides],
      longArray[Metrics::kProcessedStrides],
      longArray[Metrics::kRemainingFilterTime],
      longArray[Metrics::kIoWaitTime],
      longArray[Metrics::kStorageReadBytes],
      longArray[Metrics::kLocalReadBytes],
      longArray[Metrics::kRamReadBytes],
      longArray[Metrics::kPreloadSplits],
      longArray[Metrics::kPageLoadTime],
      longArray[Metrics::kDataSourceAddSplitWallNanos],
      longArray[Metrics::kDataSourceReadWallNanos],
      longArray[Metrics::kPhysicalWrittenBytes],
      longArray[Metrics::kWriteIOTime],
      longArray[Metrics::kNumWrittenFiles],
      longArray[Metrics::kLoadLazyVectorTime],
      metrics && metrics->stats.has_value() ? env->NewStringUTF(metrics->stats->c_str()) : nullptr);

  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeSpill( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle,
    jlong size) {
  JNI_METHOD_START
  auto it = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (it == nullptr) {
    std::string errorMessage = "Invalid result iter handle " + std::to_string(iterHandle);
    throw GlutenException(errorMessage);
  }
  return it->spillFixedSize(size);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  ObjectStore::release(iterHandle);
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeAddIteratorSplits( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle,
    jobjectArray batchItrArray) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto outIter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (outIter == nullptr) {
    throw GlutenException("Invalid iterator handle for addSplits");
  }
  
  // Get the underlying split-aware iterator
  auto* splitAwareIter = dynamic_cast<gluten::SplitAwareColumnarBatchIterator*>(outIter->getInputIter());
  if (splitAwareIter == nullptr) {
    throw GlutenException("Iterator does not support split management");
  }

  GLUTEN_CHECK(batchItrArray != nullptr, "FATAL: Splits to add cannot be null");

  // Convert Java ColumnarBatchInIterator[] to native iterators and add as splits
  jsize numIterators = env->GetArrayLength(batchItrArray);
  std::vector<std::shared_ptr<ResultIterator>> inputIterators;
  inputIterators.reserve(numIterators);

  for (jsize idx = 0; idx < numIterators; idx++) {
    jobject iter = env->GetObjectArrayElement(batchItrArray, idx);
    if (iter == nullptr) {
      inputIterators.push_back(nullptr);
    } else {
      auto arrayIter = std::make_unique<JniColumnarBatchIterator>(env, iter, ctx, idx);
      auto resultIter = std::make_shared<ResultIterator>(std::move(arrayIter));
      inputIterators.push_back(std::move(resultIter));
    }
    env->DeleteLocalRef(iter);
  }

  // Add iterator splits via interface method
  if (!inputIterators.empty()) {
    splitAwareIter->addIteratorSplits(inputIterators);
  }

  return true;
  JNI_METHOD_END(false)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeNoMoreSplits( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (iter == nullptr) {
    throw GlutenException("Invalid iterator handle for noMoreSplits");
  }
  ctx->noMoreSplits(iter.get());
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeRequestBarrier( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (iter == nullptr) {
    throw GlutenException("Invalid iterator handle for requestBarrier");
  }
  ctx->requestBarrier(iter.get());
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowInit( // NOLINT
    JNIEnv* env,
    jobject wrapper) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto& conf = ctx->getConfMap();
  int64_t column2RowMemThreshold;
  auto it = conf.find(kColumnarToRowMemoryThreshold);
  GLUTEN_CHECK(!(it == conf.end()), "Required key not found in runtime config: " + kColumnarToRowMemoryThreshold);
  column2RowMemThreshold = std::stoll(it->second);
  // Convert the native batch to Spark unsafe row.
  return ctx->saveObject(ctx->createColumnar2RowConverter(column2RowMemThreshold));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowConvert( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong c2rHandle,
    jlong batchHandle,
    jlong startRow) {
  JNI_METHOD_START
  auto columnarToRowConverter = ObjectStore::retrieve<ColumnarToRowConverter>(c2rHandle);
  auto cb = ObjectStore::retrieve<ColumnarBatch>(batchHandle);

  columnarToRowConverter->convert(cb, startRow);

  const auto& offsets = columnarToRowConverter->getOffsets();
  const auto& lengths = columnarToRowConverter->getLengths();

  auto numRows = columnarToRowConverter->numRows();

  auto offsetsArr = env->NewIntArray(numRows);
  auto offsetsSrc = reinterpret_cast<const jint*>(offsets.data());
  env->SetIntArrayRegion(offsetsArr, 0, numRows, offsetsSrc);
  auto lengthsArr = env->NewIntArray(numRows);
  auto lengthsSrc = reinterpret_cast<const jint*>(lengths.data());
  env->SetIntArrayRegion(lengthsArr, 0, numRows, lengthsSrc);
  long address = reinterpret_cast<long>(columnarToRowConverter->getBufferAddress());

  jobject nativeColumnarToRowInfo =
      env->NewObject(nativeColumnarToRowInfoClass, nativeColumnarToRowInfoConstructor, offsetsArr, lengthsArr, address);
  return nativeColumnarToRowInfo;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong c2rHandle) {
  JNI_METHOD_START
  ObjectStore::release(c2rHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  return ctx->saveObject(ctx->createRow2ColumnarConverter(reinterpret_cast<struct ArrowSchema*>(cSchema)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_nativeConvertRowToColumnar( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle,
    jlongArray rowLength,
    jlong memoryAddress) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  if (rowLength == nullptr) {
    throw GlutenException("Native convert row to columnar: buf_addrs can't be null");
  }
  int numRows = env->GetArrayLength(rowLength);
  auto safeArray = getLongArrayElementsSafe(env, rowLength);
  uint8_t* address = reinterpret_cast<uint8_t*>(memoryAddress);

  auto converter = ObjectStore::retrieve<RowToColumnarConverter>(r2cHandle);
  auto cb = converter->convert(numRows, safeArray.elems(), address);
  return ctx->saveObject(cb);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle) {
  JNI_METHOD_START
  ObjectStore::release(r2cHandle);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_getType( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return env->NewStringUTF(batch->getType().c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numBytes( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return batch->numBytes();
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numColumns( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return batch->numColumns();
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numRows( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return batch->numRows();
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_exportToArrow( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  std::shared_ptr<ArrowSchema> exportedSchema = batch->exportArrowSchema();
  std::shared_ptr<ArrowArray> exportedArray = batch->exportArrowArray();
  ArrowSchemaMove(exportedSchema.get(), reinterpret_cast<struct ArrowSchema*>(cSchema));
  ArrowArrayMove(exportedArray.get(), reinterpret_cast<struct ArrowArray*>(cArray));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_close( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  ObjectStore::release(batchHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_createWithArrowArray( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  std::unique_ptr<ArrowSchema> targetSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> targetArray = std::make_unique<ArrowArray>();
  auto* arrowSchema = reinterpret_cast<ArrowSchema*>(cSchema);
  auto* arrowArray = reinterpret_cast<ArrowArray*>(cArray);
  ArrowArrayMove(arrowArray, targetArray.get());
  ArrowSchemaMove(arrowSchema, targetSchema.get());
  std::shared_ptr<ColumnarBatch> batch =
      std::make_shared<ArrowCStructColumnarBatch>(std::move(targetSchema), std::move(targetArray));
  return ctx->saveObject(batch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_getForEmptySchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numRows) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  return ctx->saveObject(ctx->createOrGetEmptySchemaBatch(static_cast<int32_t>(numRows)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_select( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jintArray jcolumnIndices) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto safeArray = getIntArrayElementsSafe(env, jcolumnIndices);
  int size = env->GetArrayLength(jcolumnIndices);
  std::vector<int32_t> columnIndices;
  columnIndices.reserve(size);
  for (int32_t i = 0; i < size; i++) {
    columnIndices.push_back(safeArray.elems()[i]);
  }

  return ctx->saveObject(ctx->select(ObjectStore::retrieve<ColumnarBatch>(batchHandle), std::move(columnIndices)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_LocalPartitionWriterJniWrapper_createPartitionWriter( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint compressionLevel,
    jint compressionBufferSize,
    jint compressionThreshold,
    jint mergeBufferSize,
    jdouble mergeThreshold,
    jint numSubDirs,
    jint shuffleFileBufferSize,
    jstring dataFileJstr,
    jstring localDirsJstr,
    jboolean enableDictionary) {
  JNI_METHOD_START

  const auto ctx = getRuntime(env, wrapper);

  auto dataFile = jStringToCString(env, dataFileJstr);
  auto localDirs = splitPaths(jStringToCString(env, localDirsJstr));

  auto partitionWriterOptions = std::make_shared<LocalPartitionWriterOptions>(
      shuffleFileBufferSize,
      compressionBufferSize,
      compressionThreshold,
      mergeBufferSize,
      mergeThreshold,
      numSubDirs,
      enableDictionary);

  auto partitionWriter = std::make_shared<LocalPartitionWriter>(
      numPartitions,
      createCompressionCodec(
          getCompressionType(env, codecJstr), getCodecBackend(env, codecBackendJstr), compressionLevel),
      ctx->memoryManager(),
      partitionWriterOptions,
      dataFile,
      std::move(localDirs));

  return ctx->saveObject(partitionWriter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_createHashShuffleWriter(
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring partitioningNameJstr,
    jint startPartitionId,
    jint splitBufferSize,
    jdouble splitBufferReallocThreshold,
    jlong partitionWriterHandle) {
  JNI_METHOD_START
  const auto ctx = getRuntime(env, wrapper);

  auto partitionWriter = ObjectStore::retrieve<PartitionWriter>(partitionWriterHandle);
  if (partitionWriter == nullptr) {
    throw GlutenException("Partition writer handle is invalid: " + std::to_string(partitionWriterHandle));
  }
  ObjectStore::release(partitionWriterHandle);

  auto shuffleWriterOptions = std::make_shared<HashShuffleWriterOptions>(
      toPartitioning(jStringToCString(env, partitioningNameJstr)),
      startPartitionId,
      splitBufferSize,
      splitBufferReallocThreshold);

  return ctx->saveObject(ctx->createShuffleWriter(numPartitions, partitionWriter, shuffleWriterOptions));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_createGpuHashShuffleWriter(
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring partitioningNameJstr,
    jint startPartitionId,
    jint splitBufferSize,
    jdouble splitBufferReallocThreshold,
    jlong partitionWriterHandle) {
  JNI_METHOD_START
  
  const auto ctx = getRuntime(env, wrapper);

  auto partitionWriter = ObjectStore::retrieve<PartitionWriter>(partitionWriterHandle);
  if (partitionWriter == nullptr) {
    throw GlutenException("Partition writer handle is invalid: " + std::to_string(partitionWriterHandle));
  }
  ObjectStore::release(partitionWriterHandle);

  auto shuffleWriterOptions = std::make_shared<GpuHashShuffleWriterOptions>(
      toPartitioning(jStringToCString(env, partitioningNameJstr)),
      startPartitionId,
      splitBufferSize,
      splitBufferReallocThreshold);
  return ctx->saveObject(ctx->createShuffleWriter(numPartitions, partitionWriter, shuffleWriterOptions));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_createSortShuffleWriter(
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring partitioningNameJstr,
    jint startPartitionId,
    jint diskWriteBufferSize,
    jint initialSortBufferSize,
    jboolean useRadixSort,
    jlong partitionWriterHandle) {
  JNI_METHOD_START
  const auto ctx = getRuntime(env, wrapper);

  auto partitionWriter = ObjectStore::retrieve<PartitionWriter>(partitionWriterHandle);
  if (partitionWriter == nullptr) {
    throw GlutenException("Partition writer handle is invalid: " + std::to_string(partitionWriterHandle));
  }
  ObjectStore::release(partitionWriterHandle);

  auto shuffleWriterOptions = std::make_shared<SortShuffleWriterOptions>(
      toPartitioning(jStringToCString(env, partitioningNameJstr)),
      startPartitionId,
      initialSortBufferSize,
      diskWriteBufferSize,
      static_cast<bool>(useRadixSort));

  return ctx->saveObject(ctx->createShuffleWriter(numPartitions, partitionWriter, shuffleWriterOptions));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_createRssSortShuffleWriter(
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring partitioningNameJstr,
    jint startPartitionId,
    jint splitBufferSize,
    jlong sortBufferMaxSize,
    jstring codecJstr,
    jlong partitionWriterHandle) {
  JNI_METHOD_START
  const auto ctx = getRuntime(env, wrapper);

  auto partitionWriter = ObjectStore::retrieve<PartitionWriter>(partitionWriterHandle);
  if (partitionWriter == nullptr) {
    throw GlutenException("Partition writer handle is invalid: " + std::to_string(partitionWriterHandle));
  }
  ObjectStore::release(partitionWriterHandle);

  auto shuffleWriterOptions = std::make_shared<RssSortShuffleWriterOptions>(
      toPartitioning(jStringToCString(env, partitioningNameJstr)),
      startPartitionId,
      splitBufferSize,
      sortBufferMaxSize,
      getCompressionType(env, codecJstr));

  return ctx->saveObject(ctx->createShuffleWriter(numPartitions, partitionWriter, shuffleWriterOptions));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_reclaim( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jlong size) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw GlutenException(errorMessage);
  }
  int64_t evictedSize;
  arrowAssertOkOrThrow(shuffleWriter->reclaimFixedSize(size, &evictedSize), "(shuffle) nativeEvict: evict failed");
  return evictedSize;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jint numRows,
    jlong batchHandle,
    jlong memLimit) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw GlutenException(errorMessage);
  }

  // The column batch maybe VeloxColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  arrowAssertOkOrThrow(shuffleWriter->write(batch, memLimit), "Native write: shuffle writer failed");
  return shuffleWriter->bytesWritten();
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_stop( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw GlutenException(errorMessage);
  }

  arrowAssertOkOrThrow(shuffleWriter->stop(), "Native shuffle write: ShuffleWriter stop failed");

  const auto& partitionLengths = shuffleWriter->partitionLengths();
  auto partitionLengthArr = env->NewLongArray(partitionLengths.size());
  auto src = reinterpret_cast<const jlong*>(partitionLengths.data());
  env->SetLongArrayRegion(partitionLengthArr, 0, partitionLengths.size(), src);

  const auto& rawPartitionLengths = shuffleWriter->rawPartitionLengths();
  auto rawPartitionLengthArr = env->NewLongArray(rawPartitionLengths.size());
  auto rawSrc = reinterpret_cast<const jlong*>(rawPartitionLengths.data());
  env->SetLongArrayRegion(rawPartitionLengthArr, 0, rawPartitionLengths.size(), rawSrc);

  jobject splitResult = env->NewObject(
      splitResultClass,
      splitResultConstructor,
      0L,
      shuffleWriter->totalWriteTime(),
      shuffleWriter->totalEvictTime(),
      shuffleWriter->totalCompressTime(),
      shuffleWriter->totalSortTime(),
      shuffleWriter->totalC2RTime(),
      shuffleWriter->bytesWritten(),
      shuffleWriter->totalBytesEvicted(),
      shuffleWriter->totalBytesToEvict(),
      shuffleWriter->peakBytesAllocated(),
      shuffleWriter->avgDictionaryFields(),
      shuffleWriter->dictionarySize(),
      partitionLengthArr,
      rawPartitionLengthArr);

  return splitResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  ObjectStore::release(shuffleWriterHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_OnHeapJniByteInputStream_memCopyFromHeap( // NOLINT
    JNIEnv* env,
    jobject,
    jbyteArray source,
    jlong destAddress,
    jint size) {
  JNI_METHOD_START
  auto safeArray = getByteArrayElementsSafe(env, source);
  std::memcpy(reinterpret_cast<void*>(destAddress), safeArray.elems(), size);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_make( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jstring compressionType,
    jstring compressionBackend,
    jint batchSize,
    jlong readerBufferSize,
    jlong deserializerBufferSize,
    jstring shuffleWriterType) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  ShuffleReaderOptions options = ShuffleReaderOptions{};
  options.compressionType = getCompressionType(env, compressionType);
  if (compressionType != nullptr) {
    options.codecBackend = getCodecBackend(env, compressionBackend);
  }
  options.batchSize = batchSize;
  options.readerBufferSize = readerBufferSize;
  options.deserializerBufferSize = deserializerBufferSize;

  options.shuffleWriterType = ShuffleWriter::stringToType(jStringToCString(env, shuffleWriterType));
  std::shared_ptr<arrow::Schema> schema =
      arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));

  return ctx->saveObject(ctx->createShuffleReader(schema, options));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_read( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject jStreamReader) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto reader = ObjectStore::retrieve<ShuffleReader>(shuffleReaderHandle);

  auto streamReader = std::make_shared<ShuffleStreamReader>(env, jStreamReader);

  auto outItr = reader->read(streamReader);
  return ctx->saveObject(outItr);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_populateMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject metrics) {
  JNI_METHOD_START
  auto reader = ObjectStore::retrieve<ShuffleReader>(shuffleReaderHandle);
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDecompressTime, reader->getDecompressTime());
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDeserializeTime, reader->getDeserializeTime());

  checkException(env);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle) {
  JNI_METHOD_START
  auto reader = ObjectStore::retrieve<ShuffleReader>(shuffleReaderHandle);
  ObjectStore::release(shuffleReaderHandle);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
  GLUTEN_DCHECK(batch != nullptr, "Cannot find the ColumnarBatch with handle " + std::to_string(handle));

  auto serializer = ctx->createColumnarBatchSerializer(nullptr);
  serializer->append(batch);
  auto serializedSize = serializer->maxSerializedSize();
  auto byteBuffer = env->CallStaticObjectMethod(jniUnsafeByteBufferClass, jniUnsafeByteBufferAllocate, serializedSize);
  auto byteBufferAddress = env->CallLongMethod(byteBuffer, jniUnsafeByteBufferAddress);
  auto byteBufferSize = env->CallLongMethod(byteBuffer, jniUnsafeByteBufferSize);
  serializer->serializeTo(reinterpret_cast<uint8_t*>(byteBufferAddress), byteBufferSize);

  return byteBuffer;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  return ctx->saveObject(ctx->createColumnarBatchSerializer(reinterpret_cast<struct ArrowSchema*>(cSchema)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_deserialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle,
    jbyteArray data) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto serializer = ObjectStore::retrieve<ColumnarBatchSerializer>(serializerHandle);
  GLUTEN_DCHECK(serializer != nullptr, "ColumnarBatchSerializer cannot be null");
  int32_t size = env->GetArrayLength(data);
  auto safeArray = getByteArrayElementsSafe(env, data);
  auto batch = serializer->deserialize(safeArray.elems(), size);
  return ctx->saveObject(batch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_deserializeDirect( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle,
    jlong address,
    jint size) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);

  auto serializer = ObjectStore::retrieve<ColumnarBatchSerializer>(serializerHandle);
  GLUTEN_DCHECK(serializer != nullptr, "ColumnarBatchSerializer cannot be null");
  auto batch = serializer->deserialize((uint8_t*)address, size);
  return ctx->saveObject(batch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle) {
  JNI_METHOD_START
  ObjectStore::release(serializerHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif

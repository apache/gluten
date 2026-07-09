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

#include "BoltInputIterator.h"

#include "compute/BoltBackend.h"
#include "config/BoltConfig.h"
#include "utils/ConfigResolver.h"

namespace gluten {

BoltJniColumnarBatchIterator::BoltJniColumnarBatchIterator(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    Runtime* runtime,
    bool parallelEnabled,
    std::optional<int32_t> iteratorIndex)
    : JniColumnarBatchIterator(env, jColumnarBatchItr, runtime, iteratorIndex), parallelEnabled_(parallelEnabled) {
  if (env->GetJavaVM(&vm_) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }

  if (parallelEnabled_) {
    getContextClassLoader(env);
    getTaskContext(env);
    if (jclass localClass = env->FindClass("org/apache/spark/util/SparkTaskUtil")) {
      jSparkTaskUtilClass_ = reinterpret_cast<jclass>(env->NewGlobalRef(localClass));
      env->DeleteLocalRef(localClass);
    }
  }
}

BoltJniColumnarBatchIterator::~BoltJniColumnarBatchIterator() {
  if (!parallelEnabled_) {
    return;
  }

  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  if (jSparkTaskContext_ != nullptr) {
    env->DeleteGlobalRef(jSparkTaskContext_);
  }
  if (jSparkTaskUtilClass_ != nullptr) {
    env->DeleteGlobalRef(jSparkTaskUtilClass_);
  }
  if (jContextClassLoader_ != nullptr) {
    env->DeleteGlobalRef(jContextClassLoader_);
  }
}

std::shared_ptr<ColumnarBatch> BoltJniColumnarBatchIterator::next() {
  if (parallelEnabled_) {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    setTaskContext(env);
    setClassLoader(env);
  }
  return JniColumnarBatchIterator::next();
}

void BoltJniColumnarBatchIterator::getContextClassLoader(JNIEnv* env) {
  jclass threadClass = env->FindClass("java/lang/Thread");
  if (threadClass == nullptr) {
    LOG(ERROR) << "JNI Error: Could not find java.lang.Thread class";
    return;
  }

  jmethodID currentThreadMethod = env->GetStaticMethodID(threadClass, "currentThread", "()Ljava/lang/Thread;");
  if (currentThreadMethod == nullptr) {
    LOG(ERROR) << "JNI Error: Could not find Thread.currentThread method";
    env->DeleteLocalRef(threadClass);
    return;
  }

  jobject currentThread = env->CallStaticObjectMethod(threadClass, currentThreadMethod);
  if (currentThread == nullptr) {
    LOG(ERROR) << "JNI Error: Call to Thread.currentThread failed";
    env->DeleteLocalRef(threadClass);
    return;
  }

  jmethodID getContextClassLoaderMethod =
      env->GetMethodID(threadClass, "getContextClassLoader", "()Ljava/lang/ClassLoader;");
  if (getContextClassLoaderMethod == nullptr) {
    LOG(ERROR) << "JNI Error: Could not find getContextClassLoader method";
    env->DeleteLocalRef(threadClass);
    env->DeleteLocalRef(currentThread);
    return;
  }

  jobject contextClassLoader = env->CallObjectMethod(currentThread, getContextClassLoaderMethod);
  if (contextClassLoader == nullptr) {
    LOG(ERROR) << "JNI Error: Call to getContextClassLoader failed";
    env->DeleteLocalRef(threadClass);
    env->DeleteLocalRef(currentThread);
    return;
  }

  if (jContextClassLoader_ != nullptr) {
    env->DeleteGlobalRef(jContextClassLoader_);
  }
  jContextClassLoader_ = env->NewGlobalRef(contextClassLoader);
  LOG(INFO) << "Successfully cached the context class loader.";

  env->DeleteLocalRef(threadClass);
  env->DeleteLocalRef(currentThread);
  env->DeleteLocalRef(contextClassLoader);
}

void BoltJniColumnarBatchIterator::getTaskContext(JNIEnv* env) {
  jclass taskContextClass = env->FindClass("org/apache/spark/TaskContext");
  if (taskContextClass == nullptr) {
    LOG(ERROR) << "JNI Error: Could not find org.apache.spark.TaskContext class";
    return;
  }

  jmethodID getMethodID = env->GetStaticMethodID(taskContextClass, "get", "()Lorg/apache/spark/TaskContext;");
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    LOG(ERROR) << "cannot get TaskContext.get";
    env->DeleteLocalRef(taskContextClass);
    return;
  }
  if (!getMethodID) {
    LOG(ERROR) << "cannot get TaskContext.get";
    env->DeleteLocalRef(taskContextClass);
    return;
  }

  jobject taskContext = env->CallStaticObjectMethod(taskContextClass, getMethodID);
  checkException(env);
  if (!taskContext) {
    throw GlutenException("Error: Spark taskContext is null.");
  }

  jSparkTaskContext_ = env->NewGlobalRef(taskContext);
  checkException(env);
  if (!jSparkTaskContext_) {
    throw GlutenException("Error: setting global reference to Spark TaskContext failed");
  }
  LOG(INFO) << "BoltJniColumnarBatchIterator successfully cached Spark TaskContext";
  env->DeleteLocalRef(taskContextClass);
  env->DeleteLocalRef(taskContext);
}

void BoltJniColumnarBatchIterator::setClassLoader(JNIEnv* env) {
  if (!jContextClassLoader_) {
    LOG(ERROR) << "[multi-thread spark] ContextClassLoader is null. Cannot set thread ContextClassLoader";
    return;
  }

  jclass threadClass = env->FindClass("java/lang/Thread");
  jmethodID currentThreadMethod = env->GetStaticMethodID(threadClass, "currentThread", "()Ljava/lang/Thread;");
  jobject currentThread = env->CallStaticObjectMethod(threadClass, currentThreadMethod);
  jmethodID setContextMethod = env->GetMethodID(threadClass, "setContextClassLoader", "(Ljava/lang/ClassLoader;)V");
  env->CallVoidMethod(currentThread, setContextMethod, jContextClassLoader_);

  env->DeleteLocalRef(threadClass);
  env->DeleteLocalRef(currentThread);
}

void BoltJniColumnarBatchIterator::setTaskContext(JNIEnv* env) {
  if (!jSparkTaskContext_) {
    LOG(ERROR) << "[multi-thread spark] jSparkTaskContext_ is null. Cannot set Spark TaskContext";
    return;
  }

  jmethodID setTaskContextMethod =
      env->GetStaticMethodID(jSparkTaskUtilClass_, "setTaskContext", "(Lorg/apache/spark/TaskContext;)V");
  checkException(env);
  if (!setTaskContextMethod) {
    throw GlutenException("cannot get SparkTaskUtil.setTaskContext");
  }
  env->CallStaticVoidMethod(jSparkTaskUtilClass_, setTaskContextMethod, jSparkTaskContext_);
  checkException(env);
}

std::unique_ptr<ShuffleReaderWrapperedIterator> ShuffleReaderWrapperedIterator::tryFrom(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    Runtime* runtime,
    bool parallelEnabled,
    std::optional<int32_t> iteratorIndex) {
  if (env == nullptr || jColumnarBatchItr == nullptr) {
    VLOG(1) << "ShuffleReaderWrapperedIterator cannot wrap: invalid JNI arguments";
    return nullptr;
  }

  try {
    return std::make_unique<ShuffleReaderWrapperedIterator>(
        env, jColumnarBatchItr, runtime, parallelEnabled, iteratorIndex);
  } catch (const std::exception& e) {
    VLOG(1) << "ShuffleReaderWrapperedIterator cannot wrap: " << e.what();
    if (env->ExceptionCheck()) {
      env->ExceptionClear();
    }
    return nullptr;
  }
}

ShuffleReaderWrapperedIterator::ShuffleReaderWrapperedIterator(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    Runtime* runtime,
    bool parallelEnabled,
    std::optional<int32_t> iteratorIndex)
    : BoltJniColumnarBatchIterator(env, jColumnarBatchItr, runtime, parallelEnabled, iteratorIndex) {
  if (env->GetJavaVM(&shuffleVm_) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }

  auto handles = resolveShuffleReaderHandles(env, jColumnarBatchItr);

  streamReader_ = makeShuffleStreamReader(env, handles.streamReader);
  env->DeleteLocalRef(handles.streamReader);

  jclass shuffleReaderIteratorWrapperClass = env->GetObjectClass(handles.wrapper);
  GLUTEN_CHECK(shuffleReaderIteratorWrapperClass != nullptr, "Failed to get ShuffleReaderIteratorWrapper class");
  markAsOffloadedMethod_ = getMethodIdOrError(env, shuffleReaderIteratorWrapperClass, "markAsOffloaded", "()V");
  updateMetricsMethod_ = getMethodIdOrError(env, shuffleReaderIteratorWrapperClass, "updateMetrics", "(JJJJJ)V");
  getReaderInfoMethod_ = getMethodIdOrError(env, shuffleReaderIteratorWrapperClass, "getReaderInfo", "()[B");

  jShuffleReaderIteratorWrapper_ = env->NewGlobalRef(handles.wrapper);
  env->DeleteLocalRef(shuffleReaderIteratorWrapperClass);
  env->DeleteLocalRef(handles.wrapper);
}

ShuffleReaderWrapperedIterator::~ShuffleReaderWrapperedIterator() {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(shuffleVm_, &env);
  if (jShuffleReaderIteratorWrapper_ != nullptr) {
    env->DeleteGlobalRef(jShuffleReaderIteratorWrapper_);
  }
}

void ShuffleReaderWrapperedIterator::markAsOffloaded() {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(shuffleVm_, &env);
  env->CallVoidMethod(jShuffleReaderIteratorWrapper_, markAsOffloadedMethod_);
  checkException(env);
}

void ShuffleReaderWrapperedIterator::updateMetrics(
    int64_t numRows,
    int64_t numBatchesTotal,
    int64_t decompressTime,
    int64_t deserializeTime,
    int64_t totalReadTime) {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(shuffleVm_, &env);
  env->CallVoidMethod(
      jShuffleReaderIteratorWrapper_,
      updateMetricsMethod_,
      static_cast<jlong>(numRows),
      static_cast<jlong>(numBatchesTotal),
      static_cast<jlong>(decompressTime),
      static_cast<jlong>(deserializeTime),
      static_cast<jlong>(totalReadTime));
  checkException(env);
}

std::string ShuffleReaderWrapperedIterator::getRawReaderInfo() {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(shuffleVm_, &env);
  auto infoArray = static_cast<jbyteArray>(env->CallObjectMethod(jShuffleReaderIteratorWrapper_, getReaderInfoMethod_));
  checkException(env);
  if (infoArray == nullptr) {
    return {};
  }
  jsize length = env->GetArrayLength(infoArray);
  std::string info(length, '\0');
  if (length > 0) {
    env->GetByteArrayRegion(infoArray, 0, length, reinterpret_cast<jbyte*>(info.data()));
  }
  env->DeleteLocalRef(infoArray);
  return info;
}

std::shared_ptr<StreamReader> ShuffleReaderWrapperedIterator::getStreamReader() {
  return streamReader_;
}

ShuffleReaderWrapperedIterator::ShuffleReaderHandles ShuffleReaderWrapperedIterator::resolveShuffleReaderHandles(
    JNIEnv* env,
    jobject iterator) {
  GLUTEN_CHECK(env != nullptr, "JNIEnv is null when resolving ShuffleReaderInIterator");
  GLUTEN_CHECK(iterator != nullptr, "Iterator is null when resolving ShuffleReaderInIterator");

  jclass shuffleReaderInIteratorClass = env->FindClass("org/apache/gluten/backendsapi/bolt/ShuffleReaderInIterator");
  GLUTEN_CHECK(shuffleReaderInIteratorClass != nullptr && !env->ExceptionCheck(), "ShuffleReaderInIterator not found");

  bool isWrapper = env->IsInstanceOf(iterator, shuffleReaderInIteratorClass);
  env->DeleteLocalRef(shuffleReaderInIteratorClass);
  GLUTEN_CHECK(isWrapper, "Iterator is not ShuffleReaderInIterator");

  auto resolveWrapperMethod = [&](const char* name) -> jmethodID {
    jclass iteratorClass = env->GetObjectClass(iterator);
    GLUTEN_CHECK(iteratorClass != nullptr, "Failed to get ShuffleReaderInIterator class");
    jmethodID method =
        env->GetMethodID(iteratorClass, name, "()Lorg/apache/spark/shuffle/ShuffleReaderIteratorWrapper;");
    env->DeleteLocalRef(iteratorClass);
    return method;
  };

  jmethodID getWrapperMethod = resolveWrapperMethod("getReaderWrapper");
  if (getWrapperMethod == nullptr || env->ExceptionCheck()) {
    env->ExceptionClear();
    getWrapperMethod = resolveWrapperMethod("readerWrapper");
    GLUTEN_CHECK(
        getWrapperMethod != nullptr && !env->ExceptionCheck(), "ShuffleReaderInIterator#getReaderWrapper unavailable");
  }

  jobject wrapper = env->CallObjectMethod(iterator, getWrapperMethod);
  checkException(env);
  GLUTEN_CHECK(wrapper != nullptr, "ShuffleReaderInIterator#getReaderWrapper returned null");

  jclass wrapperClass = env->GetObjectClass(wrapper);
  GLUTEN_CHECK(wrapperClass != nullptr, "Failed to get ShuffleReaderIteratorWrapper class");

  jmethodID getStreamReaderMethod =
      env->GetMethodID(wrapperClass, "getStreamReader", "()Lorg/apache/gluten/vectorized/ShuffleStreamReader;");
  GLUTEN_CHECK(getStreamReaderMethod != nullptr, "ShuffleReaderIteratorWrapper#getStreamReader unavailable");

  jobject streamReader = env->CallObjectMethod(wrapper, getStreamReaderMethod);
  checkException(env);
  GLUTEN_CHECK(streamReader != nullptr, "ShuffleReaderIteratorWrapper#getStreamReader returned null");

  env->DeleteLocalRef(wrapperClass);
  return ShuffleReaderHandles{wrapper, streamReader};
}

namespace {
std::unique_ptr<ColumnarBatchIterator>
createBoltInputIterator(JNIEnv* env, jobject jColumnarBatchItr, Runtime* runtime, int32_t iteratorIndex) {
  const auto& conf = runtime->getConfMap();
  bool parallelEnabled = getBoolConfigValue(conf, kGlutenEnableParallel, false);
  LOG(INFO) << "nativeCreateKernelWithIterator parallelEnabled=" << parallelEnabled;

  auto shuffleReaderIter =
      ShuffleReaderWrapperedIterator::tryFrom(env, jColumnarBatchItr, runtime, parallelEnabled, iteratorIndex);
  if (shuffleReaderIter != nullptr) {
    LOG(INFO) << "Wrap ShuffleReaderWrapperedIterator for input iterator " << iteratorIndex;
    return shuffleReaderIter;
  }
  return std::make_unique<BoltJniColumnarBatchIterator>(
      env, jColumnarBatchItr, runtime, parallelEnabled, iteratorIndex);
}
} // namespace

void registerBoltInputIteratorFactory() {
  registerInputIteratorFactory(kBoltBackendKind, createBoltInputIterator);
}

} // namespace gluten

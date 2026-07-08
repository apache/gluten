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

#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <execinfo.h>
#include <jni.h>

#include <functional>

#include "compute/ProtobufUtils.h"
#include "compute/Runtime.h"
#include "memory/AllocationListener.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/rss/RssClient.h"
#include "threads/ThreadInitializer.h"
#include "utils/Compression.h"
#include "utils/Exception.h"
#include "utils/ResourceMap.h"

static jint jniVersion = JNI_VERSION_1_8;

static inline std::string jStringToCString(JNIEnv* env, jstring string) {
  if (!string) {
    return {};
  }

  const char* chars = env->GetStringUTFChars(string, nullptr);
  if (chars == nullptr) {
    // OOM During GetStringUTFChars.
    throw gluten::GlutenException("Error occurred during GetStringUTFChars. Probably OOM.");
  }

  std::string result(chars);
  env->ReleaseStringUTFChars(string, chars);
  return result;
}

static inline void checkException(JNIEnv* env) {
  if (env->ExceptionCheck()) {
    jthrowable t = env->ExceptionOccurred();
    env->ExceptionClear();

    jclass describerClass = env->FindClass("org/apache/gluten/exception/JniExceptionDescriber");
    jmethodID describeMethod =
        env->GetStaticMethodID(describerClass, "describe", "(Ljava/lang/Throwable;)Ljava/lang/String;");

    std::stringstream message;
    message << "Error during calling Java code from native code: ";

    const auto description = static_cast<jstring>(env->CallStaticObjectMethod(describerClass, describeMethod, t));

    if (env->ExceptionCheck()) {
      message << "Uncaught Java exception during calling the Java exception describer method!";
      env->ExceptionClear();
    } else {
      try {
        message << jStringToCString(env, description);
      } catch (const std::exception& e) {
        message << e.what();
      }
    }

    throw gluten::GlutenException(message.str());
  }
}

static inline jclass createGlobalClassReference(JNIEnv* env, const char* className) {
  jclass localClass = env->FindClass(className);
  jclass globalClass = (jclass)env->NewGlobalRef(localClass);
  env->DeleteLocalRef(localClass);
  return globalClass;
}

static inline jclass createGlobalClassReferenceOrError(JNIEnv* env, const char* className) {
  jclass globalClass = createGlobalClassReference(env, className);
  if (globalClass == nullptr) {
    std::string errorMessage = "Unable to create global class reference  for" + std::string(className);
    throw gluten::GlutenException(errorMessage);
  }
  return globalClass;
}

static inline jmethodID getMethodId(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(thisClass, name, sig);
  return ret;
}

static inline jmethodID getMethodIdOrError(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  if (thisClass == nullptr) {
    throw gluten::GlutenException("Unable to find class while looking up method " + std::string(name));
  }
  jmethodID ret = getMethodId(env, thisClass, name, sig);
  if (ret == nullptr) {
    std::string errorMessage = "Unable to find method " + std::string(name) + " within signature" + std::string(sig);
    throw gluten::GlutenException(errorMessage);
  }
  return ret;
}

static inline jmethodID getStaticMethodId(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = env->GetStaticMethodID(thisClass, name, sig);
  return ret;
}

static inline jmethodID getStaticMethodIdOrError(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = getStaticMethodId(env, thisClass, name, sig);
  if (ret == nullptr) {
    std::string errorMessage =
        "Unable to find static method " + std::string(name) + " within signature" + std::string(sig);
    throw gluten::GlutenException(errorMessage);
  }
  return ret;
}

static inline void attachCurrentThreadAsDaemonOrThrow(JavaVM* vm, JNIEnv** out) {
  int getEnvStat = vm->GetEnv(reinterpret_cast<void**>(out), jniVersion);
  if (getEnvStat == JNI_EDETACHED) {
    DLOG(INFO) << "JNIEnv was not attached to current thread.";
    // Reattach current thread to JVM
    getEnvStat = vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(out), NULL);
    if (getEnvStat != JNI_OK) {
      throw gluten::GlutenException("Failed to reattach current thread to JVM.");
    }
    DLOG(INFO) << "Succeeded attaching current thread.";
    return;
  }
  if (getEnvStat != JNI_OK) {
    throw gluten::GlutenException("Failed to attach current thread to JVM.");
  }
}

template <typename T>
static T* jniCastOrThrow(jlong handle) {
  auto instance = reinterpret_cast<T*>(handle);
  GLUTEN_CHECK(instance != nullptr, "FATAL: resource instance should not be null.");
  return instance;
}

namespace gluten {

class ShuffleStreamReader final : public StreamReader {
 public:
  ShuffleStreamReader(JNIEnv* env, jobject reader);
  ~ShuffleStreamReader() override;

  std::shared_ptr<arrow::io::InputStream> readNextStream(arrow::MemoryPool* pool) override;

 private:
  JavaVM* vm_{nullptr};
  jobject ref_{nullptr};
};

class JniCommonState {
 public:
  virtual ~JniCommonState() = default;

  void ensureInitialized(JNIEnv* env);

  void assertInitialized() const;

  void close();

  jmethodID runtimeAwareCtxHandle();

  jmethodID jniByteInputStreamRead();

  jmethodID jniByteInputStreamTell();

  jmethodID jniByteInputStreamClose();

  jmethodID shuffleStreamReaderNextStream();

  JavaVM* getJavaVM() const {
    return vm_;
  }

 private:
  void initialize(JNIEnv* env);

  jclass runtimeAwareClass_;
  jmethodID runtimeAwareCtxHandle_;

  jclass jniByteInputStreamClass_;
  jmethodID jniByteInputStreamRead_;
  jmethodID jniByteInputStreamTell_;
  jmethodID jniByteInputStreamClose_;

  jclass shuffleStreamReaderClass_;
  jmethodID shuffleStreamReaderNextStream_;

  JavaVM* vm_;
  bool initialized_{false};
  bool closed_{false};
  std::mutex mtx_;
};

inline JniCommonState* getJniCommonState() {
  static JniCommonState jniCommonState;
  return &jniCommonState;
}

Runtime* getRuntime(JNIEnv* env, jobject runtimeAware);

using JniInputIteratorFactory = std::function<
    std::unique_ptr<ColumnarBatchIterator>(JNIEnv* env, jobject iterator, Runtime* runtime, int32_t iteratorIndex)>;

void registerJniInputIteratorFactory(const std::string& runtimeKind, JniInputIteratorFactory factory);

std::unique_ptr<ColumnarBatchIterator>
createJniInputIterator(JNIEnv* env, jobject iterator, Runtime* runtime, int32_t iteratorIndex);

// Safe version of JNI {Get|Release}<PrimitiveType>ArrayElements routines.
// SafeNativeArray would release the managed array elements automatically
// during destruction.

enum class JniPrimitiveArrayType {
  kBoolean = 0,
  kByte = 1,
  kChar = 2,
  kShort = 3,
  kInt = 4,
  kLong = 5,
  kFloat = 6,
  kDouble = 7
};

#define CONCATENATE(t1, t2, t3) t1##t2##t3

#define DEFINE_PRIMITIVE_ARRAY(PRIM_TYPE, JAVA_TYPE, JNI_NATIVE_TYPE, NATIVE_TYPE, METHOD_VAR) \
  template <>                                                                                  \
  struct JniPrimitiveArray<JniPrimitiveArrayType::PRIM_TYPE> {                                 \
    using JavaType = JAVA_TYPE;                                                                \
    using JniNativeType = JNI_NATIVE_TYPE;                                                     \
    using NativeType = NATIVE_TYPE;                                                            \
                                                                                               \
    static JniNativeType get(JNIEnv* env, JavaType javaArray) {                                \
      return env->CONCATENATE(Get, METHOD_VAR, ArrayElements)(javaArray, nullptr);             \
    }                                                                                          \
                                                                                               \
    static void release(JNIEnv* env, JavaType javaArray, JniNativeType nativeArray) {          \
      env->CONCATENATE(Release, METHOD_VAR, ArrayElements)(javaArray, nativeArray, JNI_ABORT); \
    }                                                                                          \
  };

template <JniPrimitiveArrayType TYPE>
struct JniPrimitiveArray {};

DEFINE_PRIMITIVE_ARRAY(kBoolean, jbooleanArray, jboolean*, bool*, Boolean)
DEFINE_PRIMITIVE_ARRAY(kByte, jbyteArray, jbyte*, uint8_t*, Byte)
DEFINE_PRIMITIVE_ARRAY(kChar, jcharArray, jchar*, uint16_t*, Char)
DEFINE_PRIMITIVE_ARRAY(kShort, jshortArray, jshort*, int16_t*, Short)
DEFINE_PRIMITIVE_ARRAY(kInt, jintArray, jint*, int32_t*, Int)
DEFINE_PRIMITIVE_ARRAY(kLong, jlongArray, jlong*, int64_t*, Long)
DEFINE_PRIMITIVE_ARRAY(kFloat, jfloatArray, jfloat*, float_t*, Float)
DEFINE_PRIMITIVE_ARRAY(kDouble, jdoubleArray, jdouble*, double_t*, Double)

template <JniPrimitiveArrayType TYPE>
class SafeNativeArray {
  using PrimitiveArray = JniPrimitiveArray<TYPE>;
  using JavaArrayType = typename PrimitiveArray::JavaType;
  using JniNativeArrayType = typename PrimitiveArray::JniNativeType;
  using NativeArrayType = typename PrimitiveArray::NativeType;

 public:
  virtual ~SafeNativeArray() {
    PrimitiveArray::release(env_, javaArray_, nativeArray_);
    env_->DeleteLocalRef(javaArray_);
  }

  SafeNativeArray(const SafeNativeArray&) = delete;
  SafeNativeArray(SafeNativeArray&&) = delete;
  SafeNativeArray& operator=(const SafeNativeArray&) = delete;
  SafeNativeArray& operator=(SafeNativeArray&&) = delete;

  const NativeArrayType elems() const {
    return reinterpret_cast<const NativeArrayType>(nativeArray_);
  }

  const jsize length() const {
    return env_->GetArrayLength(javaArray_);
  }

  static SafeNativeArray<TYPE> get(JNIEnv* env, JavaArrayType javaArray) {
    JniNativeArrayType nativeArray = PrimitiveArray::get(env, javaArray);
    return SafeNativeArray<TYPE>(env, javaArray, nativeArray);
  }

 private:
  SafeNativeArray(JNIEnv* env, JavaArrayType javaArray, JniNativeArrayType nativeArray)
      : env_(env), javaArray_(static_cast<JavaArrayType>(env_->NewLocalRef(javaArray))), nativeArray_(nativeArray){};

  JNIEnv* env_;
  JavaArrayType javaArray_;
  JniNativeArrayType nativeArray_;
};

#define DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(PRIM_TYPE, JAVA_TYPE, METHOD_VAR)                         \
  inline SafeNativeArray<JniPrimitiveArrayType::PRIM_TYPE> CONCATENATE(get, METHOD_VAR, ArrayElementsSafe)( \
      JNIEnv * env, JAVA_TYPE array) {                                                                      \
    return SafeNativeArray<JniPrimitiveArrayType::PRIM_TYPE>::get(env, array);                              \
  }

DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kBoolean, jbooleanArray, Boolean)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kByte, jbyteArray, Byte)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kChar, jcharArray, Char)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kShort, jshortArray, Short)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kInt, jintArray, Int)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kLong, jlongArray, Long)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kFloat, jfloatArray, Float)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kDouble, jdoubleArray, Double)

class JniColumnarBatchIterator : public ColumnarBatchIterator {
 public:
  explicit JniColumnarBatchIterator(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      Runtime* runtime,
      bool parallelEnabled,
      std::optional<int32_t> iteratorIndex = std::nullopt);

  // singleton
  JniColumnarBatchIterator(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator(JniColumnarBatchIterator&&) = delete;
  JniColumnarBatchIterator& operator=(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator& operator=(JniColumnarBatchIterator&&) = delete;

  ~JniColumnarBatchIterator() override;

  // multi-thread spark: get a global reference to Spark ContextClassLoader
  void getContextClassLoader(JNIEnv* env) {
    // 1. Get the java.lang.Thread class
    jclass threadClass = env->FindClass("java/lang/Thread");
    if (threadClass == nullptr) {
      LOG(ERROR) << "JNI Error: Could not find java.lang.Thread class";
      return;
    }
    // 2. Get the static method ID for Thread.currentThread()
    jmethodID currentThreadMethod = env->GetStaticMethodID(threadClass, "currentThread", "()Ljava/lang/Thread;");
    if (currentThreadMethod == nullptr) {
      LOG(ERROR) << "JNI Error: Could not find Thread.currentThread method";
      return;
    }
    // 3. Call Thread.currentThread() to get the current thread object
    jobject currentThread = env->CallStaticObjectMethod(threadClass, currentThreadMethod);
    if (currentThread == nullptr) {
      LOG(ERROR) << "JNI Error: Call to Thread.currentThread failed";
      return;
    }
    // 4. Get the method ID for getContextClassLoader()
    jmethodID getContextClassLoaderMethod =
        env->GetMethodID(threadClass, "getContextClassLoader", "()Ljava/lang/ClassLoader;");
    if (getContextClassLoaderMethod == nullptr) {
      LOG(ERROR) << "JNI Error: Could not find getContextClassLoader method";
      return;
    }
    // 5. Call getContextClassLoader() on the current thread object
    jobject contextClassLoader = env->CallObjectMethod(currentThread, getContextClassLoaderMethod);
    if (contextClassLoader == nullptr) {
      LOG(ERROR) << "JNI Error: Call to getContextClassLoader failed";
      return;
    }
    // 6. Create a global reference to the ClassLoader. This is the crucial step.
    //    This new reference will not be garbage collected until explicitly freed.
    if (jContextClassLoader_ != nullptr) {
      env->DeleteGlobalRef(jContextClassLoader_); // Clean up any old reference
    }
    jContextClassLoader_ = env->NewGlobalRef(contextClassLoader);
    LOG(INFO) << "Successfully cached the context class loader.";
    // Clean up the local references we created
    env->DeleteLocalRef(threadClass);
    env->DeleteLocalRef(currentThread);
    env->DeleteLocalRef(contextClassLoader);
  }
  // multi-thread spark: get a global reference to Spark taskContext
  void getTaskContext(JNIEnv* env) {
    // 1. Find class org.apache.spark.TaskContext
    jclass taskContextClass = env->FindClass("org/apache/spark/TaskContext");
    if (taskContextClass == nullptr) {
      LOG(ERROR) << "JNI Error: Could not find org.apache.spark.TaskContext class";
      return;
    }
    // 2. Get the static method ID for TaskContext.get()
    // Signature for static get(): ()Lorg/apache/spark/TaskContext;
    jmethodID getMethodID = env->GetStaticMethodID(taskContextClass, "get", "()Lorg/apache/spark/TaskContext;");
    if (env->ExceptionCheck()) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(ERROR) << "cannot get TaskContext.get";
      env->DeleteLocalRef(taskContextClass);
    }
    if (!getMethodID) {
      LOG(ERROR) << "cannot get TaskContext.get";
    }
    // 3. Call the static method
    jobject taskContext = env->CallStaticObjectMethod(taskContextClass, getMethodID);
    checkException(env);
    if (!taskContext) {
      std::string errorMessage = "Error: Spark taskContext is null.";
      throw gluten::GlutenException(errorMessage);
    }
    jSparkTaskContext_ = env->NewGlobalRef(taskContext);
    checkException(env);
    if (!jSparkTaskContext_) {
      std::string errorMessage = "Error: setting global reference to Spark TaskContex failst";
      throw gluten::GlutenException(errorMessage);
    }
    LOG(INFO) << "JniColumnarBatchIterator Successfully set global reference to Spark TaskContext";
    env->DeleteLocalRef(taskContextClass);
  }
  // multi-thread spark: proactively set context class loader upon thread switch
  void setClassLoader(JNIEnv* env) {
    if (!jContextClassLoader_) {
      LOG(ERROR) << "[multi-thread spark] ContextClassLoader is null. Cannot set thread ContextClassLoader";
      return;
    }
    // 1. Get the current Thread object
    jclass threadClass = env->FindClass("java/lang/Thread");
    jmethodID currentThreadMethod = env->GetStaticMethodID(threadClass, "currentThread", "()Ljava/lang/Thread;");
    jobject currentThread = env->CallStaticObjectMethod(threadClass, currentThreadMethod);
    // 2. Get the setContextClassLoader method
    jclass classLoaderClass = env->FindClass("java/lang/ClassLoader");
    jmethodID setContextMethod = env->GetMethodID(threadClass, "setContextClassLoader", "(Ljava/lang/ClassLoader;)V");
    // 3. Set the context classloader to our cached application loader
    env->CallVoidMethod(currentThread, setContextMethod, jContextClassLoader_);
    // Cleanup local references
    env->DeleteLocalRef(threadClass);
    env->DeleteLocalRef(classLoaderClass);
    env->DeleteLocalRef(currentThread);
  }
  void setTaskContext(JNIEnv* env) {
    if (!jSparkTaskContext_) {
      LOG(ERROR) << "[multi-thread spark] jSparkTaskContext_ is null. Cannot set Spark TaskContext";
      return;
    }
    // 2. Find setTaskContext method
    jmethodID setTaskContextMethod =
        env->GetStaticMethodID(jSparkTaskUtilClass_, "setTaskContext", "(Lorg/apache/spark/TaskContext;)V");
    checkException(env);
    if (!setTaskContextMethod) {
      std::string errorMessage = "cannot get TaskContext.get";
      throw gluten::GlutenException(errorMessage);
    }
    env->CallVoidMethod(jSparkTaskUtilClass_, setTaskContextMethod, jSparkTaskContext_);
    checkException(env);
  }

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  class ColumnarBatchIteratorDumper final : public ColumnarBatchIterator {
   public:
    ColumnarBatchIteratorDumper(JniColumnarBatchIterator* self) : self_(self){};

    std::shared_ptr<ColumnarBatch> next() override {
      return self_->nextInternal();
    }

   private:
    JniColumnarBatchIterator* self_;
  };

  std::shared_ptr<ColumnarBatch> nextInternal() const;

  JavaVM* vm_;
  jobject jColumnarBatchItr_;
  Runtime* runtime_;
  bool parallelEnabled_{false}; // [multi-thread spark]
  std::optional<int32_t> iteratorIndex_;
  const bool shouldDump_;

  jclass serializedColumnarBatchIteratorClass_;
  jmethodID serializedColumnarBatchIteratorHasNext_;
  jmethodID serializedColumnarBatchIteratorNext_;

  std::shared_ptr<ColumnarBatchIterator> dumpedIteratorReader_{nullptr};

  // [multi-thread spark]
  jobject jSparkTaskContext_{nullptr};
  jclass jSparkTaskUtilClass_{nullptr};
  jobject jContextClassLoader_{nullptr};
};

// A wrapper of ShuffleReaderIteratorWrapper JNI iterator
class ShuffleReaderWrapperedIterator : public JniColumnarBatchIterator {
 public:
  static std::unique_ptr<ShuffleReaderWrapperedIterator> tryFrom(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      Runtime* runtime,
      bool parallelEnabled,
      std::optional<int32_t> iteratorIndex = std::nullopt) {
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

  explicit ShuffleReaderWrapperedIterator(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      Runtime* runtime,
      bool parallelEnabled,
      std::optional<int32_t> iteratorIndex = std::nullopt)
      : JniColumnarBatchIterator(env, jColumnarBatchItr, runtime, parallelEnabled, iteratorIndex) {
    if (env->GetJavaVM(&shuffleVm_) != JNI_OK) {
      throw gluten::GlutenException("Unable to get JavaVM instance");
    }

    auto handles = resolveShuffleReaderHandles(env, jColumnarBatchItr);

    streamReader_ = makeShuffleStreamReader(env, handles.streamReader);
    env->DeleteLocalRef(handles.streamReader);

    jclass shuffleReaderIteratorWrapperClass = env->GetObjectClass(handles.wrapper);
    GLUTEN_CHECK(shuffleReaderIteratorWrapperClass != nullptr, "Failed to get ShuffleReaderIteratorWrapper class");
    markAsOffloadedMethod_ =
        getMethodIdOrError(env, shuffleReaderIteratorWrapperClass, "markAsOffloaded", "()V");
    updateMetricsMethod_ =
        getMethodIdOrError(env, shuffleReaderIteratorWrapperClass, "updateMetrics", "(JJJJJ)V");
    getReaderInfoMethod_ = getMethodIdOrError(env, shuffleReaderIteratorWrapperClass, "getReaderInfo", "()[B");

    jShuffleReaderIteratorWrapper_ = env->NewGlobalRef(handles.wrapper);
    env->DeleteLocalRef(shuffleReaderIteratorWrapperClass);
    env->DeleteLocalRef(handles.wrapper);
  }

  // singleton
  ShuffleReaderWrapperedIterator(const ShuffleReaderWrapperedIterator&) = delete;
  ShuffleReaderWrapperedIterator(ShuffleReaderWrapperedIterator&&) = delete;
  ShuffleReaderWrapperedIterator& operator=(const ShuffleReaderWrapperedIterator&) = delete;
  ShuffleReaderWrapperedIterator& operator=(ShuffleReaderWrapperedIterator&&) = delete;

  ~ShuffleReaderWrapperedIterator() override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(shuffleVm_, &env);
    if (jShuffleReaderIteratorWrapper_ != nullptr) {
      env->DeleteGlobalRef(jShuffleReaderIteratorWrapper_);
    }
  }

  void markAsOffloaded() {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(shuffleVm_, &env);
    env->CallVoidMethod(jShuffleReaderIteratorWrapper_, markAsOffloadedMethod_);
    checkException(env);
  }

  void updateMetrics(
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

  std::string getRawReaderInfo() {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(shuffleVm_, &env);
    auto infoArray = static_cast<jbyteArray>(
        env->CallObjectMethod(jShuffleReaderIteratorWrapper_, getReaderInfoMethod_));
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

  std::shared_ptr<StreamReader> getStreamReader() {
    return streamReader_;
  }

 private:
  struct ShuffleReaderHandles {
    jobject wrapper;
    jobject streamReader;
  };

  static ShuffleReaderHandles resolveShuffleReaderHandles(JNIEnv* env, jobject iterator) {
    GLUTEN_CHECK(env != nullptr, "JNIEnv is null when resolving ShuffleReaderInIterator");
    GLUTEN_CHECK(iterator != nullptr, "Iterator is null when resolving ShuffleReaderInIterator");

    jclass shuffleReaderInIteratorClass =
        env->FindClass("org/apache/gluten/backendsapi/bolt/ShuffleReaderInIterator");
    GLUTEN_CHECK(shuffleReaderInIteratorClass != nullptr && !env->ExceptionCheck(), "ShuffleReaderInIterator not found");

    bool isWrapper = env->IsInstanceOf(iterator, shuffleReaderInIteratorClass);
    env->DeleteLocalRef(shuffleReaderInIteratorClass);
    GLUTEN_CHECK(isWrapper, "Iterator is not ShuffleReaderInIterator");

    auto resolveWrapperMethod = [&](const char* name) -> jmethodID {
      jclass iteratorClass = env->GetObjectClass(iterator);
      GLUTEN_CHECK(iteratorClass != nullptr, "Failed to get ShuffleReaderInIterator class");
      jmethodID method = env->GetMethodID(
          iteratorClass, name, "()Lorg/apache/spark/shuffle/ShuffleReaderIteratorWrapper;");
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

    jmethodID getStreamReaderMethod = env->GetMethodID(
        wrapperClass, "getStreamReader", "()Lorg/apache/gluten/vectorized/ShuffleStreamReader;");
    GLUTEN_CHECK(getStreamReaderMethod != nullptr, "ShuffleReaderIteratorWrapper#getStreamReader unavailable");

    jobject streamReader = env->CallObjectMethod(wrapper, getStreamReaderMethod);
    checkException(env);
    GLUTEN_CHECK(streamReader != nullptr, "ShuffleReaderIteratorWrapper#getStreamReader returned null");

    env->DeleteLocalRef(wrapperClass);
    return ShuffleReaderHandles{wrapper, streamReader};
  }

  JavaVM* shuffleVm_{nullptr};
  jobject jShuffleReaderIteratorWrapper_{nullptr};
  jmethodID markAsOffloadedMethod_{nullptr};
  jmethodID updateMetricsMethod_{nullptr};
  jmethodID getReaderInfoMethod_{nullptr};
  std::shared_ptr<StreamReader> streamReader_{nullptr};
};

std::unique_ptr<JniColumnarBatchIterator>
makeJniColumnarBatchIterator(JNIEnv* env, jobject jColumnarBatchItr, Runtime* runtime, bool parallelEnabled);
} // namespace gluten

// TODO: Move the static functions to namespace gluten

static inline void backtrace() {
  void* array[1024];
  auto size = backtrace(array, 1024);
  char** strings = backtrace_symbols(array, size);
  for (size_t i = 0; i < size; ++i) {
    LOG(INFO) << strings[i];
  }
  free(strings);
}

static inline std::vector<std::string> ToStringVector(JNIEnv* env, jobjectArray& str_array) {
  std::vector<std::string> vector;
  if (str_array == NULL) {
    return vector;
  }
  int length = env->GetArrayLength(str_array);
  for (int i = 0; i < length; i++) {
    auto string = reinterpret_cast<jstring>(env->GetObjectArrayElement(str_array, i));
    vector.push_back(jStringToCString(env, string));
  }
  return vector;
}
static inline std::vector<std::string> FromByteArrToStringVector(JNIEnv* env, jobjectArray& str_array) {
  std::vector<std::string> vector;
  if (str_array == NULL) {
    return vector;
  }
  int length = env->GetArrayLength(str_array);
  for (int i = 0; i < length; i++) {
    jbyteArray byte_array = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(str_array, i));
    int bytes_len = env->GetArrayLength(byte_array);
    signed char array[bytes_len];
    env->GetByteArrayRegion(byte_array, 0, bytes_len, array);
    std::string j_string(reinterpret_cast<char*>(array), sizeof(array));
    vector.push_back(j_string);
  }
  return vector;
}

static inline arrow::Compression::type getCompressionType(JNIEnv* env, jstring codecJstr) {
  if (codecJstr == NULL) {
    return arrow::Compression::UNCOMPRESSED;
  }
  auto codec = env->GetStringUTFChars(codecJstr, JNI_FALSE);

  // Convert codec string into lowercase.
  std::string codecLower;
  std::transform(codec, codec + std::strlen(codec), std::back_inserter(codecLower), ::tolower);
  GLUTEN_ASSIGN_OR_THROW(auto compressionType, arrow::util::Codec::GetCompressionType(codecLower));

  env->ReleaseStringUTFChars(codecJstr, codec);
  return compressionType;
}

static inline gluten::CodecBackend getCodecBackend(JNIEnv* env, jstring codecBackendJstr) {
  if (codecBackendJstr == nullptr) {
    return gluten::CodecBackend::NONE;
  }
  auto codecBackend = jStringToCString(env, codecBackendJstr);
  if (codecBackend == "qat") {
    return gluten::CodecBackend::QAT;
  }
  throw std::invalid_argument("Not support this codec backend " + codecBackend);
}

/*
NOTE: the class must be thread safe
 */

class SparkAllocationListener final : public gluten::AllocationListener {
 public:
  SparkAllocationListener(JavaVM* vm, jobject jListenerLocalRef) : vm_(vm) {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jListenerGlobalRef_ = env->NewGlobalRef(jListenerLocalRef);
    GLUTEN_CHECK(jListenerGlobalRef_ != nullptr, "Failed to create global reference for reservation listener.");
    // Resolve JNI symbols on the creator thread to avoid first-touch initialization from IO threads.
    (void)reserveMemoryMethod(env);
    (void)unreserveMemoryMethod(env);
  }

  SparkAllocationListener(const SparkAllocationListener&) = delete;
  SparkAllocationListener(SparkAllocationListener&&) = delete;
  SparkAllocationListener& operator=(const SparkAllocationListener&) = delete;
  SparkAllocationListener& operator=(SparkAllocationListener&&) = delete;

  ~SparkAllocationListener() override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      LOG(WARNING) << "SparkAllocationListener#~SparkAllocationListener(): "
                   << "JNIEnv was not attached to current thread";
      return;
    }
    env->DeleteGlobalRef(jListenerGlobalRef_);
  }

  void allocationChanged(int64_t size) override {
    if (size == 0) {
      return;
    }
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (size < 0) {
      env->CallLongMethod(jListenerGlobalRef_, unreserveMemoryMethod(env), -size);
      checkException(env);
    } else {
      env->CallLongMethod(jListenerGlobalRef_, reserveMemoryMethod(env), size);
      checkException(env);
    }
    usedBytes_ += size;
    while (true) {
      int64_t savedPeakBytes = peakBytes_;
      int64_t savedUsedBytes = usedBytes_;
      if (savedUsedBytes <= savedPeakBytes) {
        break;
      }
      // usedBytes_ > savedPeakBytes, update peak
      if (peakBytes_.compare_exchange_weak(savedPeakBytes, savedUsedBytes)) {
        break;
      }
    }
  }

  int64_t currentBytes() override {
    return usedBytes_;
  }

  int64_t peakBytes() override {
    return peakBytes_;
  }

 private:
  jclass javaReservationListenerClass(JNIEnv* env) {
    static jclass javaReservationListenerClass = createGlobalClassReference(
        env,
        "Lorg/apache/gluten/memory/listener/"
        "ReservationListener;");
    return javaReservationListenerClass;
  }

  jmethodID reserveMemoryMethod(JNIEnv* env) {
    static jmethodID reserveMemoryMethod =
        getMethodIdOrError(env, javaReservationListenerClass(env), "reserve", "(J)J");
    return reserveMemoryMethod;
  }

  jmethodID unreserveMemoryMethod(JNIEnv* env) {
    static jmethodID unreserveMemoryMethod =
        getMethodIdOrError(env, javaReservationListenerClass(env), "unreserve", "(J)J");
    return unreserveMemoryMethod;
  }

  JavaVM* vm_;
  jobject jListenerGlobalRef_;
  std::atomic_int64_t usedBytes_{0L};
  std::atomic_int64_t peakBytes_{0L};
};

class BacktraceAllocationListener final : public gluten::AllocationListener {
 public:
  BacktraceAllocationListener(std::unique_ptr<gluten::AllocationListener> delegator)
      : delegator_(std::move(delegator)) {}

  void allocationChanged(int64_t bytes) override {
    allocationBacktrace(bytes);
    delegator_->allocationChanged(bytes);
  }

 private:
  void allocationBacktrace(int64_t bytes) {
    allocatedBytes_ += bytes;
    if (bytes > (64L << 20)) {
      backtrace();
    } else if (allocatedBytes_ >= backtraceBytes_) {
      backtrace();
      backtraceBytes_ += (1L << 30);
    }
  }

  std::unique_ptr<gluten::AllocationListener> delegator_;
  std::atomic_int64_t allocatedBytes_{};
  std::atomic_int64_t backtraceBytes_{1L << 30};
};

class JavaRssClient : public RssClient {
 public:
  JavaRssClient(JavaVM* vm, jobject javaRssShuffleWriter, jmethodID javaPushPartitionDataMethod)
      : vm_(vm), javaPushPartitionData_(javaPushPartitionDataMethod) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }

    javaRssShuffleWriter_ = env->NewGlobalRef(javaRssShuffleWriter);
    array_ = env->NewByteArray(1024 * 1024);
    array_ = static_cast<jbyteArray>(env->NewGlobalRef(array_));
  }

  ~JavaRssClient() {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      LOG(WARNING) << "JavaRssClient#~JavaRssClient(): "
                   << "JNIEnv was not attached to current thread";
      return;
    }
    env->DeleteGlobalRef(javaRssShuffleWriter_);
    // array_ may be nullptr when failed to allocate new byte array in pushPartitionData
    if (array_ != nullptr) {
      jbyte* byteArray = env->GetByteArrayElements(array_, NULL);
      env->ReleaseByteArrayElements(array_, byteArray, JNI_ABORT);
      env->DeleteGlobalRef(array_);
    }
  }

  int32_t pushPartitionData(int32_t partitionId, const char* bytes, int64_t size) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    jint length = env->GetArrayLength(array_);
    if (size > length) {
      jbyte* byteArray = env->GetByteArrayElements(array_, NULL);
      env->ReleaseByteArrayElements(array_, byteArray, JNI_ABORT);
      env->DeleteGlobalRef(array_);
      array_ = env->NewByteArray(size);
      if (array_ == nullptr) {
        LOG(WARNING) << "Failed to allocate new byte array size: " << size;
        throw gluten::GlutenException("Failed to allocate new byte array");
      }
      array_ = static_cast<jbyteArray>(env->NewGlobalRef(array_));
    }
    env->SetByteArrayRegion(array_, 0, size, (jbyte*)bytes);
    jint javaBytesSize = env->CallIntMethod(javaRssShuffleWriter_, javaPushPartitionData_, partitionId, array_, size);
    checkException(env);
    return static_cast<int32_t>(javaBytesSize);
  }

  void stop() override {}

 private:
  JavaVM* vm_;
  jobject javaRssShuffleWriter_;
  jmethodID javaPushPartitionData_;
  jbyteArray array_;
};

/// Bridges gluten::ThreadInitializer callbacks to a Java-side
/// NativeThreadInitializer instance via JNI.
///
/// On initialize(), attaches the current native thread to the JVM and calls
/// into the Java initializer so it can install Spark TaskContext or other
/// task-local state. On destroy(), calls the Java destroy() method but
/// does NOT detach the thread — the underlying JVM thread may be reused by
/// the pool, and detaching prematurely could allow the Java Thread object
/// to be garbage-collected.
class SparkThreadInitializer final : public gluten::ThreadInitializer {
 public:
  /// @param vm The JavaVM pointer from JNI_OnLoad.
  /// @param jInitializerLocalRef A local reference to a Java object
  ///        implementing org.apache.gluten.threads.NativeThreadInitializer.
  ///        A global reference is created internally.
  SparkThreadInitializer(JavaVM* vm, jobject jInitializerLocalRef) : vm_(vm) {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jInitializerGlobalRef_ = env->NewGlobalRef(jInitializerLocalRef);
    GLUTEN_CHECK(jInitializerGlobalRef_ != nullptr, "Failed to create global reference for native thread initializer.");
    (void)initializeMethod(env);
  }

  SparkThreadInitializer(const SparkThreadInitializer&) = delete;
  SparkThreadInitializer(SparkThreadInitializer&&) = delete;
  SparkThreadInitializer& operator=(const SparkThreadInitializer&) = delete;
  SparkThreadInitializer& operator=(SparkThreadInitializer&&) = delete;

  ~SparkThreadInitializer() override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      LOG(WARNING) << "SparkThreadInitializer#~SparkThreadInitializer(): "
                   << "JNIEnv was not attached to current thread";
      return;
    }
    env->DeleteGlobalRef(jInitializerGlobalRef_);
  }

  void initialize(const std::string& threadName) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jstring jThreadName = env->NewStringUTF(threadName.c_str());
    env->CallVoidMethod(jInitializerGlobalRef_, initializeMethod(env), jThreadName);
    env->DeleteLocalRef(jThreadName);
    checkException(env);
  }

  void destroy(const std::string& threadName) override {
    // IMPORTANT: Do not call vm_.DetachCurrentThread here, otherwise Java side thread
    // object might be dereferenced and garbage-collected, to break the reuse of thread
    // resources.
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jstring jThreadName = env->NewStringUTF(threadName.c_str());
    env->CallVoidMethod(jInitializerGlobalRef_, destroyMethod(env), jThreadName);
    env->DeleteLocalRef(jThreadName);
    checkException(env);
  }

 private:
  jmethodID initializeMethod(JNIEnv* env) {
    static jmethodID initializeMethod =
        getMethodIdOrError(env, nativeThreadInitializerClass(env), "initialize", "(Ljava/lang/String;)V");
    return initializeMethod;
  }

  jmethodID destroyMethod(JNIEnv* env) {
    static jmethodID destroyMethod =
        getMethodIdOrError(env, nativeThreadInitializerClass(env), "destroy", "(Ljava/lang/String;)V");
    return destroyMethod;
  }

  jclass nativeThreadInitializerClass(JNIEnv* env) {
    static jclass javaInitializerClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/threads/NativeThreadInitializer;");
    return javaInitializerClass;
  }

 private:
  JavaVM* vm_;
  jobject jInitializerGlobalRef_;
};

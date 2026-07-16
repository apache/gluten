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

#include "JniCommon.h"
#include <folly/system/ThreadName.h>

#include "utils/ArrowStatus.h"

namespace {

std::unordered_map<std::string, gluten::JniInputIteratorFactory>& jniInputIteratorFactories() {
  static std::unordered_map<std::string, gluten::JniInputIteratorFactory> factories;
  return factories;
}

std::mutex& jniInputIteratorFactoriesMutex() {
  static std::mutex mutex;
  return mutex;
}

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, arrow::MemoryPool* pool, jobject jniIn) : pool_(pool) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw gluten::GlutenException(errorMessage);
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
    env->CallVoidMethod(jniIn_, gluten::getJniCommonState()->jniByteInputStreamClose());
    checkException(env);
    env->DeleteGlobalRef(jniIn_);
    // Do NOT call DetachCurrentThread() here.
    // libhdfs.so caches JNIEnv* in thread-local storage after AttachCurrentThread.
    // If we detach, libhdfs's TLS cache becomes stale — the next HDFS call via
    // libhdfs returns the stale env, causing SIGSEGV in jni_NewStringUTF.
    // Daemon-attached threads are safe to leave attached; they won't block JVM shutdown.
    closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong told = env->CallLongMethod(jniIn_, gluten::getJniCommonState()->jniByteInputStreamTell());
    checkException(env);
    return told;
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong read = env->CallLongMethod(
        jniIn_, gluten::getJniCommonState()->jniByteInputStreamRead(), reinterpret_cast<jlong>(out), nbytes);
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

} // namespace

void gluten::JniCommonState::ensureInitialized(JNIEnv* env) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (initialized_) {
    return;
  }
  initialize(env);
  initialized_ = true;
}

void gluten::JniCommonState::assertInitialized() const {
  if (!initialized_) {
    throw gluten::GlutenException("Fatal: JniCommonState::Initialize(...) was not called before using the utility");
  }
}

jmethodID gluten::JniCommonState::runtimeAwareCtxHandle() {
  assertInitialized();
  return runtimeAwareCtxHandle_;
}

jmethodID gluten::JniCommonState::jniByteInputStreamRead() {
  assertInitialized();
  return jniByteInputStreamRead_;
}

jmethodID gluten::JniCommonState::jniByteInputStreamTell() {
  assertInitialized();
  return jniByteInputStreamTell_;
}

jmethodID gluten::JniCommonState::jniByteInputStreamClose() {
  assertInitialized();
  return jniByteInputStreamClose_;
}

jmethodID gluten::JniCommonState::shuffleStreamReaderNextStream() {
  assertInitialized();
  return shuffleStreamReaderNextStream_;
}

void gluten::JniCommonState::initialize(JNIEnv* env) {
  runtimeAwareClass_ = createGlobalClassReference(env, "Lorg/apache/gluten/runtime/RuntimeAware;");
  runtimeAwareCtxHandle_ = getMethodIdOrError(env, runtimeAwareClass_, "rtHandle", "()J");

  jniByteInputStreamClass_ =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/JniByteInputStream;");
  jniByteInputStreamRead_ = getMethodIdOrError(env, jniByteInputStreamClass_, "read", "(JJ)J");
  jniByteInputStreamTell_ = getMethodIdOrError(env, jniByteInputStreamClass_, "tell", "()J");
  jniByteInputStreamClose_ = getMethodIdOrError(env, jniByteInputStreamClass_, "close", "()V");

  shuffleStreamReaderClass_ =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ShuffleStreamReader;");
  shuffleStreamReaderNextStream_ = getMethodIdOrError(
      env, shuffleStreamReaderClass_, "nextStream", "()Lorg/apache/gluten/vectorized/JniByteInputStream;");

  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  vm_ = vm;
}

void gluten::JniCommonState::close() {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (closed_) {
    return;
  }
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(runtimeAwareClass_);
  env->DeleteGlobalRef(jniByteInputStreamClass_);
  env->DeleteGlobalRef(shuffleStreamReaderClass_);
  closed_ = true;
}

gluten::Runtime* gluten::getRuntime(JNIEnv* env, jobject runtimeAware) {
  int64_t ctxHandle = env->CallLongMethod(runtimeAware, getJniCommonState()->runtimeAwareCtxHandle());
  checkException(env);
  auto ctx = reinterpret_cast<Runtime*>(ctxHandle);
  GLUTEN_CHECK(ctx != nullptr, "FATAL: resource instance should not be null.");
  return ctx;
}

void gluten::registerJniInputIteratorFactory(const std::string& runtimeKind, JniInputIteratorFactory factory) {
  GLUTEN_CHECK(!runtimeKind.empty(), "JNI input iterator factory runtime kind must not be empty");
  GLUTEN_CHECK(static_cast<bool>(factory), "JNI input iterator factory must not be empty");

  std::lock_guard<std::mutex> lock(jniInputIteratorFactoriesMutex());
  const bool inserted = jniInputIteratorFactories().emplace(runtimeKind, std::move(factory)).second;
  GLUTEN_CHECK(inserted, "JNI input iterator factory already registered for " + runtimeKind);
}

std::unique_ptr<gluten::ColumnarBatchIterator>
gluten::createJniInputIterator(JNIEnv* env, jobject iterator, Runtime* runtime, int32_t iteratorIndex) {
  GLUTEN_CHECK(runtime != nullptr, "Runtime must not be null");

  JniInputIteratorFactory factory;
  {
    std::lock_guard<std::mutex> lock(jniInputIteratorFactoriesMutex());
    const auto it = jniInputIteratorFactories().find(runtime->kind());
    if (it != jniInputIteratorFactories().end()) {
      factory = it->second;
    }
  }

  if (factory) {
    return factory(env, iterator, runtime, iteratorIndex);
  }
  return std::make_unique<JniColumnarBatchIterator>(env, iterator, runtime, iteratorIndex);
}

gluten::ShuffleStreamReader::ShuffleStreamReader(JNIEnv* env, jobject reader) {
  if (env->GetJavaVM(&vm_) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }
  ref_ = env->NewGlobalRef(reader);
}

gluten::ShuffleStreamReader::~ShuffleStreamReader() {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(ref_);
}

std::shared_ptr<arrow::io::InputStream> gluten::ShuffleStreamReader::readNextStream(arrow::MemoryPool* pool) {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);

  jobject jniIn = env->CallObjectMethod(ref_, getJniCommonState()->shuffleStreamReaderNextStream());
  checkException(env);
  if (jniIn == nullptr) {
    return nullptr; // No more streams to read
  }
  return std::make_shared<JavaInputStreamAdaptor>(env, pool, jniIn);
}

std::unique_ptr<gluten::JniColumnarBatchIterator>
gluten::makeJniColumnarBatchIterator(JNIEnv* env, jobject jColumnarBatchItr, gluten::Runtime* runtime) {
  return std::make_unique<JniColumnarBatchIterator>(env, jColumnarBatchItr, runtime);
}

gluten::JniColumnarBatchIterator::JniColumnarBatchIterator(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    Runtime* runtime,
    std::optional<int32_t> iteratorIndex)
    : runtime_(runtime), iteratorIndex_(iteratorIndex), shouldDump_(runtime_->getDumper() != nullptr) {
  // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
  if (env->GetJavaVM(&vm_) != JNI_OK) {
    std::string errorMessage = "Unable to get JavaVM instance";
    throw gluten::GlutenException(errorMessage);
  }
  serializedColumnarBatchIteratorClass_ =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ColumnarBatchInIterator;");
  serializedColumnarBatchIteratorHasNext_ =
      getMethodIdOrError(env, serializedColumnarBatchIteratorClass_, "hasNext", "()Z");
  serializedColumnarBatchIteratorNext_ = getMethodIdOrError(env, serializedColumnarBatchIteratorClass_, "next", "()J");
  jColumnarBatchItr_ = env->NewGlobalRef(jColumnarBatchItr);
}

gluten::JniColumnarBatchIterator::~JniColumnarBatchIterator() {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(jColumnarBatchItr_);
  env->DeleteGlobalRef(serializedColumnarBatchIteratorClass_);
  // Do NOT call DetachCurrentThread() here.
  // libhdfs.so caches JNIEnv* in thread-local storage after AttachCurrentThread.
  // If we detach, libhdfs's TLS cache becomes stale — the next HDFS call via
  // libhdfs returns the stale env, causing SIGSEGV in jni_NewStringUTF.
  // Daemon-attached threads are safe to leave attached; they won't block JVM shutdown.
}

std::shared_ptr<gluten::ColumnarBatch> gluten::JniColumnarBatchIterator::next() {
  if (shouldDump_ && dumpedIteratorReader_ == nullptr) {
    GLUTEN_CHECK(iteratorIndex_.has_value(), "iteratorIndex_ should not be null");

    const auto iter = std::make_shared<ColumnarBatchIteratorDumper>(this);
    dumpedIteratorReader_ = runtime_->getDumper()->dumpInputIterator(iteratorIndex_.value(), iter);
  }

  if (dumpedIteratorReader_ != nullptr) {
    return dumpedIteratorReader_->next();
  }

  return nextInternal();
}

std::shared_ptr<gluten::ColumnarBatch> gluten::JniColumnarBatchIterator::nextInternal() const {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  if (!env->CallBooleanMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorHasNext_)) {
    checkException(env);
    return nullptr; // stream ended
  }
  checkException(env);
  jlong handle = env->CallLongMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorNext_);
  checkException(env);
  return ObjectStore::retrieve<ColumnarBatch>(handle);
}

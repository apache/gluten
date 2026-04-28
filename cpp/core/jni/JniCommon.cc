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

void gluten::JniCommonState::initialize(JNIEnv* env) {
  runtimeAwareClass_ = createGlobalClassReference(env, "Lorg/apache/gluten/runtime/RuntimeAware;");
  runtimeAwareCtxHandle_ = getMethodIdOrError(env, runtimeAwareClass_, "rtHandle", "()J");
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
  closed_ = true;
}

gluten::Runtime* gluten::getRuntime(JNIEnv* env, jobject runtimeAware) {
  int64_t ctxHandle = env->CallLongMethod(runtimeAware, getJniCommonState()->runtimeAwareCtxHandle());
  checkException(env);
  auto ctx = reinterpret_cast<Runtime*>(ctxHandle);
  GLUTEN_CHECK(ctx != nullptr, "FATAL: resource instance should not be null.");
  return ctx;
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

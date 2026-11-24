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
#include "JniError.h"

#include <atomic>

namespace {

std::atomic<gluten::ErrorCodeGetter> errorCodeGetter{nullptr};

std::string errorCodeOf(const std::exception& error) noexcept {
  const auto getter = errorCodeGetter.load(std::memory_order_acquire);
  if (getter == nullptr) {
    return {};
  }
  try {
    return getter(error);
  } catch (...) {
    LOG(WARNING) << "Unable to get the error code of a native exception";
    return {};
  }
}

// Throws GlutenException(message, errorCode). Returns false if the exception could not be thrown.
bool throwGlutenExceptionWithErrorCode(JNIEnv* env, const std::exception& error, const std::string& errorCode) {
  auto* state = gluten::getJniErrorState();
  jstring message = env->NewStringUTF(error.what());
  jstring code = message == nullptr ? nullptr : env->NewStringUTF(errorCode.c_str());
  jobject exception = code == nullptr
      ? nullptr
      : env->NewObject(state->glutenExceptionClass(), state->glutenExceptionWithErrorCodeCtor(), message, code);
  const bool thrown = exception != nullptr && env->Throw(static_cast<jthrowable>(exception)) == JNI_OK;
  if (!thrown) {
    env->ExceptionClear();
  }
  env->DeleteLocalRef(exception);
  env->DeleteLocalRef(code);
  env->DeleteLocalRef(message);
  return thrown;
}

} // namespace

void gluten::setErrorCodeGetter(ErrorCodeGetter getter) {
  errorCodeGetter.store(getter, std::memory_order_release);
}

void gluten::throwGlutenException(JNIEnv* env, const std::exception& error) {
  const auto errorCode = errorCodeOf(error);
  if (!errorCode.empty()) {
    if (throwGlutenExceptionWithErrorCode(env, error, errorCode)) {
      return;
    }
    LOG(WARNING) << "Unable to throw GlutenException with error code " << errorCode
                 << " at JNI boundary, throwing it without the error code";
  }
  if (env->ThrowNew(getJniErrorState()->glutenExceptionClass(), error.what()) != JNI_OK) {
    LOG(ERROR) << "Unable to throw GlutenException at JNI boundary";
  }
}

void gluten::JniErrorState::ensureInitialized(JNIEnv* env) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (initialized_) {
    return;
  }
  initialize(env);
  initialized_ = true;
}

void gluten::JniErrorState::assertInitialized() {
  if (!initialized_) {
    throw gluten::GlutenException("Fatal: JniErrorState::Initialize(...) was not called before using the utility");
  }
}

jclass gluten::JniErrorState::runtimeExceptionClass() {
  assertInitialized();
  return runtimeExceptionClass_;
}

jclass gluten::JniErrorState::illegalAccessExceptionClass() {
  assertInitialized();
  return illegalAccessExceptionClass_;
}

jclass gluten::JniErrorState::glutenExceptionClass() {
  assertInitialized();
  return glutenExceptionClass_;
}

jmethodID gluten::JniErrorState::glutenExceptionWithErrorCodeCtor() {
  assertInitialized();
  return glutenExceptionWithErrorCodeCtor_;
}

void gluten::JniErrorState::initialize(JNIEnv* env) {
  glutenExceptionClass_ = createGlobalClassReference(env, "Lorg/apache/gluten/exception/GlutenException;");
  glutenExceptionWithErrorCodeCtor_ =
      getMethodIdOrError(env, glutenExceptionClass_, "<init>", "(Ljava/lang/String;Ljava/lang/String;)V");
  ioExceptionClass_ = createGlobalClassReference(env, "Ljava/io/IOException;");
  runtimeExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/RuntimeException;");
  unsupportedOperationExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
  illegalAccessExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegalArgumentExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  vm_ = vm;
}

void gluten::JniErrorState::close() {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (closed_) {
    return;
  }
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(glutenExceptionClass_);
  env->DeleteGlobalRef(ioExceptionClass_);
  env->DeleteGlobalRef(runtimeExceptionClass_);
  env->DeleteGlobalRef(unsupportedOperationExceptionClass_);
  env->DeleteGlobalRef(illegalAccessExceptionClass_);
  env->DeleteGlobalRef(illegalArgumentExceptionClass_);
  closed_ = true;
}

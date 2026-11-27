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

#include <algorithm>
#include <limits>
#include <vector>

namespace gluten {
namespace {
template <typename Callback>
struct CallbackRegistry {
  explicit CallbackRegistry(const char* kind) : kind(kind) {}

  void add(Callback callback) {
    GLUTEN_CHECK(callback != nullptr, std::string(kind) + " must not be null");
    std::lock_guard<std::mutex> lock(mutex);
    GLUTEN_CHECK(
        std::find(callbacks.begin(), callbacks.end(), callback) == callbacks.end(),
        std::string(kind) + " already registered");
    callbacks.push_back(callback);
  }

  void remove(Callback callback) {
    std::lock_guard<std::mutex> lock(mutex);
    const auto it = std::find(callbacks.begin(), callbacks.end(), callback);
    if (it == callbacks.end()) {
      LOG(ERROR) << "Cannot unregister " << kind << ": no matching registration";
      return;
    }
    callbacks.erase(it);
  }

  const char* const kind;
  std::mutex mutex;
  std::vector<Callback> callbacks;
};

CallbackRegistry<ExceptionUnwrapper>& unwrapperRegistry() {
  static CallbackRegistry<ExceptionUnwrapper> registry("JNI exception unwrapper");
  return registry;
}

CallbackRegistry<ExceptionMetadataProvider>& metadataRegistry() {
  static CallbackRegistry<ExceptionMetadataProvider> registry("JNI exception metadata provider");
  return registry;
}

std::exception_ptr unwrapBackendException(const std::exception& error) noexcept {
  auto& registry = unwrapperRegistry();
  // Unregistering a backend must wait until its callback has returned.
  std::lock_guard<std::mutex> lock(registry.mutex);
  for (auto unwrap : registry.callbacks) {
    if (auto cause = unwrap(error)) {
      return cause;
    }
  }
  return nullptr;
}

std::optional<JniExceptionMetadata> describeBackendException(const std::exception& error) {
  auto& registry = metadataRegistry();
  // Unregistering a backend must wait until its callback has returned.
  std::lock_guard<std::mutex> lock(registry.mutex);
  for (auto describe : registry.callbacks) {
    if (auto metadata = describe(error)) {
      return metadata;
    }
  }
  return std::nullopt;
}

jbyteArray toJavaBytes(JNIEnv* env, const std::string& value) {
  GLUTEN_CHECK(
      value.size() <= static_cast<size_t>(std::numeric_limits<jsize>::max()),
      "Native exception metadata exceeds the JNI array limit");
  const auto size = static_cast<jsize>(value.size());
  const auto bytes = env->NewByteArray(size);
  if (bytes == nullptr || env->ExceptionCheck()) {
    return nullptr;
  }
  if (size != 0) {
    env->SetByteArrayRegion(bytes, 0, size, reinterpret_cast<const jbyte*>(value.data()));
  }
  return env->ExceptionCheck() ? nullptr : bytes;
}

// On success the new throwable is pending. A Java exception raised while creating it, such as
// OutOfMemoryError, is left pending instead.
void throwMetadataException(JNIEnv* env, const JniExceptionMetadata& metadata) {
  // Two classes, two byte arrays and the throwable. The JNI method returns right after this handler.
  constexpr jint kMetadataLocalCapacity = 5;
  if (env->EnsureLocalCapacity(kMetadataLocalCapacity) != JNI_OK) {
    return;
  }
  const auto type = env->FindClass(metadata.className.c_str());
  if (type == nullptr || env->ExceptionCheck()) {
    return;
  }
  const auto throwableType = env->FindClass("java/lang/Throwable");
  if (throwableType == nullptr || env->ExceptionCheck()) {
    return;
  }
  // JNI Throw requires a Throwable, so never run another class's constructor.
  if (!env->IsAssignableFrom(type, throwableType)) {
    LOG(ERROR) << "Unable to throw " << metadata.className << " at JNI boundary: not a java.lang.Throwable";
    return;
  }
  const auto constructor = env->GetMethodID(type, "<init>", "([B[B)V");
  if (constructor == nullptr || env->ExceptionCheck()) {
    return;
  }
  const auto reason = toJavaBytes(env, metadata.reason);
  if (reason == nullptr) {
    return;
  }
  const auto diagnostic = toJavaBytes(env, metadata.diagnostic);
  if (diagnostic == nullptr) {
    return;
  }
  const auto throwable = static_cast<jthrowable>(env->NewObject(type, constructor, reason, diagnostic));
  if (throwable != nullptr && !env->ExceptionCheck() && env->Throw(throwable) != JNI_OK) {
    LOG(ERROR) << "Unable to throw " << metadata.className << " at JNI boundary";
  }
}
} // namespace

JniExceptionUnwrapperRegistration::JniExceptionUnwrapperRegistration(ExceptionUnwrapper unwrap) : unwrap_(unwrap) {
  unwrapperRegistry().add(unwrap);
}

JniExceptionUnwrapperRegistration::~JniExceptionUnwrapperRegistration() {
  unwrapperRegistry().remove(unwrap_);
}

JniExceptionMetadataRegistration::JniExceptionMetadataRegistration(ExceptionMetadataProvider describe)
    : describe_(describe) {
  metadataRegistry().add(describe);
}

JniExceptionMetadataRegistration::~JniExceptionMetadataRegistration() {
  metadataRegistry().remove(describe_);
}

void handleJniException(JNIEnv* env, const std::exception& error) {
  if (env->ExceptionCheck()) {
    return;
  }
  const auto cause = findJavaExceptionCause(error, unwrapBackendException);
  if (cause) {
    cause->exception.throwToJava(env);
  } else {
    try {
      if (const auto metadata = describeBackendException(error)) {
        throwMetadataException(env, *metadata);
      }
    } catch (const std::exception& failure) {
      // Describing is best effort; keep the original diagnostic or a newer pending throwable.
      LOG(WARNING) << "Unable to describe native exception at JNI boundary: " << failure.what();
    } catch (...) {
      LOG(WARNING) << "Unable to describe native exception at JNI boundary: unknown exception type";
    }
  }
  if (!env->ExceptionCheck()) {
    const char* message = cause && cause->viaBackend ? cause->exception.what() : error.what();
    if (env->ThrowNew(getJniErrorState()->glutenExceptionClass(), message) != JNI_OK) {
      LOG(ERROR) << "Unable to throw GlutenException at JNI boundary";
    }
  }
}
} // namespace gluten

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

void gluten::JniErrorState::initialize(JNIEnv* env) {
  glutenExceptionClass_ = createGlobalClassReference(env, "org/apache/gluten/exception/GlutenException");
  ioExceptionClass_ = createGlobalClassReference(env, "java/io/IOException");
  runtimeExceptionClass_ = createGlobalClassReference(env, "java/lang/RuntimeException");
  unsupportedOperationExceptionClass_ = createGlobalClassReference(env, "java/lang/UnsupportedOperationException");
  illegalAccessExceptionClass_ = createGlobalClassReference(env, "java/lang/IllegalAccessException");
  illegalArgumentExceptionClass_ = createGlobalClassReference(env, "java/lang/IllegalArgumentException");
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

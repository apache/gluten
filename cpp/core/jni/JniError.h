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

#include <optional>
#include <stdexcept>
#include <string>

#include "JniCommon.h"
#include "utils/Exception.h"

#ifndef JNI_METHOD_START
#define JNI_METHOD_START                     \
  const gluten::JniEntryGuard jniEntryGuard; \
  try {
// macro ended
#endif

#ifndef JNI_METHOD_END
#define JNI_METHOD_END(fallback_expr)   \
  }                                     \
  catch (const std::exception& e) {     \
    gluten::handleJniException(env, e); \
    return fallback_expr;               \
  }
#endif

namespace gluten {

// Register for the lifetime of the backend library. Called under the registry lock, the
// callback must only inspect its own wrapper and return its immediate cause (otherwise nullptr).
class JniExceptionUnwrapperRegistration final {
 public:
  explicit JniExceptionUnwrapperRegistration(ExceptionUnwrapper unwrap);
  ~JniExceptionUnwrapperRegistration();
  JniExceptionUnwrapperRegistration(const JniExceptionUnwrapperRegistration&) = delete;
  JniExceptionUnwrapperRegistration& operator=(const JniExceptionUnwrapperRegistration&) = delete;

 private:
  ExceptionUnwrapper unwrap_;
};

// A Java throwable to raise for a native error. className uses JNI's slash-separated form and is resolved by
// FindClass with the class loader of the class declaring the native method. The class must extend
// java.lang.Throwable and have a (byte[] reason, byte[] diagnostic) constructor. The reason and diagnostic
// should be UTF-8; their bytes, including NULs, are passed unchanged. The strings are owned, so the JNI
// boundary uses no backend memory.
struct JniExceptionMetadata {
  std::string className;
  std::string reason;
  std::string diagnostic;
};

using ExceptionMetadataProvider = std::optional<JniExceptionMetadata> (*)(const std::exception&);

// Register for the lifetime of the backend library. Called under the registry lock only when no Java
// throwable is pending or carried, the callback must not call JNI and returns metadata for its own
// errors (otherwise std::nullopt). Raising the throwable is best effort: a Java exception raised while
// constructing it stays pending. Any other failure keeps the GlutenException fallback, including an
// exception of any type from the callback or a class that isn't a Throwable, which is never constructed.
class JniExceptionMetadataRegistration final {
 public:
  explicit JniExceptionMetadataRegistration(ExceptionMetadataProvider describe);
  ~JniExceptionMetadataRegistration();
  JniExceptionMetadataRegistration(const JniExceptionMetadataRegistration&) = delete;
  JniExceptionMetadataRegistration& operator=(const JniExceptionMetadataRegistration&) = delete;

 private:
  ExceptionMetadataProvider describe_;
};

void handleJniException(JNIEnv* env, const std::exception& error);

class JniErrorState {
 public:
  virtual ~JniErrorState() = default;

  void ensureInitialized(JNIEnv* env);

  void assertInitialized();

  void close();

  jclass runtimeExceptionClass();

  jclass illegalAccessExceptionClass();

  jclass glutenExceptionClass();

 private:
  void initialize(JNIEnv* env);

  jclass ioExceptionClass_ = nullptr;
  jclass runtimeExceptionClass_ = nullptr;
  jclass unsupportedOperationExceptionClass_ = nullptr;
  jclass illegalAccessExceptionClass_ = nullptr;
  jclass illegalArgumentExceptionClass_ = nullptr;
  jclass glutenExceptionClass_ = nullptr;
  JavaVM* vm_;
  bool initialized_{false};
  bool closed_{false};
  std::mutex mtx_;
};

inline JniErrorState* getJniErrorState() {
  static JniErrorState jniErrorState;
  return &jniErrorState;
}

} // namespace gluten

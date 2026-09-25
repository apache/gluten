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

#include "JniCastException.h"

#include <folly/ScopeGuard.h>
#include <limits>
#include "jni/JavaException.h"
#include "jni/JniCommon.h"

namespace gluten {
namespace {

jclass nativeCastExceptionClass = nullptr;
jmethodID nativeCastExceptionConstructor = nullptr;

jbyteArray toByteArray(JNIEnv* env, const std::string& value) {
  GLUTEN_CHECK(
      value.size() <= static_cast<size_t>(std::numeric_limits<jsize>::max()),
      "Native cast error exceeds the JNI array limit.");
  const auto size = static_cast<jsize>(value.size());
  auto bytes = env->NewByteArray(size);
  checkException(env);
  env->SetByteArrayRegion(bytes, 0, size, reinterpret_cast<const jbyte*>(value.data()));
  checkException(env);
  return bytes;
}

} // namespace

void initVeloxJniCastException(JNIEnv* env) {
  nativeCastExceptionClass = createGlobalClassReferenceOrError(env, "org/apache/gluten/exception/NativeCastException");
  nativeCastExceptionConstructor = getMethodIdOrError(env, nativeCastExceptionClass, "<init>", "([B[B)V");
}

void finalizeVeloxJniCastException(JNIEnv* env) {
  env->DeleteGlobalRef(nativeCastExceptionClass);
  nativeCastExceptionClass = nullptr;
  nativeCastExceptionConstructor = nullptr;
}

bool isNativeCastException(const facebook::velox::VeloxException& error) {
  using namespace facebook::velox;
  if (error.errorSource() != error_source::kErrorSourceUser || error.errorCode() != error_code::kInvalidArgument) {
    return false;
  }
  const auto* properties = dynamic_cast<const ExpressionExceptionProperties*>(error.properties().get());
  return properties != nullptr && properties->owner.empty() && properties->functionName == "cast";
}

void rethrowNativeCastException(const facebook::velox::VeloxException& error) {
  if (!isNativeCastException(error) || nativeCastExceptionClass == nullptr) {
    // Native-only callers have no JVM and must retain the original Velox exception.
    return;
  }
  JNIEnv* env;
  attachCurrentThreadAsDaemonOrThrow(getJniCommonState()->getJavaVM(), &env);
  if (env->PushLocalFrame(3) != JNI_OK) {
    checkException(env);
    throw GlutenException("Unable to allocate JNI local references for a native cast error.");
  }
  auto frame = folly::makeGuard([env]() { env->PopLocalFrame(nullptr); });

  // Byte arrays preserve UTF-8, including embedded NULs; NewStringUTF expects modified UTF-8.
  auto reason = toByteArray(env, error.message());
  auto diagnostic = toByteArray(env, error.what());
  auto throwable = static_cast<jthrowable>(
      env->NewObject(nativeCastExceptionClass, nativeCastExceptionConstructor, reason, diagnostic));
  checkException(env);
  throw JavaException(env, throwable);
}

} // namespace gluten

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

#include <exception>
#include <stdexcept>

#include "jni/JniCommon.h"
#include "jni/JniError.h"

namespace {
class ScopedThrowFailure : public JNINativeInterface_ {
 public:
  ScopedThrowFailure(JNIEnv* env, jthrowable failure) noexcept
      : JNINativeInterface_(*env->functions), env_(env), original_(env->functions), failure_(failure) {
    Throw = [](JNIEnv* env, jthrowable) {
      const auto* functions = static_cast<const ScopedThrowFailure*>(env->functions);
      if (functions->failure_ != nullptr && functions->original_->Throw(env, functions->failure_) != JNI_OK) {
        LOG(ERROR) << "Unable to install injected Java restoration failure";
      }
      return JNI_ERR;
    };
    env_->functions = this;
  }

  ~ScopedThrowFailure() {
    env_->functions = original_;
  }

  ScopedThrowFailure(const ScopedThrowFailure&) = delete;
  ScopedThrowFailure& operator=(const ScopedThrowFailure&) = delete;

 private:
  JNIEnv* env_;
  const JNINativeInterface_* original_;
  jthrowable failure_;
};

jint roundTrip(JNIEnv* env, jobject callback, jboolean nested, jthrowable pending) {
  JNI_METHOD_START
  gluten::getJniErrorState()->ensureInitialized(env);
  checkException(env);
  auto callbackClass = env->GetObjectClass(callback);
  checkException(env);
  auto run = env->GetMethodID(callbackClass, "run", "()V");
  env->DeleteLocalRef(callbackClass);
  checkException(env);
  try {
    env->CallVoidMethod(callback, run);
    checkException(env);
  } catch (...) {
    if (pending != nullptr && env->Throw(pending) != JNI_OK) {
      throw std::runtime_error("Unable to install newer pending Java exception");
    }
    if (nested) {
      std::throw_with_nested(std::runtime_error("native wrapper"));
    }
    throw;
  }
  return 42;
  JNI_METHOD_END(-1)
}
} // namespace

extern "C" JNIEXPORT jint JNICALL
Java_org_apache_gluten_jni_JavaExceptionJniTest_roundTrip(JNIEnv* env, jclass, jobject callback, jboolean nested) {
  return roundTrip(env, callback, nested, nullptr);
}

extern "C" JNIEXPORT jint JNICALL Java_org_apache_gluten_jni_JavaExceptionJniTest_roundTripWithPendingException(
    JNIEnv* env,
    jclass,
    jobject callback,
    jboolean nested,
    jthrowable pending) {
  return roundTrip(env, callback, nested, pending);
}

extern "C" JNIEXPORT jint JNICALL Java_org_apache_gluten_jni_JavaExceptionJniTest_roundTripWithFailedRestore(
    JNIEnv* env,
    jclass clazz,
    jobject callback,
    jboolean nested,
    jthrowable failure) {
  // Keep fault injection active through the production boundary's catch handlers.
  const ScopedThrowFailure functions(env, failure);
  return Java_org_apache_gluten_jni_JavaExceptionJniTest_roundTrip(env, clazz, callback, nested);
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_gluten_jni_JavaExceptionJniTest_throwNativeException(JNIEnv* env, jclass) {
  JNI_METHOD_START
  gluten::getJniErrorState()->ensureInitialized(env);
  checkException(env);
  throw std::runtime_error("native-only failure");
  JNI_METHOD_END()
}

extern "C" JNIEXPORT void JNICALL Java_org_apache_gluten_jni_JavaExceptionJniTest_close(JNIEnv* env, jclass) {
  JNI_METHOD_START
  gluten::getJniErrorState()->close();
  JNI_METHOD_END()
}

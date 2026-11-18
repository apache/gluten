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
#include <thread>

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

class ScopedCaptureFailure : public JNINativeInterface_ {
 public:
  ScopedCaptureFailure(JNIEnv* env, jthrowable failure) noexcept
      : JNINativeInterface_(*env->functions), env_(env), original_(env->functions), failure_(failure) {
    NewGlobalRef = [](JNIEnv* env, jobject) -> jobject {
      const auto* functions = static_cast<const ScopedCaptureFailure*>(env->functions);
      if (functions->original_->Throw(env, functions->failure_) != JNI_OK) {
        throw std::runtime_error("Unable to install injected Java capture failure");
      }
      return nullptr;
    };
    env_->functions = this;
  }

  ~ScopedCaptureFailure() {
    env_->functions = original_;
  }

  ScopedCaptureFailure(const ScopedCaptureFailure&) = delete;
  ScopedCaptureFailure& operator=(const ScopedCaptureFailure&) = delete;

 private:
  JNIEnv* env_;
  const JNINativeInterface_* original_;
  jthrowable failure_;
};

void invokeCallback(JNIEnv* env, jobject callback) {
  auto callbackClass = env->GetObjectClass(callback);
  checkException(env);
  auto run = env->GetMethodID(callbackClass, "run", "()V");
  env->DeleteLocalRef(callbackClass);
  checkException(env);
  env->CallVoidMethod(callback, run);
  checkException(env);
}

jint roundTrip(JNIEnv* env, jobject callback, jboolean nested, jthrowable pending) {
  JNI_METHOD_START
  gluten::getJniErrorState()->ensureInitialized(env);
  checkException(env);
  try {
    invokeCallback(env, callback);
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

extern "C" JNIEXPORT void JNICALL Java_org_apache_gluten_jni_JavaExceptionJniTest_roundTripWithFailedCapture(
    JNIEnv* env,
    jclass,
    jobject callback,
    jthrowable failure) {
  JNI_METHOD_START
  gluten::getJniErrorState()->ensureInitialized(env);
  checkException(env);
  const ScopedCaptureFailure functions(env, failure);
  invokeCallback(env, callback);
  JNI_METHOD_END()
}

extern "C" JNIEXPORT void JNICALL Java_org_apache_gluten_jni_JavaExceptionJniTest_roundTripOnWorker(
    JNIEnv* env,
    jclass,
    jobject callback,
    jobject nextCallback,
    jthrowable captureFailure) {
  JNI_METHOD_START
  gluten::getJniErrorState()->ensureInitialized(env);
  checkException(env);
  JavaVM* vm = nullptr;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw std::runtime_error("Unable to get test JavaVM");
  }
  const auto release = [env](jobject ref) { env->DeleteGlobalRef(ref); };
  const auto retain = [&](jobject object) {
    std::unique_ptr<_jobject, decltype(release)> ref(env->NewGlobalRef(object), release);
    checkException(env);
    return ref;
  };
  auto globalCallback = retain(callback);
  auto globalNextCallback = retain(nextCallback);
  auto globalFailure = retain(captureFailure);
  std::exception_ptr error;
  std::thread worker([&] {
    JNIEnv* workerEnv = nullptr;
    if (vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&workerEnv), nullptr) != JNI_OK) {
      error = std::make_exception_ptr(std::runtime_error("Unable to attach test worker"));
      return;
    }
    try {
      if (gluten::JniEntryGuard::isActive()) {
        throw std::runtime_error("Worker inherited another thread's JNI entry");
      }
      try {
        if (globalFailure) {
          const ScopedCaptureFailure functions(workerEnv, static_cast<jthrowable>(globalFailure.get()));
          invokeCallback(workerEnv, globalCallback.get());
        } else {
          invokeCallback(workerEnv, globalCallback.get());
        }
      } catch (const gluten::JavaException& captured) {
        auto copy = captured;
        error = std::make_exception_ptr(copy);
      } catch (...) {
        error = std::current_exception();
      }
      if (workerEnv->ExceptionCheck()) {
        workerEnv->ExceptionClear();
        throw std::runtime_error("Capture failure left a pending exception on a reused worker");
      }
      JNIEnv* reusedEnv = nullptr;
      if (vm->GetEnv(reinterpret_cast<void**>(&reusedEnv), JNI_VERSION_1_8) != JNI_OK || reusedEnv != workerEnv) {
        throw std::runtime_error("Worker was detached between callbacks");
      }
      invokeCallback(workerEnv, globalNextCallback.get());
      if (gluten::JniEntryGuard::isActive()) {
        throw std::runtime_error("Worker retained a completed JNI entry");
      }
    } catch (...) {
      error = std::current_exception();
    }
    // This test owns the thread; detach only after both callbacks, not between jobs.
    workerEnv->ExceptionClear();
    if (vm->DetachCurrentThread() != JNI_OK) {
      error = std::make_exception_ptr(std::runtime_error("Unable to detach finished test worker"));
    }
  });
  worker.join();
  if (error) {
    std::rethrow_exception(error);
  }
  JNI_METHOD_END()
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

extern "C" JNIEXPORT jboolean JNICALL Java_org_apache_gluten_jni_JavaExceptionJniTest_entryActive(JNIEnv*, jclass) {
  // Inspect the caller's scope without adding another guarded entry.
  return gluten::JniEntryGuard::isActive();
}

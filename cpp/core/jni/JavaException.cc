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

#include "JavaException.h"

#include <glog/logging.h>

#include <stdexcept>
#include <string>

namespace gluten {
namespace {

class LocalFrame {
 public:
  explicit LocalFrame(JNIEnv* env) : env_(env), pushed_(env->PushLocalFrame(3) == JNI_OK) {}

  ~LocalFrame() {
    if (pushed_) {
      env_->PopLocalFrame(nullptr);
    }
  }

  bool pushed() const {
    return pushed_;
  }

 private:
  JNIEnv* env_;
  bool pushed_;
};

std::string describe(JNIEnv* env, jthrowable throwable) {
  const std::string prefix = "Error during calling Java code from native code: ";
  const auto unavailable = [&](const char* reason) {
    // Describing an exception must not replace the throwable we are transporting.
    env->ExceptionClear();
    return prefix + "Java stack trace unavailable (" + reason + ")";
  };
  LocalFrame frame(env);
  if (!frame.pushed()) {
    return unavailable("PushLocalFrame failed");
  }
  auto describer = env->FindClass("org/apache/gluten/exception/JniExceptionDescriber");
  if (describer == nullptr) {
    return unavailable("JniExceptionDescriber lookup failed");
  }
  auto method = env->GetStaticMethodID(describer, "describe", "(Ljava/lang/Throwable;)Ljava/lang/String;");
  if (method == nullptr) {
    return unavailable("describe method lookup failed");
  }
  auto description = static_cast<jstring>(env->CallStaticObjectMethod(describer, method, throwable));
  if (env->ExceptionCheck() || description == nullptr) {
    return unavailable("describe method failed");
  }
  const auto release = [&](const char* chars) { env->ReleaseStringUTFChars(description, chars); };
  std::unique_ptr<const char, decltype(release)> chars(env->GetStringUTFChars(description, nullptr), release);
  if (!chars) {
    return unavailable("GetStringUTFChars failed");
  }
  return prefix + chars.get();
}

} // namespace

struct JavaException::State {
  State(JNIEnv* env, jthrowable throwable) {
    if (throwable == nullptr) {
      throw std::invalid_argument("JavaException requires a non-null throwable");
    }
    if (env->GetJavaVM(&vm) != JNI_OK) {
      throw std::runtime_error("Unable to get JavaVM for Java exception");
    }
    ref = static_cast<jthrowable>(env->NewGlobalRef(throwable));
    if (ref == nullptr) {
      throw std::runtime_error("Unable to create global reference for Java exception");
    }
  }

  ~State() {
    JNIEnv* env = nullptr;
    auto status = vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    const bool detached = status == JNI_EDETACHED;
    if (detached) {
      status = vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env), nullptr);
    }
    if (status != JNI_OK) {
      LOG(ERROR) << "Unable to attach thread to release Java exception global reference: " << status;
      return;
    }
    env->DeleteGlobalRef(ref);
    // Only detach if this destructor attached the thread; never detach a caller's JNI/HDFS thread.
    if (detached && vm->DetachCurrentThread() != JNI_OK) {
      LOG(ERROR) << "Unable to detach thread after releasing Java exception global reference";
    }
  }

  JavaVM* vm{nullptr};
  jthrowable ref{nullptr};
  std::string message;
};

JavaException::JavaException(JNIEnv* env, jthrowable throwable) : state_(std::make_shared<State>(env, throwable)) {
  state_->message = describe(env, throwable);
}

const char* JavaException::what() const noexcept {
  return state_->message.c_str();
}

void JavaException::throwToJava(JNIEnv* env) const noexcept {
  if (env->Throw(state_->ref) != JNI_OK) {
    LOG(ERROR) << "Unable to restore original Java exception";
  }
}

void JavaException::check(JNIEnv* env) {
  if (!env->ExceptionCheck()) {
    return;
  }
  auto throwable = env->ExceptionOccurred();
  env->ExceptionClear();
  const auto release = [env](jthrowable ref) { env->DeleteLocalRef(ref); };
  std::unique_ptr<_jthrowable, decltype(release)> local(throwable, release);
  throw JavaException(env, throwable);
}

} // namespace gluten

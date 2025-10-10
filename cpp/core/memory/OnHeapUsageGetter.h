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

#include <glog/logging.h>
#include <jni.h>
#include <bolt/common/memory/MemoryUtils.h>
#include <cstdint>
#include <mutex>

namespace gluten {
class OnHeapMemUsedHookSetter final {
 public:
  static void init(JavaVM* vm) {
    std::lock_guard<std::mutex> guard(lock_);
    if (!hasInit_) {
      vm_ = vm;
      initJniCache();
      hasInit_ = true;
    } else {
      LOG(ERROR) << "Can only init vm once!";
    }
  }

  static int64_t getOnHeapUsedMemory() {
    if (!hasInit_) {
      return kInvalidMemUsed;
    }

    JNIEnv* env = getJniEnv();
    if (!env) {
      return kInvalidMemUsed;
    }

    jlong totalMem = env->CallLongMethod(runtimeInstance_, totalMemoryMethod_);
    return static_cast<int64_t>(totalMem);
  }

  inline static constexpr int64_t kInvalidMemUsed = bytedance::bolt::memory::MemoryUtils::kInvalidRssSize;

 private:
  static JNIEnv* getJniEnv() {
    if (!vm_) {
      LOG(ERROR) << "JVM hasn't been inited!";
      return nullptr;
    }
    JNIEnv* env = nullptr;
    jint result = vm_->GetEnv((void**)&env, JNI_VERSION_1_8);
    if (result == JNI_EDETACHED) {
      if (vm_->AttachCurrentThread((void**)&env, nullptr) != JNI_OK) {
        LOG(ERROR) << "Can't attach current thread to JVM.";
        return nullptr;
      }
    } else if (result != JNI_OK) {
      LOG(ERROR) << "Get JNI env failed! failed reason is: " << result;
      return nullptr;
    }
    return env;
  }

  static bool initJniCache() {
    if (hasInit_) {
      return true;
    }

    JNIEnv* env = getJniEnv();
    if (!env) {
      return false;
    }

    // cache Runtime class
    jclass runtimeClass = env->FindClass("java/lang/Runtime");
    if (!runtimeClass) {
      LOG(ERROR) << "Can't find Runtime class!";
      return false;
    }
    runtimeClass_ = static_cast<jclass>(env->NewGlobalRef(runtimeClass));
    env->DeleteLocalRef(runtimeClass);

    // cache getRuntime method ID
    getRuntimeMethod_ = env->GetStaticMethodID(runtimeClass_, "getRuntime", "()Ljava/lang/Runtime;");
    if (!getRuntimeMethod_) {
      LOG(ERROR) << "Can't find getRuntime() in Runtime.";
      return false;
    }

    // cache instance of Runtime
    jobject runtimeInstance = env->CallStaticObjectMethod(runtimeClass_, getRuntimeMethod_);
    if (!runtimeInstance) {
      LOG(ERROR) << "Can't get Runtime instance.";
      return false;
    }
    runtimeInstance_ = env->NewGlobalRef(runtimeInstance);
    env->DeleteLocalRef(runtimeInstance);

    // cache totalMemory method ID
    totalMemoryMethod_ = env->GetMethodID(runtimeClass_, "totalMemory", "()J");
    if (!totalMemoryMethod_) {
      LOG(ERROR) << "Can't find totalMemory in Runtime class";
      return false;
    }

    return true;
  }

  inline static JavaVM* vm_{nullptr};
  inline static bool hasInit_{false};
  inline static std::mutex lock_;

  // JNI object cache
  inline static jclass runtimeClass_{nullptr};
  inline static jmethodID getRuntimeMethod_{nullptr};
  inline static jobject runtimeInstance_{nullptr};
  inline static jmethodID totalMemoryMethod_{nullptr};
};
} // namespace gluten
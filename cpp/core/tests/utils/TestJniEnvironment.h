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

#include <jni.h>

#include <thread>
#include <unordered_map>

namespace gluten {

// JNI function tables let tests account for references without relying on JVM garbage collection.
class TestJniEnvironment {
 private:
  struct Env : JNIEnv_ {
    explicit Env(TestJniEnvironment* owner) : owner(owner) {
      functions = &owner->functions_;
    }
    TestJniEnvironment* owner;
  };

  struct Vm : JavaVM_ {
    explicit Vm(TestJniEnvironment* owner) : owner(owner) {
      functions = &owner->invocation_;
    }
    TestJniEnvironment* owner;
  };

  static TestJniEnvironment& from(JNIEnv* env) {
    return *static_cast<Env*>(env)->owner;
  }

  static TestJniEnvironment& from(JavaVM* vm) {
    return *static_cast<Vm*>(vm)->owner;
  }

 public:
  TestJniEnvironment() {
    functions_.GetJavaVM = [](JNIEnv* env, JavaVM** vm) {
      *vm = &from(env).vm_;
      return JNI_OK;
    };
    functions_.ExceptionCheck = [](JNIEnv* env) -> jboolean { return from(env).pending != nullptr; };
    functions_.ExceptionOccurred = [](JNIEnv* env) { return from(env).pending; };
    functions_.ExceptionClear = [](JNIEnv* env) { from(env).pending = nullptr; };
    functions_.NewGlobalRef = [](JNIEnv* env, jobject object) -> jobject {
      auto& self = from(env);
      if (self.failGlobalRef) {
        self.pending = &self.secondary;
        return nullptr;
      }
      auto ref = new _jthrowable();
      self.globalRefs.emplace(ref, static_cast<jthrowable>(object));
      ++self.createdGlobalRefs;
      return ref;
    };
    functions_.DeleteGlobalRef = [](JNIEnv* env, jobject object) {
      auto& self = from(env);
      self.globalRefs.erase(object);
      delete static_cast<jthrowable>(object);
      ++self.deletedGlobalRefs;
      self.deletingEnv = env;
    };
    functions_.DeleteLocalRef = [](JNIEnv* env, jobject) { ++from(env).deletedLocalRefs; };
    functions_.Throw = [](JNIEnv* env, jthrowable throwable) {
      auto& self = from(env);
      self.pending = self.globalRefs.at(throwable);
      return JNI_OK;
    };
    functions_.PushLocalFrame = [](JNIEnv* env, jint) {
      auto& self = from(env);
      if (self.failLocalFrame) {
        self.pending = &self.secondary;
        return JNI_ERR;
      }
      ++self.pushedFrames;
      return JNI_OK;
    };
    functions_.PopLocalFrame = [](JNIEnv* env, jobject result) {
      ++from(env).poppedFrames;
      return result;
    };
    functions_.FindClass = [](JNIEnv* env, const char*) -> jclass {
      auto& self = from(env);
      if (self.failDescription) {
        self.pending = &self.secondary;
        return nullptr;
      }
      return &self.describer_;
    };
    functions_.GetStaticMethodID = [](JNIEnv* env, jclass, const char*, const char*) {
      return reinterpret_cast<jmethodID>(&from(env).describer_);
    };
    functions_.CallStaticObjectMethodV = [](JNIEnv* env, jclass, jmethodID, va_list) -> jobject {
      return &from(env).description_;
    };
    functions_.GetStringUTFChars = [](JNIEnv*, jstring, jboolean*) { return "original Java exception"; };
    functions_.ReleaseStringUTFChars = [](JNIEnv* env, jstring, const char*) { ++from(env).releasedStrings; };
    invocation_.GetEnv = [](JavaVM* vm, void** env, jint) {
      auto& self = from(vm);
      if (std::this_thread::get_id() == self.ownerThread_) {
        *env = &self.env_;
        return JNI_OK;
      }
      if (!self.workerAttached) {
        return JNI_EDETACHED;
      }
      *env = &self.workerEnv_;
      return JNI_OK;
    };
    invocation_.AttachCurrentThreadAsDaemon = [](JavaVM* vm, void** env, void*) {
      auto& self = from(vm);
      ++self.attaches;
      self.workerAttached = true;
      *env = &self.workerEnv_;
      return JNI_OK;
    };
    invocation_.DetachCurrentThread = [](JavaVM* vm) {
      auto& self = from(vm);
      ++self.detaches;
      self.workerAttached = false;
      return JNI_OK;
    };
  }

  JNIEnv* env() {
    return &env_;
  }

  JNIEnv* workerEnv() {
    return &workerEnv_;
  }

  _jthrowable original;
  _jthrowable secondary;
  jthrowable pending{nullptr};
  std::unordered_map<jobject, jthrowable> globalRefs;
  int createdGlobalRefs{0};
  int deletedGlobalRefs{0};
  int deletedLocalRefs{0};
  int pushedFrames{0};
  int poppedFrames{0};
  int releasedStrings{0};
  int attaches{0};
  int detaches{0};
  bool workerAttached{false};
  bool failGlobalRef{false};
  bool failDescription{false};
  bool failLocalFrame{false};
  JNIEnv* deletingEnv{nullptr};

 private:
  JNINativeInterface_ functions_{};
  JNIInvokeInterface_ invocation_{};
  Env env_{this};
  Env workerEnv_{this};
  Vm vm_{this};
  _jclass describer_;
  _jstring description_;
  const std::thread::id ownerThread_{std::this_thread::get_id()};
};

} // namespace gluten

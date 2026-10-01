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

#include <gtest/gtest.h>
#include <jni.h>
#include <cstdlib>
#include <string>

namespace gluten {

// Each test executable uses one JVM fixture; workers must finish before teardown.
class JniTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    JavaVMInitArgs args{};
    args.version = JNI_VERSION_1_8;
    JavaVMOption options[2]{};
    std::string classPathOption;
    if (const char* classPath = std::getenv("CLASSPATH")) {
      classPathOption = std::string("-Djava.class.path=") + classPath;
      options[0].optionString = classPathOption.data();
      // Hadoop can launch shell commands. Cold class initialization in an
      // embedded JVM can overflow JDK 17's small process-reaper stack.
      options[1].optionString = const_cast<char*>("-Djdk.lang.processReaperUseDefaultStackSize=true");
      args.nOptions = 2;
      args.options = options;
    }
    ASSERT_EQ(JNI_CreateJavaVM(&vm_, reinterpret_cast<void**>(&env_), &args), JNI_OK);
    auto local = env_->FindClass("java/lang/Thread");
    ASSERT_NE(local, nullptr);
    threadClass_ = static_cast<jclass>(env_->NewGlobalRef(local));
    env_->DeleteLocalRef(local);
    ASSERT_NE(threadClass_, nullptr);
    currentThread_ = env_->GetStaticMethodID(threadClass_, "currentThread", "()Ljava/lang/Thread;");
    isAlive_ = env_->GetMethodID(threadClass_, "isAlive", "()Z");
    ASSERT_NE(currentThread_, nullptr);
    ASSERT_NE(isAlive_, nullptr);
  }

  static void TearDownTestSuite() {
    if (vm_ != nullptr) {
      env_->DeleteGlobalRef(threadClass_);
      EXPECT_EQ(vm_->DestroyJavaVM(), JNI_OK);
    }
  }

  static jobject captureThread(JNIEnv* env) {
    auto local = env->CallStaticObjectMethod(threadClass_, currentThread_);
    auto global = env->NewGlobalRef(local);
    env->DeleteLocalRef(local);
    return global;
  }

  static void checkExited(jobject thread) {
    ASSERT_NE(thread, nullptr);
    EXPECT_FALSE(env_->CallBooleanMethod(thread, isAlive_));
    env_->DeleteGlobalRef(thread);
  }

  inline static JavaVM* vm_ = nullptr;
  inline static JNIEnv* env_ = nullptr;
  inline static jclass threadClass_ = nullptr;
  inline static jmethodID currentThread_ = nullptr;
  inline static jmethodID isAlive_ = nullptr;
};

} // namespace gluten

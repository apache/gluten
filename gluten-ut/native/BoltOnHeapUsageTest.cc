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

#include "JniTest.h"
#include "jni/JniThreadFactory.h"
#include "memory/OnHeapUsageGetter.h"

#include <folly/ThreadLocal.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <functional>

#if !defined(__linux__) || !defined(__GLIBC__)
TEST(BoltOnHeapUsageTest, requiresGlibcThreadExitOrdering) {
  GTEST_SKIP() << "Automatic JNI thread-exit cleanup is only enabled on Linux/glibc";
}
#else

namespace gluten {
namespace {

class BoltOnHeapUsageTest : public JniTest {
 protected:
  static void SetUpTestSuite() {
    ASSERT_NO_FATAL_FAILURE(JniTest::SetUpTestSuite());
    OnHeapMemUsedHookSetter::init(vm_);
    ASSERT_GT(OnHeapMemUsedHookSetter::getOnHeapUsedMemory(), 0);
    isDaemon_ = env_->GetMethodID(threadClass_, "isDaemon", "()Z");
    ASSERT_NE(isDaemon_, nullptr);
  }

  inline static jmethodID isDaemon_ = nullptr;
};

TEST_F(BoltOnHeapUsageTest, firstCallbackOnWorkerKeepsNonDaemonSemanticsAndDetachesAtExit) {
  jobject thread = nullptr;
  folly::CPUThreadPoolExecutor pool(1, std::make_shared<JniThreadFactory>("jni-bolt-memory-"));
  pool.add([&] {
    JNIEnv* env = nullptr;
    ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8), JNI_EDETACHED);
    ASSERT_GT(OnHeapMemUsedHookSetter::getOnHeapUsedMemory(), 0);
    ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8), JNI_OK);
    thread = captureThread(env);
    EXPECT_FALSE(env->CallBooleanMethod(thread, isDaemon_));
    for (int i = 0; i < 4; ++i) {
      ASSERT_GT(OnHeapMemUsedHookSetter::getOnHeapUsedMemory(), 0);
      JNIEnv* current = nullptr;
      ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&current), JNI_VERSION_1_8), JNI_OK);
      EXPECT_EQ(current, env);
      EXPECT_TRUE(env->CallBooleanMethod(thread, isAlive_));
    }
  });
  pool.join();
  checkExited(thread);
}

TEST_F(BoltOnHeapUsageTest, javaThreadAttachmentIsPreserved) {
  ASSERT_GT(OnHeapMemUsedHookSetter::getOnHeapUsedMemory(), 0);
  JNIEnv* current = nullptr;
  ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&current), JNI_VERSION_1_8), JNI_OK);
  EXPECT_EQ(current, env_);
}

TEST_F(BoltOnHeapUsageTest, firstCallbackFromFollyCleanupIsAlsoDetached) {
  struct OnExit {
    std::function<void()> action;
    ~OnExit() {
      if (action) {
        action();
      }
    }
  };
  folly::ThreadLocal<OnExit> cleanup;
  jobject thread = nullptr;
  folly::CPUThreadPoolExecutor pool(1, std::make_shared<JniThreadFactory>("jni-bolt-cleanup-"));
  pool.add([&] {
    cleanup->action = [&] {
      JNIEnv* env = nullptr;
      ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8), JNI_EDETACHED);
      ASSERT_GT(OnHeapMemUsedHookSetter::getOnHeapUsedMemory(), 0);
      ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8), JNI_OK);
      thread = captureThread(env);
      EXPECT_FALSE(env->CallBooleanMethod(thread, isDaemon_));
    };
  });
  pool.join();
  checkExited(thread);
}

} // namespace
} // namespace gluten

#endif

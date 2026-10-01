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

#include "jni/JniThreadFactory.h"
#include "JniTest.h"

#include <folly/ThreadLocal.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>
#include <atomic>
#include <functional>
#include <vector>

#if !defined(__linux__) || !defined(__GLIBC__)
TEST(JniThreadFactoryTest, requiresGlibcThreadExitOrdering) {
  GTEST_SKIP() << "Automatic JNI thread-exit cleanup is only enabled on Linux/glibc";
}
#else

namespace gluten {
namespace {

class JniThreadFactoryTest : public JniTest {
 protected:
  struct OnExit {
    std::function<void()> action;
    ~OnExit() {
      if (action) {
        action();
      }
    }
  };
};

TEST_F(JniThreadFactoryTest, poolWorkersDetachAfterFollyThreadLocalCleanup) {
  folly::ThreadLocal<OnExit> cleanup;
  std::atomic<int> cleanups{0};
  std::vector<jobject> threads(16, nullptr);
  folly::CPUThreadPoolExecutor pool(
      2, folly::CPUThreadPoolExecutor::makeLifoSemQueue(), std::make_shared<JniThreadFactory>("jni-test-"));
  for (size_t i = 0; i < threads.size(); ++i) {
    pool.add([&, i] {
      JNIEnv* env = nullptr;
      ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &env), JNI_OK);
      threads[i] = captureThread(env);
      cleanup->action = [&, env] {
        ++cleanups;
        JNIEnv* current = nullptr;
        ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&current), JNI_VERSION_1_8), JNI_OK);
        ASSERT_EQ(current, env);
        auto value = env->NewStringUTF("Folly cleanup can still call JNI");
        ASSERT_NE(value, nullptr);
        env->DeleteLocalRef(value);
      };
    });
  }
  pool.join();
  EXPECT_GT(cleanups, 0);
  for (auto thread : threads) {
    checkExited(thread);
  }
}

TEST_F(JniThreadFactoryTest, firstAttachmentFromFollyCleanupIsReleased) {
  folly::ThreadLocal<OnExit> cleanup;
  jobject thread = nullptr;
  folly::CPUThreadPoolExecutor pool(1, std::make_shared<JniThreadFactory>("jni-exit-test-"));
  pool.add([&] {
    cleanup->action = [&] {
      JNIEnv* env = nullptr;
      ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &env), JNI_OK);
      thread = captureThread(env);
    };
  });
  pool.join();
  checkExited(thread);
}

TEST_F(JniThreadFactoryTest, ioPoolWorkersAreDetached) {
  jobject thread = nullptr;
  folly::IOThreadPoolExecutor pool(1, std::make_shared<JniThreadFactory>("jni-io-test-"));
  pool.add([&] {
    JNIEnv* env = nullptr;
    ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &env), JNI_OK);
    thread = captureThread(env);
  });
  pool.join();
  checkExited(thread);
}

} // namespace
} // namespace gluten

#endif

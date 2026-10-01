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

#include "jni/JniThreadAttachment.h"
#include "JniTest.h"

#include <gtest/gtest.h>
#include <limits.h>
#include <pthread.h>
#include <atomic>
#include <future>
#include <thread>
#include <vector>

#if !defined(__linux__) || !defined(__GLIBC__)
TEST(JniThreadAttachmentTest, requiresGlibcThreadExitOrdering) {
  GTEST_SKIP() << "Automatic JNI thread-exit cleanup is only enabled on Linux/glibc";
}
#else

namespace gluten {
namespace {

template <typename Function>
std::thread startWorker(Function&& function) {
  return std::thread(withJniThreadLifecycle(std::forward<Function>(function)));
}

class FakeJvm {
 public:
  FakeJvm() {
    functions.reserved0 = this;
    functions.GetEnv = [](JavaVM* vm, void** out, jint) -> jint {
      auto& self = from(vm);
      *out = nullptr;
      if (self.getEnvError != JNI_OK) {
        return self.getEnvError;
      }
      if (!attached) {
        return JNI_EDETACHED;
      }
      *out = &self.env;
      return JNI_OK;
    };
    functions.AttachCurrentThread = [](JavaVM* vm, void** out, void*) -> jint {
      auto& self = from(vm);
      ++self.attachCalls;
      if (self.attachError != JNI_OK) {
        return self.attachError;
      }
      attached = true;
      *out = &self.env;
      return JNI_OK;
    };
    functions.AttachCurrentThreadAsDaemon = [](JavaVM* vm, void** out, void* args) -> jint {
      ++from(vm).daemonAttachCalls;
      return vm->AttachCurrentThread(out, args);
    };
    functions.DetachCurrentThread = [](JavaVM* vm) -> jint {
      ++from(vm).detachCalls;
      attached = false;
      return JNI_OK;
    };
  }

  static FakeJvm& from(JavaVM* vm) {
    return *static_cast<FakeJvm*>(vm->functions->reserved0);
  }

  static thread_local bool attached;
  JNIInvokeInterface_ functions{};
  JavaVM vm{&functions};
  JNIEnv env{};
  std::atomic<int> attachCalls{0};
  std::atomic<int> daemonAttachCalls{0};
  std::atomic<int> detachCalls{0};
  jint getEnvError = JNI_OK;
  jint attachError = JNI_OK;
};

thread_local bool FakeJvm::attached = false;

using AttachFunction = jint (*)(JavaVM*, JNIEnv**);
class JniThreadAttachment : public testing::TestWithParam<AttachFunction> {};

TEST_P(JniThreadAttachment, preserveExistingAttachment) {
  FakeJvm jvm;
  auto worker = startWorker([&] {
    FakeJvm::attached = true;
    JNIEnv* env = nullptr;
    ASSERT_EQ(GetParam()(&jvm.vm, &env), JNI_OK);
    EXPECT_EQ(env, &jvm.env);
  });
  worker.join();
  EXPECT_EQ(jvm.attachCalls, 0);
  EXPECT_EQ(jvm.detachCalls, 0);
}

TEST_P(JniThreadAttachment, failedAttachmentHasNoCleanup) {
  FakeJvm jvm;
  jvm.attachError = JNI_ERR;
  auto worker = startWorker([&] {
    JNIEnv* env = nullptr;
    EXPECT_EQ(GetParam()(&jvm.vm, &env), JNI_ERR);
    EXPECT_EQ(env, nullptr);
  });
  worker.join();
  EXPECT_EQ(jvm.attachCalls, 1);
  EXPECT_EQ(jvm.detachCalls, 0);
}

TEST_P(JniThreadAttachment, propagateGetEnvError) {
  FakeJvm jvm;
  jvm.getEnvError = JNI_EVERSION;
  auto worker = startWorker([&] {
    JNIEnv* env = nullptr;
    EXPECT_EQ(GetParam()(&jvm.vm, &env), JNI_EVERSION);
    EXPECT_EQ(env, nullptr);
  });
  worker.join();
  EXPECT_EQ(jvm.attachCalls, 0);
  EXPECT_EQ(jvm.detachCalls, 0);
}

TEST_P(JniThreadAttachment, explicitDetachAndReattach) {
  for (bool reattach : {false, true}) {
    SCOPED_TRACE(reattach);
    FakeJvm jvm;
    auto worker = startWorker([&] {
      JNIEnv* env = nullptr;
      ASSERT_EQ(GetParam()(&jvm.vm, &env), JNI_OK);
      ASSERT_EQ(jvm.vm.DetachCurrentThread(), JNI_OK);
      if (reattach) {
        ASSERT_EQ(GetParam()(&jvm.vm, &env), JNI_OK);
      }
    });
    worker.join();
    EXPECT_EQ(jvm.attachCalls, reattach ? 2 : 1);
    EXPECT_EQ(jvm.detachCalls, jvm.attachCalls.load());
    EXPECT_EQ(jvm.daemonAttachCalls, GetParam() == &getOrAttachCurrentThreadAsDaemon ? jvm.attachCalls.load() : 0);
  }
}

TEST_P(JniThreadAttachment, unmanagedThreadsKeepTheirExistingLifecycle) {
  FakeJvm jvm;
  std::thread worker([&] {
    JNIEnv* env = nullptr;
    ASSERT_EQ(GetParam()(&jvm.vm, &env), JNI_OK);
  });
  worker.join();
  EXPECT_EQ(jvm.attachCalls, 1);
  EXPECT_EQ(jvm.detachCalls, 0);
}

INSTANTIATE_TEST_SUITE_P(
    AttachmentModes,
    JniThreadAttachment,
    testing::Values(&getOrAttachCurrentThreadAsDaemon, &getOrAttachCurrentThread),
    [](const testing::TestParamInfo<AttachFunction>& info) { return info.index == 0 ? "Daemon" : "NonDaemon"; });

class JniThreadAttachmentJvmTest : public JniTest {
 protected:
  struct Cleanup {
    JavaVM* vm;
    JNIEnv* cachedEnv = nullptr;
    bool ran = false;
    bool detach = false;

    void run() {
      ran = true;
      JNIEnv* env = nullptr;
      // Check before using the cached env so a broken cleanup order fails an assertion,
      // rather than dereferencing a freed JNIEnv and crashing the test process.
      ASSERT_EQ(vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8), JNI_OK);
      ASSERT_EQ(env, cachedEnv);
      auto value = env->NewStringUTF("JNI is still usable during thread cleanup");
      ASSERT_NE(value, nullptr);
      env->DeleteLocalRef(value);
      if (detach) {
        ASSERT_EQ(vm->DetachCurrentThread(), JNI_OK);
      }
    }
  };
};

TEST_F(JniThreadAttachmentJvmTest, workersDetachAfterTheirLastCallback) {
  JNIEnv* existingEnv = nullptr;
  ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &existingEnv), JNI_OK);
  EXPECT_EQ(existingEnv, env_);
  std::vector<jobject> threads(32, nullptr);
  std::vector<std::thread> workers;
  for (auto& thread : threads) {
    workers.push_back(startWorker([&] {
      initializeNativeThreadJni(); // Registration is idempotent and does not attach.
      JNIEnv* cachedEnv = nullptr;
      ASSERT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&cachedEnv), JNI_VERSION_1_8), JNI_EDETACHED);
      ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &cachedEnv), JNI_OK);
      thread = captureThread(cachedEnv);
      for (int i = 0; i < 4; ++i) {
        JNIEnv* env = nullptr;
        ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &env), JNI_OK);
        EXPECT_EQ(env, cachedEnv);
        EXPECT_TRUE(env->CallBooleanMethod(thread, isAlive_));
        auto value = env->NewStringUTF("another callback on the same worker");
        ASSERT_NE(value, nullptr);
        env->DeleteLocalRef(value);
      }
    }));
  }
  for (auto& worker : workers) {
    worker.join();
  }
  for (auto thread : threads) {
    checkExited(thread);
  }
  EXPECT_EQ(vm_->GetEnv(reinterpret_cast<void**>(&existingEnv), JNI_VERSION_1_8), JNI_OK);
  EXPECT_EQ(existingEnv, env_);
}

TEST_F(JniThreadAttachmentJvmTest, cppThreadLocalCleanupCanStillUseJni) {
  Cleanup cleanup{vm_};
  jobject thread = nullptr;
  auto worker = startWorker([&] {
    struct BeforeAttachment {
      Cleanup* cleanup;
      ~BeforeAttachment() {
        cleanup->run();
      }
    };
    // Construct before attaching: a C++ thread_local detacher would be destroyed
    // first, invalidating the env before this older thread_local object's cleanup.
    thread_local BeforeAttachment beforeAttachment{&cleanup};
    ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &cleanup.cachedEnv), JNI_OK);
    thread = captureThread(cleanup.cachedEnv);
  });
  worker.join();
  EXPECT_TRUE(cleanup.ran);
  checkExited(thread);
}

TEST_F(JniThreadAttachmentJvmTest, asyncWorkerIsDetachedWhenFutureJoins) {
  jobject thread = nullptr;
  auto result = std::async(std::launch::async, withJniThreadLifecycle([&] {
                             JNIEnv* env = nullptr;
                             ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &env), JNI_OK);
                             thread = captureThread(env);
                           }));
  result.get();
  checkExited(thread);
}

TEST_F(JniThreadAttachmentJvmTest, pthreadCleanupCanStillUseJni) {
  // Install Gluten's key first, even when this case runs in its own process.
  startWorker([] {}).join();
  for (bool detach : {false, true}) {
    SCOPED_TRACE(detach);
    Cleanup cleanup{vm_};
    cleanup.detach = detach; // A library may release the attachment before Gluten.
    pthread_key_t key{};
    ASSERT_EQ(pthread_key_create(&key, [](void* value) { static_cast<Cleanup*>(value)->run(); }), 0);
    jobject thread = nullptr;
    auto worker = startWorker([&] {
      ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &cleanup.cachedEnv), JNI_OK);
      thread = captureThread(cleanup.cachedEnv);
      ASSERT_EQ(pthread_setspecific(key, &cleanup), 0);
    });
    worker.join();
    EXPECT_TRUE(cleanup.ran);
    EXPECT_EQ(pthread_key_delete(key), 0);
    checkExited(thread);
  }
}

TEST_F(JniThreadAttachmentJvmTest, firstAttachmentDuringPthreadCleanupIsReleased) {
  // Allocate Gluten's key before the callback key, including when this test is
  // run alone. The first destructor pass can already have visited Gluten's key
  // by the time a later callback attaches for the first time.
  auto primeKey = startWorker([&] {
    JNIEnv* env = nullptr;
    ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &env), JNI_OK);
  });
  primeKey.join();
  pthread_key_t key{};
  jobject thread = nullptr;
  ASSERT_EQ(
      pthread_key_create(
          &key,
          [](void* value) {
            JNIEnv* env = nullptr;
            ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &env), JNI_OK);
            *static_cast<jobject*>(value) = captureThread(env);
          }),
      0);
  auto worker = startWorker([&] { ASSERT_EQ(pthread_setspecific(key, &thread), 0); });
  worker.join();
  EXPECT_EQ(pthread_key_delete(key), 0);
  checkExited(thread);
}

TEST_F(JniThreadAttachmentJvmTest, lateDestructorPassCannotLeaveANewAttachmentBehind) {
  auto primeKey = startWorker([] {});
  primeKey.join();
  for (int attachPass = 2; attachPass <= PTHREAD_DESTRUCTOR_ITERATIONS; ++attachPass) {
    struct LateCallback {
      pthread_key_t key{};
      int remaining;
      jint status = JNI_ERR;
      jobject thread = nullptr;
    } callback{{}, attachPass};
    ASSERT_EQ(
        pthread_key_create(
            &callback.key,
            [](void* value) {
              auto* callback = static_cast<LateCallback*>(value);
              if (--callback->remaining > 0) {
                ASSERT_EQ(pthread_setspecific(callback->key, callback), 0);
                return;
              }
              JNIEnv* env = nullptr;
              callback->status = getOrAttachCurrentThreadAsDaemon(vm_, &env);
              if (callback->status == JNI_OK) {
                callback->thread = captureThread(env);
              } else {
                EXPECT_EQ(env, nullptr);
              }
            }),
        0);
    auto worker = startWorker([&] { ASSERT_EQ(pthread_setspecific(callback.key, &callback), 0); });
    worker.join();
    EXPECT_EQ(pthread_key_delete(callback.key), 0);
    if (attachPass < PTHREAD_DESTRUCTOR_ITERATIONS) {
      EXPECT_EQ(callback.status, JNI_OK);
    }
    // POSIX does not specify key ordering within the final pass. An attachment
    // must either be released by the pending finalizer or rejected if it ran.
    if (callback.status == JNI_OK) {
      checkExited(callback.thread);
    } else {
      EXPECT_EQ(callback.status, JNI_ERR);
      EXPECT_EQ(attachPass, PTHREAD_DESTRUCTOR_ITERATIONS);
    }
  }
}

} // namespace
} // namespace gluten

#endif

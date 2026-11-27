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

#include <gtest/gtest.h>

#include <cstdarg>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "compute/VeloxBackend.h"
#include "config/GlutenConfig.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"
#include "jni/VeloxAllocationListener.h"
#include "tests/utils/TestJniEnvironment.h"

namespace gluten {
namespace {
using facebook::velox::VeloxException;

template <typename F>
bool atJniBoundary(JNIEnv* env, F&& call) {
  JNI_METHOD_START
  call();
  return true;
  JNI_METHOD_END(false)
}

// One process-wide JNI mock: the listener caches its Java class and method IDs.
class VeloxJniAllocationListenerTest : public ::testing::Test {
 protected:
  static inline TestJniEnvironment jni;
  static inline JavaVM* vm;
  static inline _jthrowable javaListener;
  static inline char reserveId;
  static inline char unreserveId;
  static inline jthrowable reserveError;
  static inline jthrowable unreserveError;
  // Signed byte changes passed to Java, in call order.
  static inline std::vector<jlong> calls;
  static inline const JNINativeInterface_* originalFunctions;
  static inline JNINativeInterface_ functions;

  static void SetUpTestSuite() {
    originalFunctions = jni.env()->functions;
    functions = *originalFunctions;
    functions.GetMethodID = [](JNIEnv*, jclass, const char* name, const char*) {
      return reinterpret_cast<jmethodID>(std::string_view(name) == "reserve" ? &reserveId : &unreserveId);
    };
    functions.CallLongMethodV = [](JNIEnv* env, jobject, jmethodID method, va_list args) -> jlong {
      EXPECT_FALSE(env->ExceptionCheck()) << "Java callback entered with an exception pending";
      const auto bytes = va_arg(args, jlong);
      const bool reserve = method == reinterpret_cast<jmethodID>(&reserveId);
      calls.push_back(reserve ? bytes : -bytes);
      if (const auto error = std::exchange(reserve ? reserveError : unreserveError, nullptr)) {
        env->Throw(error);
      }
      return bytes;
    };
    jni.env()->functions = jni.workerEnv()->functions = &functions;
    ASSERT_EQ(jni.env()->GetJavaVM(&vm), JNI_OK);
    getJniErrorState()->ensureInitialized(jni.env());
    // The JNI backend initializer creates its global listener with the same factory.
    VeloxBackend::create(createVeloxJniAllocationListener(vm, &javaListener), {{kSessionTimezone, "UTC"}});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
    getJniErrorState()->close();
    jni.env()->functions = jni.workerEnv()->functions = originalFunctions;
  }

  void SetUp() override {
    calls.clear();
  }

  void TearDown() override {
    reserveError = unreserveError = nullptr;
    jni.pending = nullptr;
    jni.failGlobalRef = false;
    jni.workerAttached = false;
  }

  // The listener NativeMemoryManagerJniWrapper.create builds for a task.
  static std::unique_ptr<AllocationListener> taskListener(const std::string& backend = kVeloxBackendKind) {
    return createJniAllocationListener(backend, vm, &javaListener);
  }

  static const facebook::velox::config::ConfigBase& backendConf() {
    return *VeloxBackend::get()->getBackendConf();
  }

  static void allocate(VeloxMemoryManager& manager, int64_t bytes) {
    auto* pool = manager.getLeafMemoryPool().get();
    pool->free(pool->allocate(bytes), bytes);
  }

  // Leaves free capacity, so a larger allocation reclaims it before reserving more.
  static int64_t leaveFreeCapacity(VeloxMemoryManager& manager) {
    allocate(manager, 64);
    calls.clear();
    return manager.getAggregateMemoryPool()->capacity();
  }

  static void expectVeloxSystemError(const std::exception& error) {
    const auto* veloxError = dynamic_cast<const VeloxException*>(&error);
    ASSERT_NE(veloxError, nullptr) << error.what();
    // A system error passes through the driver and TRY with its Java cause.
    EXPECT_EQ(veloxError->exceptionType(), VeloxException::Type::kSystem);
    EXPECT_NE(veloxError->wrappedException(), nullptr);
  }
};

TEST_F(VeloxJniAllocationListenerTest, reservationFailureKeepsTheJavaThrowable) {
  enum class Path { kGlobal, kVeloxPool, kArrowAllocator };
  for (const auto path : {Path::kGlobal, Path::kVeloxPool, Path::kArrowAllocator}) {
    SCOPED_TRACE(static_cast<int>(path));
    VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
    auto* listener =
        path == Path::kGlobal ? VeloxBackend::get()->getGlobalMemoryManager()->getListener() : manager.getListener();
    const auto bytes = listener->currentBytes();
    const auto refs = jni.createdGlobalRefs - jni.deletedGlobalRefs;
    reserveError = &jni.original;
    try {
      if (path == Path::kGlobal) {
        listener->allocationChanged(64);
      } else if (path == Path::kVeloxPool) {
        allocate(manager, 64);
      } else {
        void* buffer = nullptr;
        if (manager.allocator()->allocate(64, &buffer)) {
          manager.allocator()->free(buffer, 64);
        }
      }
      FAIL() << "Expected the reservation to fail";
    } catch (const std::exception& error) {
      expectVeloxSystemError(error);
      EXPECT_EQ(jni.pending, nullptr);
      EXPECT_FALSE(atJniBoundary(jni.env(), [] { throw; }));
    }
    EXPECT_EQ(jni.pending, &jni.original);
    EXPECT_EQ(listener->currentBytes(), bytes);
    EXPECT_EQ(jni.createdGlobalRefs - jni.deletedGlobalRefs, refs);
    jni.pending = nullptr;
  }
}

TEST_F(VeloxJniAllocationListenerTest, unreserveFailureKeepsTheJavaThrowable) {
  VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
  const auto capacity = leaveFreeCapacity(manager);
  unreserveError = &jni.original;
  try {
    manager.shrink(0);
    FAIL() << "Expected the unreserve to fail";
  } catch (const std::exception& error) {
    expectVeloxSystemError(error);
    EXPECT_FALSE(atJniBoundary(jni.env(), [] { throw; }));
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(calls, std::vector<jlong>{-capacity});
  EXPECT_EQ(manager.getListener()->currentBytes(), capacity);
}

TEST_F(VeloxJniAllocationListenerTest, rollbackFailureKeepsTheReservationError) {
  {
    VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
    const auto capacity = leaveFreeCapacity(manager);
    reserveError = &jni.original;
    unreserveError = &jni.secondary;
    EXPECT_FALSE(atJniBoundary(jni.env(), [&] { allocate(manager, capacity + 1); }));
    EXPECT_EQ(jni.pending, &jni.original);
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_GT(calls.front(), 0);
    EXPECT_EQ(calls.back(), -capacity);
    jni.pending = nullptr;
  }

  struct NativeListener final : AllocationListener {
    bool fail{false};
    void allocationChanged(int64_t diff) override {
      if (fail && diff > 0) {
        throw std::runtime_error("reserve");
      }
      if (fail && diff < 0) {
        throw std::logic_error("unreserve");
      }
    }
  };
  auto owned = std::make_unique<NativeListener>();
  auto* native = owned.get();
  VeloxMemoryManager manager(kVeloxBackendKind, std::move(owned), backendConf());
  const auto capacity = leaveFreeCapacity(manager);
  native->fail = true;
  EXPECT_THROW(allocate(manager, capacity + 1), std::runtime_error);
  native->fail = false;
}

TEST_F(VeloxJniAllocationListenerTest, javaIsNotCalledWithAnExceptionPending) {
  {
    // Capturing the reserve failure fails, as when memory is short, and leaves a newer exception pending.
    VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
    const auto capacity = leaveFreeCapacity(manager);
    reserveError = &jni.original;
    jni.failGlobalRef = true;
    EXPECT_FALSE(atJniBoundary(jni.env(), [&] { allocate(manager, capacity + 1); }));
    jni.failGlobalRef = false;
    EXPECT_EQ(jni.pending, &jni.secondary);
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls.back(), -capacity);
    EXPECT_EQ(manager.getListener()->currentBytes(), 0);
    jni.pending = nullptr;
  }

  calls.clear();
  auto listener = taskListener();
  jni.env()->Throw(&jni.secondary);
  EXPECT_FALSE(atJniBoundary(jni.env(), [&] { listener->allocationChanged(64); }));
  EXPECT_TRUE(calls.empty());
  EXPECT_EQ(jni.pending, &jni.secondary);
  EXPECT_EQ(listener->currentBytes(), 0);
  jni.pending = nullptr;

  // An attached pool thread has no JNI return, so it must not keep the exception.
  listener->allocationChanged(64);
  calls.clear();
  jni.workerAttached = true;
  std::exception_ptr error;
  std::thread worker([&] {
    EXPECT_FALSE(JniEntryGuard::isActive());
    jni.workerEnv()->Throw(&jni.secondary);
    try {
      listener->allocationChanged(-64);
    } catch (...) {
      error = std::current_exception();
    }
    EXPECT_EQ(jni.pending, nullptr);
  });
  worker.join();
  EXPECT_EQ(calls, std::vector<jlong>{-64});
  EXPECT_EQ(listener->currentBytes(), 0);
  ASSERT_NE(error, nullptr);
  EXPECT_FALSE(atJniBoundary(jni.env(), [&] { std::rethrow_exception(error); }));
  EXPECT_EQ(jni.pending, &jni.secondary);
}

TEST_F(VeloxJniAllocationListenerTest, onlyVeloxJavaListenersAreWrapped) {
  auto otherBackend = taskListener("other-backend");
  reserveError = &jni.original;
  EXPECT_THROW(otherBackend->allocationChanged(64), JavaException);

  struct NativeListener final : AllocationListener {
    std::exception_ptr error;
    void allocationChanged(int64_t diff) override {
      if (diff > 0) {
        std::rethrow_exception(error);
      }
    }
  };
  auto owned = std::make_unique<NativeListener>();
  const auto error = std::make_exception_ptr(std::runtime_error("native reservation failure"));
  owned->error = error;
  VeloxMemoryManager manager(kVeloxBackendKind, std::move(owned), backendConf());
  try {
    allocate(manager, 64);
    FAIL() << "Expected the native exception";
  } catch (const std::runtime_error&) {
    EXPECT_EQ(std::current_exception(), error);
  }
}

} // namespace
} // namespace gluten

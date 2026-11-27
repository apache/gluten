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
#include <initializer_list>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "compute/VeloxBackend.h"
#include "config/GlutenConfig.h"
#include "jni/JniError.h"
#include "jni/VeloxAllocationListener.h"
#include "tests/utils/TestAllocationFailure.h"
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

class VeloxJniAllocationListenerTest : public ::testing::Test {
 protected:
  static inline TestJniEnvironment jni;
  static inline JavaVM* vm;
  static inline _jthrowable listenerObject;
  static inline char reserveId, unreserveId;
  static inline jthrowable reserveError, unreserveError, failCaptureOf;
  static inline jlong javaBytes;
  static inline std::vector<jlong> calls;
  static inline const JNINativeInterface_* original;
  static inline JNINativeInterface_ functions;

  static void SetUpTestSuite() {
    original = jni.env()->functions;
    functions = *original;
    functions.GetMethodID = [](JNIEnv* env, jclass cls, const char* name, const char* signature) {
      if (std::string_view(name) == "reserve") {
        return reinterpret_cast<jmethodID>(&reserveId);
      }
      if (std::string_view(name) == "unreserve") {
        return reinterpret_cast<jmethodID>(&unreserveId);
      }
      return original->GetMethodID(env, cls, name, signature);
    };
    functions.CallLongMethodV = [](JNIEnv* env, jobject, jmethodID method, va_list args) -> jlong {
      EXPECT_FALSE(env->ExceptionCheck()) << "Called Java with an exception pending";
      const auto bytes = va_arg(args, jlong);
      const bool reserve = method == reinterpret_cast<jmethodID>(&reserveId);
      calls.push_back(reserve ? bytes : -bytes);
      if (!reserve || !reserveError) {
        // Like the JVM test listener, unreserve repays before it throws.
        javaBytes += reserve ? bytes : -bytes;
      }
      if (const auto error = std::exchange(reserve ? reserveError : unreserveError, nullptr)) {
        env->Throw(error);
        jni.failGlobalRef = error == failCaptureOf;
      }
      return bytes;
    };
    jni.env()->functions = jni.workerEnv()->functions = &functions;
    ASSERT_EQ(jni.env()->GetJavaVM(&vm), JNI_OK);
    getJniCommonState()->ensureInitialized(jni.env());
    getJniErrorState()->ensureInitialized(jni.env());
    VeloxBackend::create(createVeloxJniAllocationListener(vm, &listenerObject), {{kSessionTimezone, "UTC"}});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
    getJniErrorState()->close();
    getJniCommonState()->close();
    jni.env()->functions = jni.workerEnv()->functions = original;
  }

  void SetUp() override {
    calls.clear();
    javaBytes = 0;
  }

  void TearDown() override {
    functions.DeleteLocalRef = original->DeleteLocalRef;
    reserveError = unreserveError = failCaptureOf = jni.pending = nullptr;
    jni.workerAttached = jni.failGlobalRef = jni.failThrow = test::failNextAllocation = false;
  }

  static std::unique_ptr<AllocationListener> taskListener(const std::string& backend = kVeloxBackendKind) {
    return createJniAllocationListener(backend, vm, &listenerObject);
  }

  static auto& backendConf() {
    return *VeloxBackend::get()->getBackendConf();
  }

  static void allocate(VeloxMemoryManager& manager, int64_t bytes) {
    auto* pool = manager.getLeafMemoryPool().get();
    pool->free(pool->allocate(bytes), bytes);
  }

  template <typename F>
  static void expectWrapped(F&& call, bool expectFallback = false) {
    bool systemError = false;
    bool hasWrappedCause = false;
    const auto returned = atJniBoundary(jni.env(), [&] {
      try {
        call();
      } catch (const VeloxException& error) {
        systemError = error.exceptionType() == VeloxException::Type::kSystem;
        hasWrappedCause = error.wrappedException() != nullptr;
        throw;
      }
    });
    test::failNextAllocation = false;
    EXPECT_FALSE(returned);
    EXPECT_TRUE(systemError);
    // The preallocated wrapper has no per-call wrappedException; its Java cause is nested instead.
    EXPECT_EQ(hasWrappedCause, !expectFallback);
    EXPECT_EQ(jni.pending, &jni.original);
    jni.pending = nullptr;
  }
};

TEST_F(VeloxJniAllocationListenerTest, javaReservationsAreWrappedAtTheCallback) {
  VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
  reserveError = &jni.original;
  expectWrapped([] { VeloxBackend::get()->getGlobalMemoryManager()->getListener()->allocationChanged(64); });
  reserveError = &jni.original;
  expectWrapped([&] { allocate(manager, 64); });
  reserveError = &jni.original;
  expectWrapped([&] {
    void* buffer = nullptr;
    if (manager.allocator()->allocate(64, &buffer)) {
      manager.allocator()->free(buffer, 64);
    }
  });
  auto other = taskListener("other-backend");
  reserveError = &jni.original;
  EXPECT_THROW(other->allocationChanged(64), JavaException);
}

TEST_F(VeloxJniAllocationListenerTest, pendingThrowableSurvivesOptionalAndRequiredGrowth) {
  for (bool optional : {false, true}) {
    SCOPED_TRACE(optional);
    VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
    auto pool = manager.getLeafMemoryPool();
    reserveError = failCaptureOf = &jni.original;
    calls.clear();
    const bool returned = atJniBoundary(jni.env(), [&] {
      EXPECT_FALSE(pool->maybeReserve(64));
      EXPECT_EQ(jni.pending, &jni.secondary);
      jni.failGlobalRef = false; // Only the first callback failed capture.
      if (optional) {
        EXPECT_FALSE(pool->maybeReserve(64));
      } else {
        allocate(manager, 64);
      }
    });
    EXPECT_EQ(returned, optional);
    EXPECT_EQ(calls.size(), 1u); // Neither retry can call Java with the earlier error pending.
    EXPECT_EQ(jni.pending, &jni.secondary);
    jni.pending = nullptr;
  }
}

TEST_F(VeloxJniAllocationListenerTest, rollbackPreservesCapturedAndPendingJavaFailures) {
  for (auto failCapture : std::initializer_list<jthrowable>{nullptr, &jni.original, &jni.fallback}) {
    SCOPED_TRACE(failCapture);
    VeloxMemoryManager manager(
        kVeloxBackendKind, std::make_unique<BacktraceAllocationListener>(taskListener()), backendConf());
    allocate(manager, 64);
    const auto capacity = manager.getAggregateMemoryPool()->capacity();
    calls.clear();
    reserveError = &jni.original;
    unreserveError = &jni.fallback;
    failCaptureOf = failCapture;
    EXPECT_FALSE(atJniBoundary(jni.env(), [&] { allocate(manager, capacity + 1); }));
    EXPECT_EQ(jni.pending, failCapture == &jni.original ? &jni.secondary : &jni.original);
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_GT(calls.front(), 0);
    EXPECT_EQ(calls.back(), -capacity);
    jni.pending = nullptr;
    jni.failGlobalRef = false;
  }
}

TEST_F(VeloxJniAllocationListenerTest, nativeRollbackPrecedenceIsUnchanged) {
  struct NativeListener final : AllocationListener {
    void allocationChanged(int64_t) override {
      throw std::logic_error("native unreserve failed");
    }
  } native;
  EXPECT_THROW(rollbackVeloxReservation(native, 64, std::runtime_error("native reserve failed")), std::logic_error);
}

TEST_F(VeloxJniAllocationListenerTest, ordinaryShrinkRepaysWhileACaptureErrorIsPending) {
  for (bool failUnreserve : {false, true}) {
    SCOPED_TRACE(failUnreserve);
    VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
    allocate(manager, 64);
    const auto capacity = manager.getAggregateMemoryPool()->capacity();
    calls.clear();
    reserveError = failCaptureOf = &jni.original;
    unreserveError = failUnreserve ? &jni.fallback : nullptr;
    bool shrinkReturned = false;
    EXPECT_FALSE(atJniBoundary(jni.env(), [&] {
      try {
        manager.getListener()->allocationChanged(64);
      } catch (const std::runtime_error&) {
        EXPECT_EQ(jni.pending, &jni.secondary);
        // Independent cleanup, not the arbitrator's rollback of the failed growth.
        manager.shrink(0);
        shrinkReturned = true;
        throw;
      }
    }));
    EXPECT_EQ(shrinkReturned, !failUnreserve);
    EXPECT_EQ(jni.pending, &jni.secondary);
    EXPECT_EQ(calls, (std::vector<jlong>{64, -capacity}));
    EXPECT_EQ(manager.getAggregateMemoryPool()->capacity(), 0);
    EXPECT_EQ(javaBytes, 0);
    jni.pending = nullptr;
    jni.failGlobalRef = false;
  }
}

TEST_F(VeloxJniAllocationListenerTest, failedRestoreAfterFreeDoesNotUndoRepayment) {
  constexpr int64_t block = kMemoryReservationBlockSizeDefault;
  auto listener = taskListener();
  BlockAllocationListener blocks(listener.get(), block);
  StdMemoryAllocator heap;
  ListenableMemoryAllocator allocator(&heap, &blocks);
  void* buffer = nullptr;
  ASSERT_TRUE(allocator.allocate(block, &buffer));
  jni.env()->Throw(&jni.original);
  jni.failThrow = true;
  // Native free succeeds; the guarded Java caller still gets an exception.
  EXPECT_TRUE(atJniBoundary(jni.env(), [&] { allocator.free(buffer, block); }));
  EXPECT_EQ(jni.pending, &jni.fallback);
  EXPECT_EQ(javaBytes, 0);
  EXPECT_EQ(allocator.getBytes(), 0);
  jni.pending = nullptr;
  jni.failThrow = false;
  calls.clear();
  ASSERT_TRUE(allocator.allocate(64, &buffer));
  EXPECT_EQ(calls, std::vector<jlong>{block});
  allocator.free(buffer, 64);
}

TEST_F(VeloxJniAllocationListenerTest, workerRollbackPreservesOnlyACapturedJavaError) {
  for (bool failCapture : {false, true}) {
    SCOPED_TRACE(failCapture);
    VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
    allocate(manager, 64);
    const auto capacity = manager.getAggregateMemoryPool()->capacity();
    reserveError = &jni.original;
    unreserveError = &jni.secondary;
    failCaptureOf = failCapture ? &jni.original : nullptr;
    jni.workerAttached = true;
    calls.clear();
    std::exception_ptr error;
    std::thread worker([&] {
      EXPECT_FALSE(JniEntryGuard::isActive());
      try {
        allocate(manager, capacity + 1);
      } catch (...) {
        error = std::current_exception();
      }
      EXPECT_FALSE(jni.workerEnv()->ExceptionCheck());
    });
    worker.join();
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls.back(), -capacity);
    ASSERT_NE(error, nullptr);
    EXPECT_FALSE(atJniBoundary(jni.env(), [&] { std::rethrow_exception(error); }));
    EXPECT_EQ(jni.pending, failCapture ? &jni.secondary : &jni.original);
    jni.pending = nullptr;
  }
}

TEST_F(VeloxJniAllocationListenerTest, wrapperAllocationFailureKeepsTheJavaThrowable) {
  VeloxMemoryManager manager(kVeloxBackendKind, taskListener(), backendConf());
  allocate(manager, 64);
  unreserveError = &jni.original;
  functions.DeleteLocalRef = [](JNIEnv* env, jobject ref) {
    original->DeleteLocalRef(env, ref);
    if (ref == &jni.original) {
      test::failNextAllocation = true; // Fail after capture, while constructing the Velox wrapper.
    }
  };
  expectWrapped([&] { manager.shrink(0); }, /*expectFallback=*/true);
}

TEST_F(VeloxJniAllocationListenerTest, nextJavaChargeAfterThrowingUnreserveMatchesThePlainListener) {
  constexpr int64_t block = kMemoryReservationBlockSizeDefault;
  for (const auto& backend : {std::string("plain"), std::string(kVeloxBackendKind)}) {
    SCOPED_TRACE(backend);
    auto listener = taskListener(backend);
    BlockAllocationListener blocks(listener.get(), block);
    StdMemoryAllocator heap;
    ListenableMemoryAllocator allocator(&heap, &blocks);
    void* buffer = nullptr;
    ASSERT_TRUE(allocator.allocate(block, &buffer));
    ASSERT_EQ(javaBytes, block);
    unreserveError = &jni.original;
    EXPECT_FALSE(atJniBoundary(jni.env(), [&] { allocator.free(buffer, block); }));
    EXPECT_EQ(jni.pending, &jni.original);
    jni.pending = nullptr;
    ASSERT_EQ(javaBytes, 0);
    calls.clear();
    ASSERT_TRUE(allocator.allocate(block, &buffer));
    EXPECT_EQ(calls, std::vector<jlong>{block});
    EXPECT_EQ(javaBytes, block);
    allocator.free(buffer, block);
    EXPECT_EQ(javaBytes, 0);
  }
}

} // namespace
} // namespace gluten

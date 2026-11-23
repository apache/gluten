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

#include "compute/VeloxBackend.h"
#include "compute/VeloxRuntime.h"
#include "compute/WholeStageResultIterator.h"
#include "config/GlutenConfig.h"
#include "jni/JniError.h"
#include "jni/VeloxJavaException.h"
#include "tests/utils/TestJniEnvironment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace gluten {
namespace {

using namespace facebook::velox;

class VeloxJavaExceptionTest : public ::testing::Test {
 protected:
  static inline TestJniEnvironment boundaryJni;

  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {{kSessionTimezone, "UTC"}});
    getJniErrorState()->ensureInitialized(boundaryJni.env());
  }

  static void TearDownTestSuite() {
    getJniErrorState()->close();
    EXPECT_EQ(boundaryJni.createdGlobalRefs, boundaryJni.deletedGlobalRefs);
    VeloxBackend::get()->tearDown();
  }
};

TEST_F(VeloxJavaExceptionTest, callbackIsWrappedBeforeReachingDriver) {
  TestJniEnvironment jni;
  try {
    wrapJavaException([&]() { throw JavaException(jni.env(), &jni.original); });
    FAIL() << "Expected VeloxException";
  } catch (const VeloxException& error) {
    EXPECT_EQ(error.exceptionType(), VeloxException::Type::kSystem);
    ASSERT_NE(error.wrappedException(), nullptr);
    auto original = findJavaException(std::current_exception());
    ASSERT_TRUE(original.has_value());
    original->throwToJava(jni.env());
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST_F(VeloxJavaExceptionTest, nestedNativeWrappersRetainOriginal) {
  TestJniEnvironment jni;
  {
    auto error = std::make_exception_ptr(JavaException(jni.env(), &jni.original));
    error = std::make_exception_ptr(VeloxUserError(error, "inner Velox wrapper", false));
    try {
      std::rethrow_exception(error);
    } catch (...) {
      try {
        std::throw_with_nested(std::runtime_error("nested native callback"));
      } catch (...) {
        error = std::current_exception();
      }
    }
    error = std::make_exception_ptr(VeloxRuntimeError(error, "outer Velox wrapper", false));
    auto original = findJavaException(error);
    ASSERT_TRUE(original.has_value());
    error = nullptr;
    original->throwToJava(jni.env());
    EXPECT_EQ(jni.deletedGlobalRefs, 0);
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST_F(VeloxJavaExceptionTest, nativeExceptionsAreNotChanged) {
  const auto original = std::make_exception_ptr(std::runtime_error("native error"));
  try {
    wrapJavaException([&]() { std::rethrow_exception(original); });
    FAIL() << "Expected native exception";
  } catch (const std::runtime_error&) {
    EXPECT_EQ(std::current_exception(), original);
    EXPECT_FALSE(findJavaException(std::current_exception()).has_value());
  }
  const auto wrapped = std::make_exception_ptr(VeloxUserError(original, "native wrapper", false));
  EXPECT_FALSE(findJavaException(wrapped).has_value());
  EXPECT_FALSE(findJavaException(std::make_exception_ptr(42)).has_value());
  EXPECT_FALSE(findJavaException(nullptr).has_value());
}

TEST_F(VeloxJavaExceptionTest, javaCauseInStandardNestedVeloxException) {
  TestJniEnvironment jni;
  try {
    throw JavaException(jni.env(), &jni.original);
  } catch (...) {
    try {
      const auto native = std::make_exception_ptr(std::runtime_error("unrelated native cause"));
      std::throw_with_nested(VeloxUserError(native, "two native wrapper mechanisms", false));
    } catch (...) {
      auto original = findJavaException(std::current_exception());
      ASSERT_TRUE(original.has_value());
      original->throwToJava(jni.env());
    }
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST_F(VeloxJavaExceptionTest, sharedHandlerPreservesPendingAndFallsBackOnFailedRestore) {
  struct CountingRuntime : Runtime {
    CountingRuntime() : Runtime("test", nullptr, nullptr, {}) {}
    std::optional<JavaException> findJniOutputException(std::exception_ptr error) const noexcept override {
      ++lookups;
      return findJavaException(std::move(error));
    }
    mutable int lookups{0};
  } runtime;
  auto& jni = boundaryJni;
  const JavaException original(jni.env(), &jni.original);
  auto invoke = [&](JNIEnv* env) {
    JNI_METHOD_START
    throw VeloxRuntimeError(std::make_exception_ptr(original), "outer wrapper", false);
    JNI_METHOD_END_WITH_RUNTIME(false, &runtime)
  };

  jni.pending = &jni.secondary;
  EXPECT_FALSE(invoke(jni.env()));
  EXPECT_EQ(jni.pending, &jni.secondary);
  EXPECT_EQ(runtime.lookups, 0);

  jni.pending = nullptr;
  jni.failThrow = true;
  EXPECT_FALSE(invoke(jni.env()));
  EXPECT_EQ(runtime.lookups, 1);
  EXPECT_EQ(jni.pending, &jni.fallback);
  EXPECT_EQ(jni.fallbackMessage, original.what());
  jni.failThrow = false;
  jni.pending = nullptr;
}

TEST_F(VeloxJavaExceptionTest, lazyOutputRestoresOriginalOnlyAtJniBoundary) {
  auto* backend = VeloxBackend::get();
  auto* memoryManager = backend->getGlobalMemoryManager();
  auto* pool = memoryManager->getLeafMemoryPool().get();
  std::unique_ptr<ThreadManager, decltype(&ThreadManager::release)> threads(
      ThreadManager::create(kVeloxBackendKind, ThreadInitializer::noop()), &ThreadManager::release);
  VeloxRuntime runtime(kVeloxBackendKind, memoryManager, threads.get(), {{kSessionTimezone, "UTC"}});
  auto spillDir = exec::test::TempDirectoryPath::create();

  enum class Read { kNative, kJniHasNext, kJniNext, kJniNativeError };
  for (const auto read : {Read::kNative, Read::kJniHasNext, Read::kJniNext, Read::kJniNativeError}) {
    SCOPED_TRACE(static_cast<int>(read));
    auto& jni = boundaryJni;
    jni.pending = nullptr;
    const auto refsBefore = jni.createdGlobalRefs - jni.deletedGlobalRefs;
    const VeloxRuntimeError nativeError(
        std::make_exception_ptr(std::runtime_error("native cause")), "native lazy load", false);
    bool loaded = false;
    bool loadedInDriver = false;
    auto lazy = std::make_shared<LazyVector>(
        pool, INTEGER(), 1, std::make_unique<test::SimpleVectorLoader>([&](RowSet) -> VectorPtr {
          loaded = true;
          loadedInDriver = exec::driverThreadContext() != nullptr;
          if (read == Read::kJniNativeError) {
            throw nativeError;
          }
          // This wrapper has no std::nested_exception link; only the Velox cause points to Java.
          throw VeloxRuntimeError(
              std::make_exception_ptr(JavaException(jni.env(), &jni.original)), "lazy Java callback", false);
        }));
    auto batch =
        std::make_shared<RowVector>(pool, ROW({"value"}, {INTEGER()}), nullptr, 1, std::vector<VectorPtr>{lazy});
    auto plan = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{batch});
    {
      ResultIterator iterator(
          std::make_unique<WholeStageResultIterator>(
              memoryManager,
              plan,
              std::vector<core::PlanNodeId>{},
              std::vector<std::shared_ptr<SplitInfo>>{},
              std::vector<core::PlanNodeId>{},
              runtime.executor(),
              runtime.spillExecutor(),
              runtime.connectorIds(),
              spillDir->getPath(),
              runtime.veloxCfg(),
              SparkTaskInfo{}),
          &runtime);
      if (read == Read::kNative) {
        EXPECT_THROW(iterator.next(), VeloxException);
        EXPECT_EQ(jni.pending, nullptr);
      } else {
        auto readAtBoundary = [&](JNIEnv* env) {
          JNI_METHOD_START
          return read == Read::kJniHasNext ? iterator.hasNext() : iterator.next() != nullptr;
          JNI_METHOD_END_WITH_RUNTIME(false, iterator.runtime())
        };
        EXPECT_FALSE(readAtBoundary(jni.env()));
        if (read == Read::kJniNativeError) {
          EXPECT_EQ(jni.pending, &jni.fallback);
          EXPECT_EQ(jni.fallbackMessage, nativeError.what());
        } else {
          EXPECT_EQ(jni.pending, &jni.original);
        }
      }
    }
    EXPECT_TRUE(loaded);
    EXPECT_FALSE(loadedInDriver);
    EXPECT_EQ(jni.createdGlobalRefs - jni.deletedGlobalRefs, refsBefore);
    jni.pending = nullptr;
  }
}

} // namespace
} // namespace gluten

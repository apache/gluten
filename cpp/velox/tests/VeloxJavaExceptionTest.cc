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

#include "compute/ResultIterator.h"
#include "compute/VeloxBackend.h"
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

bool returnToJava(JNIEnv* env, const std::exception_ptr& error) {
  JNI_METHOD_START
  std::rethrow_exception(error);
  JNI_METHOD_END(false)
}

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
    EXPECT_FALSE(returnToJava(jni.env(), std::current_exception()));
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST_F(VeloxJavaExceptionTest, nestedNativeWrappersRetainOriginal) {
  struct NativeContext {};
  TestJniEnvironment jni;
  {
    auto error = std::make_exception_ptr(JavaException(jni.env(), &jni.original));
    error = std::make_exception_ptr(VeloxUserError(error, "inner Velox wrapper", false));
    try {
      std::rethrow_exception(error);
    } catch (...) {
      try {
        // std::throw_with_nested can wrap a type that isn't a std::exception.
        std::throw_with_nested(NativeContext{});
      } catch (...) {
        error = std::current_exception();
      }
    }
    error = std::make_exception_ptr(VeloxRuntimeError(error, "outer Velox wrapper", false));
    EXPECT_FALSE(returnToJava(jni.env(), error));
    EXPECT_EQ(jni.deletedGlobalRefs, 0);
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST_F(VeloxJavaExceptionTest, nativeExceptionsAreNotChanged) {
  auto& jni = boundaryJni;
  const auto original = std::make_exception_ptr(std::runtime_error("native error"));
  try {
    wrapJavaException([&]() { std::rethrow_exception(original); });
    FAIL() << "Expected native exception";
  } catch (const std::runtime_error&) {
    EXPECT_EQ(std::current_exception(), original);
    EXPECT_FALSE(returnToJava(jni.env(), std::current_exception()));
    EXPECT_EQ(jni.pending, &jni.fallback);
    EXPECT_EQ(jni.fallbackMessage, "native error");
    jni.pending = nullptr;
  }
  for (const auto& cause : {original, std::make_exception_ptr(42), std::exception_ptr{}}) {
    const VeloxUserError wrapped(cause, "native wrapper", false);
    EXPECT_FALSE(returnToJava(jni.env(), std::make_exception_ptr(wrapped)));
    EXPECT_EQ(jni.pending, &jni.fallback);
    EXPECT_EQ(jni.fallbackMessage, wrapped.what());
    jni.pending = nullptr;
  }
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
      EXPECT_FALSE(returnToJava(jni.env(), std::current_exception()));
    }
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST_F(VeloxJavaExceptionTest, sharedHandlerPreservesPendingAndFallsBackOnFailedRestore) {
  auto& jni = boundaryJni;
  const JavaException original(jni.env(), &jni.original);
  const auto error =
      std::make_exception_ptr(VeloxRuntimeError(std::make_exception_ptr(original), "outer wrapper", false));

  jni.pending = &jni.secondary;
  EXPECT_FALSE(returnToJava(jni.env(), error));
  EXPECT_EQ(jni.pending, &jni.secondary);

  jni.pending = nullptr;
  jni.failThrow = true;
  EXPECT_FALSE(returnToJava(jni.env(), error));
  EXPECT_EQ(jni.pending, &jni.fallback);
  EXPECT_EQ(jni.fallbackMessage, original.what());
  jni.pending = nullptr;
  try {
    throw original;
  } catch (...) {
    try {
      std::throw_with_nested(std::runtime_error("standard wrapper"));
    } catch (...) {
      EXPECT_FALSE(returnToJava(jni.env(), std::current_exception()));
      EXPECT_EQ(jni.pending, &jni.fallback);
      EXPECT_EQ(jni.fallbackMessage, "standard wrapper");
    }
  }
  jni.failThrow = false;
  jni.pending = nullptr;
}

TEST_F(VeloxJavaExceptionTest, registrationIsScopedAndComposesWithOtherWrappers) {
  struct BackendError : std::runtime_error {
    explicit BackendError(std::exception_ptr cause) : std::runtime_error("other backend"), cause(std::move(cause)) {}
    std::exception_ptr cause;
  };
  auto& jni = boundaryJni;
  auto error = std::make_exception_ptr(BackendError(std::make_exception_ptr(VeloxRuntimeError(
      std::make_exception_ptr(JavaException(jni.env(), &jni.original)), "inner Velox wrapper", false))));
  try {
    std::rethrow_exception(error);
  } catch (...) {
    try {
      std::throw_with_nested(std::runtime_error("outer standard wrapper"));
    } catch (...) {
      error = std::current_exception();
    }
  }
  {
    static int matches;
    matches = 0;
    const JniExceptionUnwrapperRegistration registration([](const std::exception& e) noexcept -> std::exception_ptr {
      if (auto* other = dynamic_cast<const BackendError*>(&e)) {
        ++matches;
        return other->cause;
      }
      return nullptr;
    });
    jni.pending = &jni.secondary;
    EXPECT_FALSE(returnToJava(jni.env(), error));
    EXPECT_EQ(jni.pending, &jni.secondary);
    EXPECT_EQ(matches, 0);
    jni.pending = nullptr;
    EXPECT_FALSE(returnToJava(jni.env(), error));
    EXPECT_EQ(jni.pending, &jni.original);
    EXPECT_EQ(matches, 1);
  }
  jni.pending = nullptr;
  EXPECT_FALSE(returnToJava(jni.env(), error));
  EXPECT_EQ(jni.pending, &jni.fallback);
  EXPECT_EQ(jni.fallbackMessage, "outer standard wrapper");
  jni.pending = nullptr;
}

TEST_F(VeloxJavaExceptionTest, lazyOutputRestoresOriginalOnlyAtJniBoundary) {
  auto* backend = VeloxBackend::get();
  auto* memoryManager = backend->getGlobalMemoryManager();
  auto* pool = memoryManager->getLeafMemoryPool().get();
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
      ResultIterator iterator(std::make_unique<WholeStageResultIterator>(
          memoryManager,
          plan,
          std::vector<core::PlanNodeId>{},
          std::vector<std::shared_ptr<SplitInfo>>{},
          std::vector<core::PlanNodeId>{},
          backend->executor(),
          backend->spillExecutor(),
          VeloxConnectorIds{},
          spillDir->getPath(),
          backend->getBackendConf(),
          SparkTaskInfo{}));
      if (read == Read::kNative) {
        EXPECT_THROW(iterator.next(), VeloxException);
        EXPECT_EQ(jni.pending, nullptr);
      } else {
        auto readAtBoundary = [&](JNIEnv* env) {
          JNI_METHOD_START
          return read == Read::kJniHasNext ? iterator.hasNext() : iterator.next() != nullptr;
          JNI_METHOD_END(false)
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

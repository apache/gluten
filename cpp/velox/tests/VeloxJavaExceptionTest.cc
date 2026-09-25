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
#include "compute/WholeStageResultIterator.h"
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
  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {});
  }

  static void TearDownTestSuite() {
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
    try {
      rethrowJavaException(std::current_exception());
      FAIL() << "Expected JavaException";
    } catch (const JavaException& original) {
      original.throwToJava(jni.env());
    }
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
    try {
      rethrowJavaException(error);
      FAIL() << "Expected JavaException";
    } catch (const JavaException& original) {
      original.throwToJava(jni.env());
    }
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
    EXPECT_NO_THROW(rethrowJavaException(std::current_exception()));
  }
  const auto wrapped = std::make_exception_ptr(VeloxUserError(original, "native wrapper", false));
  EXPECT_NO_THROW(rethrowJavaException(wrapped));
  EXPECT_NO_THROW(rethrowJavaException(std::make_exception_ptr(42)));
  EXPECT_NO_THROW(rethrowJavaException(nullptr));
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
      try {
        rethrowJavaException(std::current_exception());
        FAIL() << "Expected JavaException";
      } catch (const JavaException& original) {
        original.throwToJava(jni.env());
      }
    }
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST_F(VeloxJavaExceptionTest, lazyOutputRestoresOriginalOutsideDriver) {
  TestJniEnvironment jni;
  auto* backend = VeloxBackend::get();
  auto* memoryManager = backend->getGlobalMemoryManager();
  auto* pool = memoryManager->getLeafMemoryPool().get();
  bool loaded = false;
  bool loadedInDriver = false;
  {
    auto lazy = std::make_shared<LazyVector>(
        pool, INTEGER(), 1, std::make_unique<test::SimpleVectorLoader>([&](RowSet) -> VectorPtr {
          loaded = true;
          loadedInDriver = exec::driverThreadContext() != nullptr;
          return wrapJavaException([&]() -> VectorPtr { throw JavaException(jni.env(), &jni.original); });
        }));
    auto batch =
        std::make_shared<RowVector>(pool, ROW({"value"}, {INTEGER()}), nullptr, 1, std::vector<VectorPtr>{lazy});
    auto plan = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{batch});
    auto spillDir = exec::test::TempDirectoryPath::create();
    WholeStageResultIterator iterator(
        memoryManager,
        plan,
        {},
        {},
        {},
        backend->executor(),
        backend->spillExecutor(),
        {},
        spillDir->getPath(),
        backend->getBackendConf(),
        {});
    try {
      iterator.next();
      FAIL() << "Expected JavaException";
    } catch (const JavaException& error) {
      error.throwToJava(jni.env());
    }
  }
  EXPECT_TRUE(loaded);
  EXPECT_FALSE(loadedInDriver);
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.createdGlobalRefs, jni.deletedGlobalRefs);
}

} // namespace
} // namespace gluten

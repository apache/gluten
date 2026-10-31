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

#include <functional>

#include "compute/VeloxBackend.h"
#include "compute/WholeStageResultIterator.h"
#include "config/GlutenConfig.h"
#include "jni/VeloxJavaException.h"
#include "tests/utils/TestJniEnvironment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace gluten {
namespace {

using namespace facebook::velox;

class CallbackAllocationListener final : public AllocationListener {
 public:
  explicit CallbackAllocationListener(std::function<void(int64_t)> callback) : callback_(std::move(callback)) {}

  void allocationChanged(int64_t diff) override {
    callback_(diff);
    currentBytes_ += diff;
    peakBytes_ = std::max(currentBytes_, peakBytes_);
  }

  int64_t currentBytes() override {
    return currentBytes_;
  }

  int64_t peakBytes() override {
    return peakBytes_;
  }

 private:
  std::function<void(int64_t)> callback_;
  int64_t currentBytes_{0};
  int64_t peakBytes_{0};
};

class VeloxJavaExceptionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {{kSessionTimezone, "UTC"}});
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

TEST_F(VeloxJavaExceptionTest, reservationsAreWrappedForBothNativeAllocators) {
  for (const bool useArrow : {false, true}) {
    SCOPED_TRACE(useArrow);
    TestJniEnvironment jni;
    auto listener = std::make_unique<CallbackAllocationListener>([&](int64_t diff) {
      if (diff > 0) {
        throw JavaException(jni.env(), &jni.original);
      }
    });
    VeloxMemoryManager memoryManager(kVeloxBackendKind, std::move(listener), *VeloxBackend::get()->getBackendConf());
    try {
      if (useArrow) {
        void* buffer = nullptr;
        if (memoryManager.allocator()->allocate(64, &buffer)) {
          memoryManager.allocator()->free(buffer, 64);
        }
      } else {
        auto* pool = memoryManager.getLeafMemoryPool().get();
        auto* buffer = pool->allocate(64);
        pool->free(buffer, 64);
      }
      FAIL() << "Expected wrapped JavaException";
    } catch (const VeloxException& error) {
      EXPECT_EQ(error.exceptionType(), VeloxException::Type::kSystem);
      EXPECT_TRUE(JavaException::throwNestedToJava(jni.env(), error));
    }
    EXPECT_EQ(jni.pending, &jni.original);
    EXPECT_EQ(jni.createdGlobalRefs, 1);
    EXPECT_EQ(jni.deletedGlobalRefs, 1);
    EXPECT_EQ(memoryManager.getListener()->currentBytes(), 0);
  }
}

TEST_F(VeloxJavaExceptionTest, unreservePreservesJavaExceptionAndDelegatedMetrics) {
  TestJniEnvironment jni;
  bool fail = false;
  auto listener = std::make_unique<CallbackAllocationListener>([&](int64_t diff) {
    if (fail && diff < 0) {
      throw JavaException(jni.env(), &jni.original);
    }
  });
  VeloxMemoryManager memoryManager(kVeloxBackendKind, std::move(listener), *VeloxBackend::get()->getBackendConf());
  auto* wrapped = memoryManager.getListener();
  wrapped->allocationChanged(64);
  EXPECT_EQ(wrapped->currentBytes(), 64);
  EXPECT_EQ(wrapped->peakBytes(), 64);
  fail = true;
  try {
    wrapped->allocationChanged(-64);
    FAIL() << "Expected wrapped JavaException";
  } catch (const VeloxException& error) {
    EXPECT_TRUE(JavaException::throwNestedToJava(jni.env(), error));
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
  fail = false;
  wrapped->allocationChanged(-64);
  EXPECT_EQ(wrapped->currentBytes(), 0);
  EXPECT_EQ(wrapped->peakBytes(), 64);
}

TEST_F(VeloxJavaExceptionTest, nativeReservationErrorsAreUnchanged) {
  const auto original = std::make_exception_ptr(std::runtime_error("native reservation failure"));
  auto listener = std::make_unique<CallbackAllocationListener>([&](int64_t diff) {
    if (diff > 0) {
      std::rethrow_exception(original);
    }
  });
  VeloxMemoryManager memoryManager(kVeloxBackendKind, std::move(listener), *VeloxBackend::get()->getBackendConf());
  try {
    auto* pool = memoryManager.getLeafMemoryPool().get();
    auto* buffer = pool->allocate(64);
    pool->free(buffer, 64);
    FAIL() << "Expected native reservation failure";
  } catch (const std::runtime_error&) {
    EXPECT_EQ(std::current_exception(), original);
  }
}

} // namespace
} // namespace gluten

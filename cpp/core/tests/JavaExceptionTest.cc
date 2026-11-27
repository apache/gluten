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

#include <new>
#include <optional>
#include <stdexcept>
#include <string>

#include "jni/JavaException.h"
#include "jni/JniError.h"
#include "tests/utils/TestJniEnvironment.h"

using namespace gluten;

TEST(JavaExceptionTest, noPendingException) {
  TestJniEnvironment jni;
  EXPECT_NO_THROW(JavaException::check(jni.env()));
  EXPECT_EQ(jni.createdGlobalRefs, 0);
}

TEST(JavaExceptionTest, copiesKeepOriginalAliveUntilLastRelease) {
  TestJniEnvironment jni;
  std::exception_ptr saved;
  jni.pending = &jni.original;
  try {
    JavaException::check(jni.env());
    FAIL() << "Expected JavaException";
  } catch (const JavaException& error) {
    EXPECT_EQ(jni.pending, nullptr);
    EXPECT_EQ(jni.deletedLocalRefs, 1);
    EXPECT_STREQ(error.what(), "Error during calling Java code from native code: original Java exception");
    auto copy = error;
    saved = std::make_exception_ptr(copy);
  }
  EXPECT_EQ(jni.createdGlobalRefs, 1);
  EXPECT_EQ(jni.deletedGlobalRefs, 0);
  try {
    std::rethrow_exception(saved);
  } catch (const JavaException& error) {
    error.throwToJava(jni.env());
  }
  EXPECT_EQ(jni.pending, &jni.original);
  saved = nullptr;
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
  EXPECT_TRUE(jni.globalRefs.empty());
  EXPECT_EQ(jni.pushedFrames, jni.poppedFrames);
  EXPECT_EQ(jni.releasedStrings, 1);
  EXPECT_EQ(jni.attaches, 0);
  EXPECT_EQ(jni.detaches, 0);
}

TEST(JavaExceptionTest, cleanupOnDetachedThread) {
  TestJniEnvironment jni;
  auto saved = std::make_exception_ptr(JavaException(jni.env(), &jni.original));
  std::thread worker([error = std::move(saved)]() mutable { error = nullptr; });
  worker.join();
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
  EXPECT_TRUE(jni.globalRefs.empty());
  EXPECT_EQ(jni.deletingEnv, jni.workerEnv());
  EXPECT_EQ(jni.attaches, 1);
  EXPECT_EQ(jni.detaches, 1);
}

TEST(JavaExceptionTest, cleanupDoesNotDetachPreviouslyAttachedThread) {
  TestJniEnvironment jni;
  jni.workerAttached = true;
  auto saved = std::make_exception_ptr(JavaException(jni.env(), &jni.original));
  std::thread worker([error = std::move(saved)]() mutable { error = nullptr; });
  worker.join();
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
  EXPECT_EQ(jni.deletingEnv, jni.workerEnv());
  EXPECT_EQ(jni.attaches, 0);
  EXPECT_EQ(jni.detaches, 0);
}

TEST(JavaExceptionTest, descriptionFailureDoesNotReplaceOriginal) {
  TestJniEnvironment jni;
  jni.failDescription = true;
  jni.pending = &jni.original;
  try {
    JavaException::check(jni.env());
    FAIL() << "Expected JavaException";
  } catch (const JavaException& error) {
    EXPECT_EQ(jni.pending, nullptr);
    EXPECT_NE(std::string(error.what()).find("JniExceptionDescriber lookup failed"), std::string::npos);
    error.throwToJava(jni.env());
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
  EXPECT_EQ(jni.deletedLocalRefs, 1);
  EXPECT_EQ(jni.pushedFrames, jni.poppedFrames);
}

TEST(JavaExceptionTest, localFrameFailureDoesNotReplaceOriginal) {
  TestJniEnvironment jni;
  jni.failLocalFrame = true;
  {
    JavaException error(jni.env(), &jni.original);
    EXPECT_EQ(jni.pending, nullptr);
    error.throwToJava(jni.env());
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
  EXPECT_EQ(jni.poppedFrames, 0);
}

TEST(JavaExceptionTest, globalRefFailureRetainsPendingAllocationError) {
  TestJniEnvironment jni;
  const JniEntryGuard entry;
  jni.failGlobalRef = true;
  jni.pending = &jni.original;
  EXPECT_THROW(JavaException::check(jni.env()), std::runtime_error);
  EXPECT_EQ(jni.pending, &jni.secondary);
  EXPECT_EQ(jni.deletedLocalRefs, 1);
  EXPECT_EQ(jni.createdGlobalRefs, 0);
  EXPECT_EQ(jni.deletedGlobalRefs, 0);
}

TEST(JavaExceptionTest, workerGlobalRefFailureLeavesNoPendingException) {
  TestJniEnvironment jni;
  const JniEntryGuard callerEntry;
  jni.workerAttached = true;
  std::thread worker([&] {
    EXPECT_FALSE(JniEntryGuard::isActive());
    jni.failGlobalRef = true;
    jni.pending = &jni.original;
    EXPECT_THROW(JavaException::check(jni.workerEnv()), std::runtime_error);
    EXPECT_EQ(jni.pending, nullptr);
    jni.failGlobalRef = false;
    EXPECT_NO_THROW(JavaException::check(jni.workerEnv()));
  });
  worker.join();
  EXPECT_EQ(jni.deletedLocalRefs, 1);
  EXPECT_EQ(jni.createdGlobalRefs, 0);
  EXPECT_EQ(jni.deletedGlobalRefs, 0);
  EXPECT_EQ(jni.attaches, 0);
  EXPECT_EQ(jni.detaches, 0);
}

TEST(JavaExceptionTest, standardNestedWrappersRestoreOriginal) {
  TestJniEnvironment jni;
  try {
    throw JavaException(jni.env(), &jni.original);
  } catch (...) {
    try {
      std::throw_with_nested(std::runtime_error("inner native wrapper"));
    } catch (...) {
      try {
        std::throw_with_nested(std::runtime_error("outer native wrapper"));
      } catch (const std::exception& error) {
        EXPECT_TRUE(JavaException::throwNestedToJava(jni.env(), error));
      }
    }
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.createdGlobalRefs, 1);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
}

TEST(JavaExceptionTest, nativeOnlyWrappersRetainExistingFallback) {
  TestJniEnvironment jni;
  EXPECT_FALSE(JavaException::throwNestedToJava(jni.env(), std::runtime_error("native error")));
  try {
    throw std::runtime_error("native cause");
  } catch (...) {
    try {
      std::throw_with_nested(std::runtime_error("native wrapper"));
    } catch (const std::exception& error) {
      EXPECT_FALSE(JavaException::throwNestedToJava(jni.env(), error));
    }
  }
  try {
    throw 42;
  } catch (...) {
    try {
      std::throw_with_nested(std::runtime_error("nonstandard native cause"));
    } catch (const std::exception& error) {
      EXPECT_FALSE(JavaException::throwNestedToJava(jni.env(), error));
    }
  }
  EXPECT_EQ(jni.pending, nullptr);
  EXPECT_EQ(jni.createdGlobalRefs, 0);
}

class JniExceptionMetadataTest : public ::testing::Test {
 protected:
  static inline TestJniEnvironment boundaryJni;
  static inline int descriptions = 0;

  static void SetUpTestSuite() {
    getJniErrorState()->ensureInitialized(boundaryJni.env());
  }

  static void TearDownTestSuite() {
    getJniErrorState()->close();
    EXPECT_EQ(boundaryJni.createdGlobalRefs, boundaryJni.deletedGlobalRefs);
  }

  void SetUp() override {
    descriptions = 0;
  }

  // Describes every error, but fails as if it could not allocate the metadata.
  static std::optional<JniExceptionMetadata> failToDescribe(const std::exception&) {
    ++descriptions;
    throw std::bad_alloc();
  }

  enum class ProviderFailure { kNonStandard };

  // Fails with an exception that isn't a std::exception.
  static std::optional<JniExceptionMetadata> failWithNonStandardException(const std::exception&) {
    ++descriptions;
    throw ProviderFailure::kNonStandard;
  }

  // Breaks the no-JNI contract: a Java exception becomes pending before the provider fails.
  static std::optional<JniExceptionMetadata> failWithPendingThrowable(const std::exception&) {
    ++descriptions;
    boundaryJni.pending = &boundaryJni.secondary;
    throw ProviderFailure::kNonStandard;
  }
};

TEST_F(JniExceptionMetadataTest, metadataFailureKeepsThePendingThrowableOrOriginalDiagnostic) {
  auto& jni = boundaryJni;
  const JniExceptionMetadataRegistration registration(failToDescribe);
  const std::runtime_error error("original native diagnostic");

  jni.pending = &jni.secondary;
  handleJniException(jni.env(), error);
  EXPECT_EQ(jni.pending, &jni.secondary);
  EXPECT_EQ(descriptions, 0);

  jni.pending = nullptr;
  handleJniException(jni.env(), error);
  EXPECT_EQ(jni.pending, &jni.fallback);
  EXPECT_EQ(jni.fallbackMessage, error.what());
  EXPECT_EQ(descriptions, 1);
  jni.pending = nullptr;
}

TEST_F(JniExceptionMetadataTest, nonStandardMetadataFailureKeepsThePendingThrowableOrOriginalDiagnostic) {
  auto& jni = boundaryJni;
  const std::runtime_error error("original native diagnostic");
  {
    const JniExceptionMetadataRegistration registration(failWithNonStandardException);
    EXPECT_NO_THROW(handleJniException(jni.env(), error));
    EXPECT_EQ(jni.pending, &jni.fallback);
    EXPECT_EQ(jni.fallbackMessage, error.what());
    EXPECT_EQ(descriptions, 1);
    jni.pending = nullptr;
  }
  const JniExceptionMetadataRegistration registration(failWithPendingThrowable);
  EXPECT_NO_THROW(handleJniException(jni.env(), error));
  EXPECT_EQ(jni.pending, &jni.secondary);
  EXPECT_EQ(descriptions, 2);
  jni.pending = nullptr;
}

TEST_F(JniExceptionMetadataTest, carriedJavaThrowableWinsAndRegistrationIsScoped) {
  auto& jni = boundaryJni;
  {
    const JniExceptionMetadataRegistration registration(failToDescribe);
    try {
      throw JavaException(jni.env(), &jni.original);
    } catch (...) {
      try {
        std::throw_with_nested(std::runtime_error("native wrapper"));
      } catch (const std::exception& error) {
        handleJniException(jni.env(), error);
      }
    }
    EXPECT_EQ(jni.pending, &jni.original);
    EXPECT_EQ(descriptions, 0);
    jni.pending = nullptr;
  }
  handleJniException(jni.env(), std::runtime_error("native error"));
  EXPECT_EQ(jni.pending, &jni.fallback);
  EXPECT_EQ(jni.fallbackMessage, "native error");
  EXPECT_EQ(descriptions, 0);
  jni.pending = nullptr;
}

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
#include <utility>

#include "jni/JavaException.h"
#include "tests/utils/TestAllocationFailure.h"
#include "tests/utils/TestJniEnvironment.h"

using gluten::test::failNextAllocation;

namespace {
struct RestoreJniFunctions {
  JNIEnv* env;
  const JNINativeInterface_* original;

  ~RestoreJniFunctions() {
    env->functions = original;
  }
};
} // namespace

TEST(JavaExceptionAllocationTest, stateAllocationFailureRetainsOriginal) {
  gluten::TestJniEnvironment jni;
  const gluten::JniEntryGuard entry;
  jni.pending = &jni.original;
  failNextAllocation = true;
  try {
    gluten::JavaException::check(jni.env());
    failNextAllocation = false;
    FAIL() << "Expected std::bad_alloc";
  } catch (const std::bad_alloc&) {
    EXPECT_FALSE(std::exchange(failNextAllocation, false));
  } catch (...) {
    failNextAllocation = false;
    throw;
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.createdGlobalRefs, 0);
  EXPECT_EQ(jni.deletedGlobalRefs, 0);
  EXPECT_EQ(jni.deletedLocalRefs, 1);
  EXPECT_EQ(jni.pushedFrames, 0);
  EXPECT_EQ(jni.poppedFrames, 0);
  EXPECT_EQ(jni.releasedStrings, 0);
}

TEST(JavaExceptionAllocationTest, workerStateAllocationFailureLeavesNoPendingException) {
  gluten::TestJniEnvironment jni;
  const gluten::JniEntryGuard callerEntry;
  jni.workerAttached = true;
  std::thread worker([&] {
    EXPECT_FALSE(gluten::JniEntryGuard::isActive());
    jni.pending = &jni.original;
    failNextAllocation = true;
    try {
      gluten::JavaException::check(jni.workerEnv());
      failNextAllocation = false;
      FAIL() << "Expected std::bad_alloc";
    } catch (const std::bad_alloc&) {
      EXPECT_FALSE(std::exchange(failNextAllocation, false));
    }
    EXPECT_EQ(jni.pending, nullptr);
    EXPECT_NO_THROW(gluten::JavaException::check(jni.workerEnv()));
  });
  worker.join();
  EXPECT_EQ(jni.deletedLocalRefs, 1);
  EXPECT_EQ(jni.createdGlobalRefs, 0);
  EXPECT_EQ(jni.deletedGlobalRefs, 0);
  EXPECT_EQ(jni.attaches, 0);
  EXPECT_EQ(jni.detaches, 0);
}

TEST(JavaExceptionAllocationTest, entryTrackingDoesNotAllocate) {
  bool nestedActive = false;
  bool outerActive = false;
  failNextAllocation = true;
  {
    const gluten::JniEntryGuard outer;
    try {
      const gluten::JniEntryGuard inner;
      nestedActive = gluten::JniEntryGuard::isActive();
      throw 7;
    } catch (int) {
      outerActive = gluten::JniEntryGuard::isActive();
    }
  }
  const bool noAllocation = std::exchange(failNextAllocation, false);
  EXPECT_TRUE(noAllocation);
  EXPECT_TRUE(nestedActive);
  EXPECT_TRUE(outerActive);
  EXPECT_FALSE(gluten::JniEntryGuard::isActive());
}

TEST(JavaExceptionAllocationTest, diagnosticAllocationFailureRetainsOriginal) {
  gluten::TestJniEnvironment jni;
  auto functions = *jni.env()->functions;
  functions.GetStringUTFChars = [](JNIEnv*, jstring, jboolean*) -> const char* {
    // The throwable is already captured; fail the native diagnostic string, not NewGlobalRef.
    failNextAllocation = true;
    return "original Java exception";
  };
  const RestoreJniFunctions restore{jni.env(), jni.env()->functions};
  jni.env()->functions = &functions;
  jni.pending = &jni.original;
  try {
    gluten::JavaException::check(jni.env());
    failNextAllocation = false;
    FAIL() << "Expected JavaException";
  } catch (const gluten::JavaException& error) {
    EXPECT_FALSE(std::exchange(failNextAllocation, false));
    EXPECT_STREQ(
        error.what(),
        "Error during calling Java code from native code: Java stack trace unavailable (native diagnostic failure)");
    EXPECT_EQ(jni.deletedGlobalRefs, 0);
    auto copy = error;
    copy.throwToJava(jni.env());
  }
  EXPECT_EQ(jni.pending, &jni.original);
  EXPECT_EQ(jni.createdGlobalRefs, 1);
  EXPECT_EQ(jni.deletedGlobalRefs, 1);
  EXPECT_EQ(jni.deletedLocalRefs, 1);
  EXPECT_EQ(jni.releasedStrings, 1);
  EXPECT_EQ(jni.pushedFrames, jni.poppedFrames);
}

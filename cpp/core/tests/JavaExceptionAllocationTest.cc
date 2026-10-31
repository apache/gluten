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

#include <cstdlib>
#include <new>
#include <utility>

#include "jni/JavaException.h"
#include "tests/utils/TestJniEnvironment.h"

namespace {
thread_local bool failNextAllocation = false;
}

// This executable isolates the replacement allocator from other core tests.
void* operator new(std::size_t size) {
  if (std::exchange(failNextAllocation, false)) {
    throw std::bad_alloc();
  }
  if (auto* memory = std::malloc(size == 0 ? 1 : size)) {
    return memory;
  }
  throw std::bad_alloc();
}

void operator delete(void* memory) noexcept {
  std::free(memory);
}

void operator delete(void* memory, std::size_t) noexcept {
  std::free(memory);
}

TEST(JavaExceptionAllocationTest, diagnosticAllocationFailureRetainsOriginal) {
  gluten::TestJniEnvironment jni;
  auto functions = *jni.env()->functions;
  functions.GetStringUTFChars = [](JNIEnv*, jstring, jboolean*) -> const char* {
    // The throwable is already captured; fail the native diagnostic string, not NewGlobalRef.
    failNextAllocation = true;
    return "original Java exception";
  };
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

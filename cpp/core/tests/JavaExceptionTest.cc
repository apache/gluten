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

#include <stdexcept>
#include <string>

#include "jni/JavaException.h"
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
  jni.failGlobalRef = true;
  jni.pending = &jni.original;
  EXPECT_THROW(JavaException::check(jni.env()), std::runtime_error);
  EXPECT_EQ(jni.pending, &jni.secondary);
  EXPECT_EQ(jni.deletedLocalRefs, 1);
  EXPECT_EQ(jni.createdGlobalRefs, 0);
  EXPECT_EQ(jni.deletedGlobalRefs, 0);
}

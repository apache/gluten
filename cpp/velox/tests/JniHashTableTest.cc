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

#include "jni/JniHashTable.h"

#include <gtest/gtest.h>

namespace gluten {

// This test binary, like the micro benchmark, runs without a JVM. JNI_OnLoad is therefore never
// invoked and JniHashTableContext::vm_ stays null.
//
// Looking up the build side of a broadcast hash join used to dereference that null JavaVM
// unconditionally, so converting any substrait plan carrying a non-empty `hashTableId` crashed the
// process. See https://github.com/apache/gluten/issues/12504.
class JniHashTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(JniHashTableContext::getInstance().getJavaVM(), nullptr)
        << "This test asserts behaviour of a process with no JVM attached";
  }
};

TEST_F(JniHashTableTest, getJoinReportsMissWithoutJvm) {
  // Must report a miss rather than segfault. SubstraitToVeloxPlanConverter treats 0 as "no
  // pre-built table" and falls back to building one from the build side input.
  EXPECT_EQ(getJoin("no-such-broadcast-hash-table-id"), 0);
}

TEST_F(JniHashTableTest, callJavaGetReportsUnreachableCacheWithoutJvm) {
  // Keeps "the cache says there is no such table" distinguishable from "the cache cannot be
  // reached at all", rather than collapsing both into a handle of 0 at this level.
  EXPECT_FALSE(JniHashTableContext::getInstance().callJavaGet("any-id").has_value());
}

} // namespace gluten

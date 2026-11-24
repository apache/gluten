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

#include <string>
#include <vector>

#include "jni/JniCommon.h"
#include "jni/JniError.h"
#include "tests/utils/TestJniEnvironment.h"

namespace gluten {
namespace {

// FindClass takes a binary name such as "java/lang/String". HotSpot also accepts the field
// descriptor "Ljava/lang/String;", but warns under -Xcheck:jni that it will stop doing so.
std::vector<std::string> descriptorNames(const std::vector<std::string>& names) {
  std::vector<std::string> descriptors;
  for (const auto& name : names) {
    if (name.size() >= 2 && name.front() == 'L' && name.back() == ';') {
      descriptors.push_back(name);
    }
  }
  return descriptors;
}

} // namespace

TEST(JniClassNameTest, commonStateUsesBinaryClassNames) {
  TestJniEnvironment jni;
  JniCommonState state;
  state.ensureInitialized(jni.env());
  EXPECT_FALSE(jni.foundClassNames.empty());
  EXPECT_EQ(descriptorNames(jni.foundClassNames), std::vector<std::string>{});
}

TEST(JniClassNameTest, errorStateUsesBinaryClassNames) {
  TestJniEnvironment jni;
  JniErrorState state;
  state.ensureInitialized(jni.env());
  EXPECT_FALSE(jni.foundClassNames.empty());
  EXPECT_EQ(descriptorNames(jni.foundClassNames), std::vector<std::string>{});
}

} // namespace gluten

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

#include "jni/JniCommon.h"

#include <gtest/gtest.h>

using namespace gluten;

namespace {

class TestColumnarBatchIterator final : public ColumnarBatchIterator {
 public:
  std::shared_ptr<ColumnarBatch> next() override {
    return nullptr;
  }
};

class TestRuntime final : public Runtime {
 public:
  TestRuntime() : Runtime("jni-input-iterator-test", nullptr, nullptr, {}) {}

  std::unique_ptr<ColumnarBatchIterator> createJniInputIterator(const JniInputIteratorContext& context) override {
    env = context.env;
    iterator = context.jColumnarBatchIterator;
    iteratorIndex = context.iteratorIndex;
    return std::make_unique<TestColumnarBatchIterator>();
  }

  JNIEnv* env{nullptr};
  jobject iterator{nullptr};
  int32_t iteratorIndex{-1};
};

TEST(JniInputIterator, DispatchesThroughRuntime) {
  static int envMarker;
  static int iteratorMarker;
  auto* env = reinterpret_cast<JNIEnv*>(&envMarker);
  auto iterator = reinterpret_cast<jobject>(&iteratorMarker);

  TestRuntime runtime;
  Runtime* baseRuntime = &runtime;
  auto result = baseRuntime->createJniInputIterator({env, iterator, 7});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(runtime.env, env);
  EXPECT_EQ(runtime.iterator, iterator);
  EXPECT_EQ(runtime.iteratorIndex, 7);
}

} // namespace

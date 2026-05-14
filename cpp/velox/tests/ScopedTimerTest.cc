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

#include "utils/Common.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "velox/common/time/CpuWallTimer.h"

namespace gluten {

// Regression test: SCOPED_TIMER must accumulate elapsed time across the
// enclosing scope. A previous implementation wrapped the underlying
// DeltaCpuWallTimer in a `do { ... } while (0)` block, which destroyed the
// timer immediately after construction and recorded ~0 ns. See
// https://github.com/apache/gluten/pull/<this-PR>.
TEST(ScopedTimerTest, accumulatesElapsedTime) {
  facebook::velox::CpuWallTiming timing{};
  constexpr auto kSleep = std::chrono::milliseconds(50);

  {
    SCOPED_TIMER(timing);
    std::this_thread::sleep_for(kSleep);
  }

  EXPECT_EQ(timing.count, 1);
  // The sleep is on wall clock, not CPU. Allow a generous lower bound
  // to tolerate scheduler jitter on CI.
  constexpr uint64_t kMinWallNanos = std::chrono::nanoseconds(kSleep).count() / 2;
  EXPECT_GT(timing.wallNanos, kMinWallNanos);
}

// Multiple SCOPED_TIMER calls in the same scope must each accumulate
// independently. The unique-naming pattern in the macro (using __LINE__)
// is what makes this work.
TEST(ScopedTimerTest, multipleTimersInSameScope) {
  facebook::velox::CpuWallTiming timing1{};
  facebook::velox::CpuWallTiming timing2{};
  constexpr auto kSleep = std::chrono::milliseconds(20);

  {
    SCOPED_TIMER(timing1);
    SCOPED_TIMER(timing2);
    std::this_thread::sleep_for(kSleep);
  }

  EXPECT_EQ(timing1.count, 1);
  EXPECT_EQ(timing2.count, 1);
  constexpr uint64_t kMinWallNanos = std::chrono::nanoseconds(kSleep).count() / 2;
  EXPECT_GT(timing1.wallNanos, kMinWallNanos);
  EXPECT_GT(timing2.wallNanos, kMinWallNanos);
}

// SCOPED_TIMER must accept arbitrary lvalue expressions (e.g. an array
// element) as the timing target, not just simple identifiers.
TEST(ScopedTimerTest, acceptsArrayElementAsTarget) {
  std::vector<facebook::velox::CpuWallTiming> timings(2);
  constexpr auto kSleep = std::chrono::milliseconds(20);

  {
    SCOPED_TIMER(timings[1]);
    std::this_thread::sleep_for(kSleep);
  }

  EXPECT_EQ(timings[0].count, 0);
  EXPECT_EQ(timings[1].count, 1);
  constexpr uint64_t kMinWallNanos = std::chrono::nanoseconds(kSleep).count() / 2;
  EXPECT_GT(timings[1].wallNanos, kMinWallNanos);
}

} // namespace gluten

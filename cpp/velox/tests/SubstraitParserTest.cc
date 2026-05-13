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

#include "substrait/SubstraitParser.h"

#include <google/protobuf/wrappers.pb.h>
#include <gtest/gtest.h>

namespace gluten {

namespace {

// Build an AdvancedExtension whose optimization payload is exactly `payload`,
// matching the on-the-wire format used by WindowGroupLimitExecTransformer
// (i.e. a serialized google.protobuf.StringValue).
::substrait::extensions::AdvancedExtension makeOptimizationExtension(const std::string& payload) {
  google::protobuf::StringValue msg;
  msg.set_value(payload);
  ::substrait::extensions::AdvancedExtension ext;
  ext.mutable_optimization()->PackFrom(msg);
  return ext;
}

} // namespace

// Lock in the desired behavior of checkWindowFunction's bounds check.
// Without the fix in this commit, the third case below would attempt to
// substr() past the end of msg.value() — std::string::substr clamps and
// the comparison silently returns false, but std::string_view::substr
// would throw std::out_of_range. Keep the bounds tight either way.
TEST(SubstraitParserTest, checkWindowFunction) {
  // Well-formed payload, target matches.
  EXPECT_TRUE(SubstraitParser::checkWindowFunction(
      makeOptimizationExtension("WindowGroupLimitParameters:window_function=row_number\n"), "row_number"));

  // Well-formed payload, target does not match.
  EXPECT_FALSE(SubstraitParser::checkWindowFunction(
      makeOptimizationExtension("WindowGroupLimitParameters:window_function=row_number\n"), "rank"));

  // Truncated payload: bytes after `window_function=` are shorter than the
  // target. Must return false without overrunning the buffer.
  EXPECT_FALSE(SubstraitParser::checkWindowFunction(
      makeOptimizationExtension("WindowGroupLimitParameters:window_function=r"), "row_number"));

  // Empty payload — no `window_function=` selector at all.
  EXPECT_FALSE(SubstraitParser::checkWindowFunction(makeOptimizationExtension(""), "row_number"));

  // Extension with no optimization at all.
  EXPECT_FALSE(SubstraitParser::checkWindowFunction(::substrait::extensions::AdvancedExtension{}, "row_number"));
}

} // namespace gluten

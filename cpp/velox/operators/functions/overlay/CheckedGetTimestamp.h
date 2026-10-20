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

#pragma once

#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/TimestampUtils.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace gluten {

namespace {
// Mirrors facebook::velox::functions::sparksql::detail::initializeFormatter,
// calling buildSimpleDateTimeFormatter/buildJodaDateTimeFormatter directly
// instead of through detail::getDateTimeFormatter: that function is defined
// (not just declared) in DateTimeFunctions.h without `inline`, and is already
// compiled into libvelox.a via RegisterDatetime.cpp, so including that header
// here causes a multiple-definition link error.
inline std::shared_ptr<facebook::velox::functions::DateTimeFormatter> initializeCheckedFormatter(
    std::string_view format,
    bool legacyFormatter) {
  auto formatter = legacyFormatter ? facebook::velox::functions::buildSimpleDateTimeFormatter(format, /*lenient=*/false)
                                   : facebook::velox::functions::buildJodaDateTimeFormatter(format);
  if (formatter.hasError()) {
    if (legacyFormatter) {
      return nullptr;
    }
    VELOX_USER_FAIL(formatter.error().message());
  }
  return formatter.value();
}
} // namespace

/// checked_get_timestamp(string, format) -> timestamp
///
/// get_timestamp's failOnError = true sibling, picked by
/// VeloxSparkPlanExecApi.genGetTimestampTransformer from the Spark
/// expression's own frozen failOnError field. Identical to
/// GetTimestampFunction except it throws on a parse failure instead of
/// returning NULL; an invalid format pattern is unchanged (never ANSI-gated).
template <typename T>
struct CheckedGetTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<facebook::velox::TypePtr>& /*inputTypes*/,
      const facebook::velox::core::QueryConfig& config,
      const arg_type<facebook::velox::Varchar>* /*input*/,
      const arg_type<facebook::velox::Varchar>* format) {
    legacyFormatter_ = facebook::velox::functions::sparksql::SparkQueryConfig{config}.legacyDateFormatter();
    auto sessionTimezoneName = config.sessionTimezone();
    if (!sessionTimezoneName.empty()) {
      sessionTimeZone_ = facebook::velox::tz::locateZone(sessionTimezoneName);
    }
    if (format != nullptr) {
      auto formatter = initializeCheckedFormatter(std::string_view(*format), legacyFormatter_);
      if (formatter) {
        formatter_ = formatter;
      } else {
        invalidFormat_ = true;
      }
      isConstantTimeFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<facebook::velox::Timestamp>& result,
      const arg_type<facebook::velox::Varchar>& input,
      const arg_type<facebook::velox::Varchar>& format) {
    if (invalidFormat_) {
      // Invalid pattern under the legacy formatter: never ANSI-gated.
      return false;
    }
    if (!isConstantTimeFormat_) {
      auto formatter = initializeCheckedFormatter(std::string_view(format), legacyFormatter_);
      if (formatter) {
        formatter_ = formatter;
      } else {
        return false;
      }
    }
    auto dateTimeResult = formatter_->parse(std::string_view(input));
    if (dateTimeResult.hasError()) {
      // The one site ANSI mode gates; get_timestamp returns NULL here instead.
      VELOX_USER_FAIL("{}", dateTimeResult.error().message());
    }
    facebook::velox::functions::sparksql::toGMTWithGapCorrection(
        (*dateTimeResult).timestamp, *getTimeZone(*dateTimeResult));
    result = (*dateTimeResult).timestamp;
    return true;
  }

 private:
  const facebook::velox::tz::TimeZone* getTimeZone(const facebook::velox::functions::DateTimeResult& result) const {
    // If timezone was not parsed, fallback to the session timezone. If
    // there's no session timezone, fallback to 0 (GMT).
    return result.timezone ? result.timezone : sessionTimeZone_;
  }

  std::shared_ptr<facebook::velox::functions::DateTimeFormatter> formatter_{nullptr};
  bool isConstantTimeFormat_{false};
  const facebook::velox::tz::TimeZone* sessionTimeZone_{facebook::velox::tz::locateZone(0)}; // default to GMT.
  bool legacyFormatter_{false};
  bool invalidFormat_{false};
};

} // namespace gluten

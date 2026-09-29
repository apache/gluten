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
#include "config/GlutenConfig.h"

namespace gluten {

// Helpers to check whether a key's value is redacted or plain in the output.
static bool isRedacted(const std::string& output, const std::string& key) {
  // Look for the pattern " [<key>, *********(redacted)]"
  return output.find("[" + key + ", " + kSparkRedactionString + "]") != std::string::npos;
}

static bool isPlain(const std::string& output, const std::string& key, const std::string& value) {
  return output.find("[" + key + ", " + value + "]") != std::string::npos;
}

// ── Default-redaction tests (no spark.redaction.regex in config) ─────────────

TEST(PrintConfig, DefaultRedactsPassword) {
  std::unordered_map<std::string, std::string> conf = {
      {"spark.datasource.jdbc.password", "secret123"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isRedacted(out, "spark.datasource.jdbc.password"));
}

TEST(PrintConfig, DefaultRedactsSecret) {
  std::unordered_map<std::string, std::string> conf = {
      {"my.secret.value", "topsecret"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isRedacted(out, "my.secret.value"));
}

TEST(PrintConfig, DefaultRedactsToken) {
  std::unordered_map<std::string, std::string> conf = {
      {"spark.hadoop.fs.s3a.access.token", "tok_abc123"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isRedacted(out, "spark.hadoop.fs.s3a.access.token"));
}

TEST(PrintConfig, DefaultRedactsAccessKey) {
  std::unordered_map<std::string, std::string> conf = {
      {"spark.hadoop.fs.s3a.accesskey", "AKIAIOSFODNN7EXAMPLE"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isRedacted(out, "spark.hadoop.fs.s3a.accesskey"));
}

TEST(PrintConfig, DefaultDoesNotRedactSafeKey) {
  std::unordered_map<std::string, std::string> conf = {
      {"spark.sql.session.timeZone", "UTC"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isPlain(out, "spark.sql.session.timeZone", "UTC"));
}

// ── Custom-regex tests (spark.redaction.regex present) ───────────────────────

TEST(PrintConfig, CustomRegexRedactsMatchingKey) {
  std::unordered_map<std::string, std::string> conf = {
      {kSparkRedactionRegex, "supersensitive"},
      {"my.supersensitive.config", "very_private"},
      {"spark.sql.session.timeZone", "UTC"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isRedacted(out, "my.supersensitive.config"));
  EXPECT_TRUE(isPlain(out, "spark.sql.session.timeZone", "UTC"));
}

TEST(PrintConfig, CustomRegexOverridesDefault) {
  // When spark.redaction.regex is set, only keys matching it are redacted.
  // A key that would match the default pattern (e.g. "password") but NOT the
  // custom regex must be printed in plain text.
  std::unordered_map<std::string, std::string> conf = {
      {kSparkRedactionRegex, "supersensitive"},
      {"spark.datasource.jdbc.password", "pass123"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isPlain(out, "spark.datasource.jdbc.password", "pass123"));
}

TEST(PrintConfig, CaseInsensitiveDefaultRedaction) {
  std::unordered_map<std::string, std::string> conf = {
      {"spark.my.PASSWORD", "uppercase_pw"},
  };
  auto out = printConfig(conf);
  EXPECT_TRUE(isRedacted(out, "spark.my.PASSWORD"));
}

} // namespace gluten

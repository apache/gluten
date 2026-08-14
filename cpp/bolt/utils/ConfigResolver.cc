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

// This File includes common helper functions with Arrow dependency.

#include "ConfigResolver.h"
#include "utils/Exception.h"

#include <algorithm>
#include <cctype>

namespace {

bool parseBoolConfigValue(const std::string& value, const std::string& key) {
  auto normalizedValue = value;
  std::transform(normalizedValue.begin(), normalizedValue.end(), normalizedValue.begin(), [](unsigned char c) {
    return std::tolower(c);
  });

  if (normalizedValue == "true" || normalizedValue == "1" || normalizedValue == "yes" || normalizedValue == "y") {
    return true;
  }
  if (normalizedValue == "false" || normalizedValue == "0" || normalizedValue == "no" || normalizedValue == "n") {
    return false;
  }
  GLUTEN_CHECK(false, "Invalid boolean config value for " + key + ": " + value);
  return false;
}

} // namespace

namespace gluten {

std::string getConfigValue(
    const std::unordered_map<std::string, std::string>& confMap,
    const std::string& key,
    const std::optional<std::string>& fallbackValue) {
  auto got = confMap.find(key);
  if (got == confMap.end()) {
    GLUTEN_CHECK(fallbackValue != std::nullopt, "No such config key: " + key);
    return fallbackValue.value();
  }
  return got->second;
}

bool getBoolConfigValue(
    const std::unordered_map<std::string, std::string>& confMap,
    const std::string& key,
    bool fallbackValue) {
  auto got = confMap.find(key);
  if (got == confMap.end()) {
    return fallbackValue;
  }
  return parseBoolConfigValue(got->second, key);
}
} // namespace gluten

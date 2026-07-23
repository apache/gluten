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

#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include "velox/common/config/Config.h"

namespace gluten {

enum class FileSystemType : uint8_t { kHdfs, kS3, kAbfs, kGcs, kAll };

/// Create hive connector session config.
std::shared_ptr<facebook::velox::config::ConfigBase> createHiveConnectorSessionConfig(
    const std::shared_ptr<facebook::velox::config::ConfigBase>& conf);

std::string getConfigValue(
    const std::unordered_map<std::string, std::string>& confMap,
    const std::string& key,
    const std::optional<std::string>& fallbackValue);

/// Create hive connector config.
std::shared_ptr<facebook::velox::config::ConfigBase> createHiveConnectorConfig(
    const std::shared_ptr<facebook::velox::config::ConfigBase>& conf,
    FileSystemType fsType = FileSystemType::kAll);

void overwriteVeloxConf(
    const facebook::velox::config::ConfigBase* from,
    std::unordered_map<std::string, std::string>& to,
    const std::string& prefix);

} // namespace gluten

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
#include <fmt/ranges.h>
#include "dwio/common/Options.h"
#include "substrait/SubstraitToBoltPlan.h"
#include <unordered_map>

namespace gluten::paimon {


struct PaimonSplitInfo : SplitInfo {
    struct FileMeta {
        dwio::common::FileFormat format;
        int32_t bucket;
        int64_t firstRowId;
        int64_t maxSequenceNumber;
        int32_t splitGroup;
        bool useHiveSplit;
        std::vector<std::string> primaryKeys;
        bool rawConvertible;
    };

    std::unordered_map<std::string, FileMeta> metaByPath_;

    explicit PaimonSplitInfo(const SplitInfo& splitInfo)
        : SplitInfo(splitInfo) {}

    const FileMeta& metaAt(size_t i) const {
        const auto& path = paths.at(i);
        auto it = metaByPath_.find(path);
        if (it == metaByPath_.end()) {
            throw std::runtime_error("Missing Paimon split meta for path: " + path);
        }
        return it->second;
    }

    const std::string toString() const {
        // convert metaByPath to string
        std::ostringstream metaByPathStr;
        for (const auto& [path, meta] : metaByPath_) {
            metaByPathStr << fmt::format("{}: {{format: {}, bucket: {}, firstRowId: {}, maxSequenceNumber: {}, splitGroup: {}, useHiveSplit: {}, primaryKeys: {}}}",
                path, dwio::common::toString(meta.format), meta.bucket, meta.firstRowId, meta.maxSequenceNumber, meta.splitGroup, meta.useHiveSplit, meta.primaryKeys);
        }

        return fmt::format(
            "PaimonSplitInfo[{}, {}]",
            dwio::common::toString(format), metaByPathStr.str());
    }

};

class PaimonPlanUtils {
public:
    static std::shared_ptr<PaimonSplitInfo> parsePaimonSplitInfo(
      substrait::ReadRel_LocalFiles_FileOrFiles file,
      std::shared_ptr<SplitInfo> splitInfo);
};


} // namespace gluten::paimon

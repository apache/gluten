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

#include "shuffle/rss/RssClient.h"
#include "utils/Common.h"
#include "utils/Macros.h"

#include <arrow/buffer.h>

#include <map>

namespace gluten {

/// A local implementation of the RssClient interface for testing purposes.
class LocalRssClient : public RssClient {
 public:
  LocalRssClient(std::string dataFile) : dataFile_(dataFile) {}

  int32_t pushPartitionData(int32_t partitionId, const char* bytes, int64_t size) override;

  void stop() override;

 private:
  std::string dataFile_;
  std::vector<std::unique_ptr<arrow::ResizableBuffer>> buffers_;
  std::map<uint32_t, uint32_t> partitionBufferMap_;
};
} // namespace gluten

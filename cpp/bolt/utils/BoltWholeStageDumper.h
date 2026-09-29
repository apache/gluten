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

#include "compute/BoltRuntime.h"
#include "utils/WholeStageDumper.h"

namespace gluten {

class BoltWholeStageDumper final : public WholeStageDumper {
 public:
  BoltWholeStageDumper(
      const SparkTaskInfo& taskInfo,
      const std::string& saveDir,
      int64_t batchSize,
      bytedance::bolt::memory::MemoryPool* aggregatePool);

  void dumpConf(const std::unordered_map<std::string, std::string>& confMap) override;

  void dumpPlan(const std::string& planJson) override;

  void dumpInputSplit(int32_t splitIndex, const std::string& splitJson) override;

  std::shared_ptr<ColumnarBatchIterator> dumpInputIterator(
      int32_t iteratorIndex,
      const std::shared_ptr<ColumnarBatchIterator>& inputIterator) override;

 private:
  SparkTaskInfo taskInfo_;
  std::string saveDir_;
  int64_t batchSize_;

  bytedance::bolt::memory::MemoryPool* pool_;
};

} // namespace gluten

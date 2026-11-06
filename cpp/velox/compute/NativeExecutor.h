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

#include "compute/VeloxPlanEvaluator.h"

namespace gluten {
// The runtime must outlive this benchmark driver and its output iterator.
class NativeExecutor final {
 public:
  NativeExecutor(
      VeloxRuntime* runtime,
      std::vector<std::shared_ptr<VeloxPlanEvaluator>> stages,
      int batchSize,
      bool hashPartitioned,
      bool resizeShuffleInput,
      bool resizeShuffleOutput,
      int minRows,
      int maxRows,
      int64_t preferredBytes,
      bool copyRanges,
      std::string shuffleDir);
  std::shared_ptr<ResultIterator> execute(
      std::vector<std::vector<std::shared_ptr<ColumnarBatch>>> inputs,
      int64_t& shuffleBytes);

 private:
  std::shared_ptr<ResultIterator> runSingleStage(std::vector<std::vector<std::shared_ptr<ColumnarBatch>>> inputs);
  std::shared_ptr<ResultIterator> runWithShuffle(
      std::vector<std::vector<std::shared_ptr<ColumnarBatch>>> inputs,
      int64_t& shuffleBytes);
  std::shared_ptr<ResultIterator> resize(std::shared_ptr<ResultIterator> input) const;
  VeloxRuntime* runtime_;
  std::vector<std::shared_ptr<VeloxPlanEvaluator>> stages_;
  std::shared_ptr<arrow::Schema> stateSchema_;
  int batchSize_;
  bool hashPartitioned_;
  bool resizeShuffleInput_;
  bool resizeShuffleOutput_;
  int minRows_;
  int maxRows_;
  int64_t preferredBytes_;
  bool copyRanges_;
  std::string shuffleDir_;
};
} // namespace gluten

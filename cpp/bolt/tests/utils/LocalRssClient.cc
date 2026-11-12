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

#include "tests/utils/LocalRssClient.h"
#include "utils/Common.h"

#include <arrow/io/file.h>

namespace gluten {

int32_t LocalRssClient::pushPartitionData(int32_t partitionId, const char* bytes, int64_t size) {
  auto [iter, inserted] = partitionBufferMap_.try_emplace(partitionId, buffers_.size());
  int32_t idx = iter->second;

  // Allocate new buffer if it's a new partition.
  if (inserted) {
    buffers_.emplace_back();
    GLUTEN_ASSIGN_OR_THROW(buffers_.back(), arrow::AllocateResizableBuffer(0));
  }

  auto& buffer = buffers_[idx];
  auto newSize = buffer->size() + size;

  if (buffer->capacity() < newSize) {
    GLUTEN_THROW_NOT_OK(buffer->Reserve(newSize));
  }

  fastCopy(buffer->mutable_data() + buffer->size(), bytes, size);
  GLUTEN_THROW_NOT_OK(buffer->Resize(newSize));

  return size;
}

void LocalRssClient::stop() {
  std::shared_ptr<arrow::io::FileOutputStream> out;
  GLUTEN_ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(dataFile_));

  for (auto item : partitionBufferMap_) {
    GLUTEN_THROW_NOT_OK(out->Write(buffers_[item.second]->data(), buffers_[item.second]->size()));
    GLUTEN_THROW_NOT_OK(out->Flush());
  }
  GLUTEN_THROW_NOT_OK(out->Close());

  buffers_.clear();
  partitionBufferMap_.clear();
}

} // namespace gluten

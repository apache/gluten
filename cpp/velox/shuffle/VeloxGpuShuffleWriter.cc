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

#include "VeloxGpuShuffleWriter.h"

namespace gluten {

// Split bool bit to bytes.
void VeloxGpuHashShuffleWriter::splitBoolValueType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
  for (auto& pid : partitionUsed_) {
    auto dstaddr = dstAddrs[pid];
    if (dstaddr == nullptr) {
      continue;
    }
    auto dstPidBase = reinterpret_cast<uint8_t*>(dstaddr + partitionBufferBase_[pid] * sizeof(uint8_t));
    auto pos = partition2RowOffsetBase_[pid];
    auto end = partition2RowOffsetBase_[pid + 1];
    for (; pos < end; ++pos) {
      auto rowId = rowOffset2RowId_[pos];
      // === Extract one bool value from the bit-packed source ===
      uint8_t byte = srcAddr[rowId >> 3]; // find the source byte
      uint8_t bit = (byte >> (rowId & 7)) & 0x01; // extract 0 or 1
      *dstPidBase++ = bit; // copy
    }
  }
}

// Split timestamp from int128_t to int64_t, both of them represents the timestamp nanoseconds.
arrow::Status VeloxGpuHashShuffleWriter::splitTimestamp(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
   for (auto& pid : partitionUsed_) {
      auto dstPidBase = reinterpret_cast<int64_t*>(dstAddrs[pid] + partitionBufferBase_[pid] * sizeof(int64_t));
      auto pos = partition2RowOffsetBase_[pid];
      auto end = partition2RowOffsetBase_[pid + 1];
      for (; pos < end; ++pos) {
        auto rowId = rowOffset2RowId_[pos];
        auto* src = reinterpret_cast<const int64_t*>(srcAddr) + rowId * 2;
        // src[0] is seconds, src[1] is nanoseconds in Timestamp.
        *dstPidBase++ = src[0] * 1'000'000'000LL + src[1];
      }
    }
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxGpuHashShuffleWriter::create(
    uint32_t numPartitions,
    const std::shared_ptr<PartitionWriter>& partitionWriter,
    const std::shared_ptr<ShuffleWriterOptions>& options,
    MemoryManager* memoryManager) {
  if (auto hashOptions = std::dynamic_pointer_cast<GpuHashShuffleWriterOptions>(options)) {
    std::shared_ptr<VeloxGpuHashShuffleWriter> res =
        std::make_shared<VeloxGpuHashShuffleWriter>(numPartitions, partitionWriter, hashOptions, memoryManager);
    RETURN_NOT_OK(res->init());
    return res;
  }
  return arrow::Status::Invalid("Error casting ShuffleWriterOptions to GpuHashShuffleWriterOptions. ");
}

} // namespace gluten

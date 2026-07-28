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

#include "shuffle/ShuffleWriter.h"
#include "utils/Exception.h"

namespace gluten {

namespace {
const std::string kHashShuffleName = "hash";
const std::string kSortShuffleName = "sort";
const std::string kRssSortShuffleName = "rss_sort";
} // namespace

ShuffleWriterType ShuffleWriter::stringToType(const std::string& typeString) {
  if (typeString == kHashShuffleName) {
    return ShuffleWriterType::kHashShuffle;
  }
  if (typeString == kSortShuffleName) {
    return ShuffleWriterType::kSortShuffle;
  }
  if (typeString == kRssSortShuffleName) {
    return ShuffleWriterType::kRssSortShuffle;
  }
  throw GlutenException("Unrecognized shuffle writer type: " + typeString);
}

std::string ShuffleWriter::typeToString(ShuffleWriterType type) {
  switch (type) {
    case ShuffleWriterType::kHashShuffle:
      return kHashShuffleName;
    case ShuffleWriterType::kSortShuffle:
      return kSortShuffleName;
    case ShuffleWriterType::kRssSortShuffle:
      return kRssSortShuffleName;
  }
  GLUTEN_UNREACHABLE();
}

int32_t ShuffleWriter::numPartitions() const {
  return numPartitions_;
}

int64_t ShuffleWriter::bytesWritten() const {
  return writtenBytes_;
}

int64_t ShuffleWriter::totalBytesEvicted() const {
  return metrics_.totalBytesEvicted;
}

int64_t ShuffleWriter::totalBytesToEvict() const {
  return metrics_.totalBytesToEvict;
}

int64_t ShuffleWriter::totalWriteTime() const {
  return metrics_.totalWriteTime;
}

int64_t ShuffleWriter::totalEvictTime() const {
  return metrics_.totalEvictTime;
}

int64_t ShuffleWriter::totalCompressTime() const {
  return metrics_.totalCompressTime;
}

int64_t ShuffleWriter::totalSortTime() const {
  return 0;
}

int64_t ShuffleWriter::totalC2RTime() const {
  return 0;
}

double ShuffleWriter::avgDictionaryFields() const {
  return metrics_.avgDictionaryFields;
}

int64_t ShuffleWriter::dictionarySize() const {
  return metrics_.dictionarySize;
}

const std::vector<int64_t>& ShuffleWriter::partitionLengths() const {
  return metrics_.partitionLengths;
}

const std::vector<int64_t>& ShuffleWriter::rawPartitionLengths() const {
  return metrics_.rawPartitionLengths;
}

const std::vector<int64_t>& ShuffleWriter::rowBasedChecksums() const {
  return metrics_.rowBasedChecksums;
}

ShuffleWriter::ShuffleWriter(int32_t numPartitions, Partitioning partitioning)
    : numPartitions_(numPartitions), partitioning_(partitioning) {}
} // namespace gluten

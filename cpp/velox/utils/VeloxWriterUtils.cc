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

#include "utils/VeloxWriterUtils.h"

#include <arrow/type.h>
#include <boost/algorithm/string.hpp>
#include <algorithm>

#include "config/GlutenConfig.h"
#include "utils/ConfigExtractor.h"
#include "utils/Exception.h"
#include "utils/VeloxArrowUtils.h"

#include "memory/VeloxMemoryManager.h"
#include "velox/common/compression/Compression.h"
#include "velox/type/Type.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace gluten {

using namespace facebook::velox;
using namespace facebook::velox::parquet;
using namespace facebook::velox::common;

namespace {
const int32_t kGzipWindowBits4k = 12;
const int32_t kZSTDDefaultCompressionLevel = 3;

TypePtr parquetWriteType(const TypePtr& type, const ::arrow::DataType& arrowType) {
  switch (arrowType.id()) {
    case ::arrow::Type::TIMESTAMP: {
      const auto& timestamp = static_cast<const ::arrow::TimestampType&>(arrowType);
      if (timestamp.timezone().empty()) {
        return TIMESTAMP_UTC();
      }
      return TIMESTAMP();
    }
    case ::arrow::Type::STRUCT: {
      std::vector<TypePtr> children;
      children.reserve(type->size());
      for (auto i = 0; i < type->size(); ++i) {
        children.push_back(parquetWriteType(type->childAt(i), *arrowType.field(i)->type()));
      }
      return ROW(asRowType(type)->names(), std::move(children));
    }
    case ::arrow::Type::LIST: {
      const auto& list = static_cast<const ::arrow::ListType&>(arrowType);
      return ARRAY(parquetWriteType(type->childAt(0), *list.value_type()));
    }
    case ::arrow::Type::MAP: {
      const auto& map = static_cast<const ::arrow::MapType&>(arrowType);
      return MAP(
          parquetWriteType(type->childAt(0), *map.key_type()), parquetWriteType(type->childAt(1), *map.item_type()));
    }
    default:
      return type;
  }
}

bool hasNullStructs(const VectorPtr& vector) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::ROW: {
      const auto* row = vector->asUnchecked<RowVector>();
      if (row->mayHaveNulls() && BaseVector::countNulls(row->nulls(), row->size()) != 0) {
        return true;
      }
      return std::any_of(row->children().begin(), row->children().end(), hasNullStructs);
    }
    case VectorEncoding::Simple::ARRAY:
      return hasNullStructs(vector->asUnchecked<ArrayVector>()->elements());
    case VectorEncoding::Simple::MAP: {
      const auto* map = vector->asUnchecked<MapVector>();
      return hasNullStructs(map->mapKeys()) || hasNullStructs(map->mapValues());
    }
    case VectorEncoding::Simple::FLAT:
      return false;
    default:
      return !vector->isScalar();
  }
}

void propagateStructNulls(const VectorPtr& vector) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::ROW: {
      auto* row = vector->asUnchecked<RowVector>();
      const SelectivityVector allRows(row->size());
      for (auto& child : row->children()) {
        child->addNulls(row->rawNulls(), allRows);
        propagateStructNulls(child);
      }
      break;
    }
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP: {
      auto* array = vector->asUnchecked<ArrayVectorBase>();
      for (auto row = 0; row < array->size(); ++row) {
        if (array->isNullAt(row)) {
          array->setOffsetAndSize(row, 0, 0);
        }
      }
      if (vector->encoding() == VectorEncoding::Simple::ARRAY) {
        propagateStructNulls(vector->asUnchecked<ArrayVector>()->elements());
      } else {
        auto* map = vector->asUnchecked<MapVector>();
        propagateStructNulls(map->mapKeys());
        propagateStructNulls(map->mapValues());
      }
      break;
    }
    case VectorEncoding::Simple::FLAT:
      break;
    default:
      VELOX_FAIL("Expected a materialized Parquet input, got {}", vector->encoding());
  }
}
} // namespace

std::shared_ptr<dwio::common::FormatSpecificOptions> GlutenParquetWriterFactory::createFormatOptions(
    const config::ConfigBase& connectorConfig,
    const config::ConfigBase& session) const {
  auto options = ParquetWriterFactory::createFormatOptions(connectorConfig, session);
  if (auto level = session.get<int32_t>("writer_compression_level")) {
    auto parquetOptions = std::static_pointer_cast<ParquetWriterOptions>(options);
    parquetOptions->codecOptions = std::make_shared<parquet::arrow::util::CodecOptions>(*level);
  }
  return options;
}

std::shared_ptr<facebook::velox::dwio::common::WriterOptions> makeParquetWriteOption(
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  int64_t maxRowGroupBytes = 134217728; // 128MB
  int64_t maxRowGroupRows = 100000000; // 100M
  if (auto it = sparkConfs.find(kParquetBlockSize); it != sparkConfs.end()) {
    maxRowGroupBytes = std::stoll(it->second);
  }
  if (auto it = sparkConfs.find(kParquetBlockRows); it != sparkConfs.end()) {
    maxRowGroupRows = std::stoll(it->second);
  }

  auto writeOption = std::make_shared<facebook::velox::dwio::common::WriterOptions>();
  auto parquetOptions = std::make_shared<ParquetWriterOptions>();
  parquetOptions->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds /*micro*/;
  bool writeLegacyParquetFormat = false;
  if (auto it = sparkConfs.find(kParquetStoreDecimalAsInteger); it != sparkConfs.end()) {
    writeLegacyParquetFormat = boost::iequals(it->second, "true");
  }
  // Maps spark.sql.parquet.writeLegacyFormat to Velox enableStoreDecimalAsInteger (inverted).
  // Controls how DECIMAL values are stored by the Writer:
  // - true (default when legacy format is off): store as integer using INT32/INT64 for short
  //   DECIMAL precisions; higher precisions use FIXED_LEN_BYTE_ARRAY.
  // - false (when spark.sql.parquet.writeLegacyFormat is true): store as FIXED_LEN_BYTE_ARRAY
  //   regardless of precision (Spark legacy Parquet decimal layout).
  // TODO: Only DECIMAL is handled here. Spark's writeLegacyFormat also changes ARRAY (bag/array
  // vs list/element) and MAP (map vs key_value) physical layouts, which are not yet supported
  // in Gluten native Parquet write.
  parquetOptions->enableStoreDecimalAsInteger = !writeLegacyParquetFormat;
  auto compressionCodec = CompressionKind::CompressionKind_SNAPPY;
  if (auto it = sparkConfs.find(kParquetCompressionCodec); it != sparkConfs.end()) {
    auto compressionCodecStr = it->second;
    // spark support none, uncompressed, snappy, gzip, lzo, brotli, lz4, zstd.
    if (boost::iequals(compressionCodecStr, "snappy")) {
      compressionCodec = CompressionKind::CompressionKind_SNAPPY;
    } else if (boost::iequals(compressionCodecStr, "gzip")) {
      compressionCodec = CompressionKind::CompressionKind_GZIP;
      if (sparkConfs.find(kParquetGzipWindowSize) != sparkConfs.end()) {
        auto parquetGzipWindowSizeStr = sparkConfs.find(kParquetGzipWindowSize)->second;
        if (parquetGzipWindowSizeStr == kGzipWindowSize4k) {
          auto codecOptions = std::make_shared<parquet::arrow::util::GZipCodecOptions>();
          codecOptions->windowBits = kGzipWindowBits4k;
          parquetOptions->codecOptions = std::move(codecOptions);
        }
      }
    } else if (boost::iequals(compressionCodecStr, "lzo")) {
      compressionCodec = CompressionKind::CompressionKind_LZO;
    } else if (boost::iequals(compressionCodecStr, "brotli")) {
      // please make sure `brotli` is enabled when compiling
      throw GlutenException("Gluten+velox does not support write parquet using brotli.");
    } else if (boost::iequals(compressionCodecStr, "lz4")) {
      compressionCodec = CompressionKind::CompressionKind_LZ4_HADOOP;
    } else if (boost::iequals(compressionCodecStr, "zstd")) {
      compressionCodec = CompressionKind::CompressionKind_ZSTD;
      auto codecOptions = std::make_shared<parquet::arrow::util::CodecOptions>();
      auto it = sparkConfs.find(kParquetZSTDCompressionLevel);
      auto compressionLevel = it != sparkConfs.end() ? std::stoi(it->second) : kZSTDDefaultCompressionLevel;
      codecOptions->compressionLevel = compressionLevel;
      parquetOptions->codecOptions = std::move(codecOptions);
    } else if (boost::iequals(compressionCodecStr, "uncompressed")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    } else if (boost::iequals(compressionCodecStr, "none")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    }
  }
  writeOption->compressionKind = compressionCodec;
  writeOption->flushPolicyFactory = [maxRowGroupRows, maxRowGroupBytes]() {
    return std::make_unique<LambdaFlushPolicy>(maxRowGroupRows, maxRowGroupBytes, [&]() { return false; });
  };
  if (auto it = sparkConfs.find(kSessionTimezone); it != sparkConfs.end()) {
    parquetOptions->parquetWriteTimestampTimeZone = normalizeSessionTimezone(it->second);
  }
  parquetOptions->arrowMemoryPool =
      getDefaultMemoryManager()->getOrCreateArrowMemoryPool("VeloxParquetWrite.ArrowMemoryPool");
  if (auto it = sparkConfs.find(kParquetDataPageSize); it != sparkConfs.end()) {
    auto dataPageSize = std::stoll(it->second);
    parquetOptions->dataPageSize = dataPageSize;
  }
  if (auto it = sparkConfs.find(kParquetWriterVersion); it != sparkConfs.end()) {
    auto parquetVersion = it->second;
    if (boost::iequals(parquetVersion, "v2")) {
      parquetOptions->useParquetDataPageV2 = true;
    }
  }
  if (auto it = sparkConfs.find(kParquetEnableDictionary); it != sparkConfs.end()) {
    auto enableDictionary = it->second;
    if (boost::iequals(enableDictionary, "true")) {
      parquetOptions->enableDictionary = true;
    } else {
      parquetOptions->enableDictionary = false;
    }
  }
  // Write the Parquet page index (column index + offset index) by default to match
  // Spark/parquet-mr (SPARK-26345); Velox's writer leaves it opt-in. Disable with
  // parquet.enable.page.index=false.
  bool enableWritePageIndex = true;
  if (auto it = sparkConfs.find(kParquetEnablePageIndex); it != sparkConfs.end()) {
    enableWritePageIndex = boost::iequals(it->second, "true");
  }
  parquetOptions->enableWritePageIndex = enableWritePageIndex;
  // Velox's writer emits Parquet size statistics by default, which parquet-mr
  // used by Spark 3.x does not write. Disable them to keep file output (and the
  // size-based planning that depends on it) unchanged.
  parquetOptions->sizeStatisticsLevel = parquet::arrow::SizeStatisticsLevel::kNone;
  writeOption->formatSpecificOptions = std::move(parquetOptions);
  return writeOption;
}

RowTypePtr makeParquetWriteSchema(const std::shared_ptr<::arrow::Schema>& schema) {
  // Arrow's C++ map import drops key-field metadata. Spark writer schemas still
  // identify NTZ unambiguously through the Arrow timestamp's empty timezone.
  return asRowType(parquetWriteType(fromArrowSchema(schema), *::arrow::struct_(schema->fields())));
}

RowVectorPtr prepareParquetWriteInput(const RowVectorPtr& input) {
  auto children = input->children();
  bool changed = false;
  for (auto& child : children) {
    if (!hasNullStructs(child)) {
      continue;
    }
    // Arrow conversion visits child payloads even underneath null structs.
    // Materialize only affected columns, then mask their unreachable children.
    auto materialized = BaseVector::create(child->type(), child->size(), input->pool());
    materialized->copy(child.get(), 0, 0, child->size());
    propagateStructNulls(materialized);
    child = std::move(materialized);
    changed = true;
  }
  if (!changed) {
    return input;
  }
  return std::make_shared<RowVector>(input->pool(), input->type(), input->nulls(), input->size(), std::move(children));
}

RowVectorPtr makePartitionIdInput(const RowVectorPtr& input, const std::vector<uint32_t>& partitionChannels) {
  const auto rowType = asRowType(input->type());
  auto types = rowType->children();
  auto children = input->children();
  bool changed = false;
  for (auto channel : partitionChannels) {
    if (!types.at(channel)->isTimestamp()) {
      continue;
    }
    // PartitionIdGenerator only accepts millisecond timestamps. Use Spark's exact
    // microseconds for IDs, retaining the original vectors for files and paths.
    DecodedVector decoded(*children.at(channel));
    auto micros = BaseVector::create<FlatVector<int64_t>>(BIGINT(), input->size(), input->pool());
    for (auto row = 0; row < input->size(); ++row) {
      if (decoded.isNullAt(row)) {
        micros->setNull(row, true);
      } else {
        micros->set(row, decoded.valueAt<Timestamp>(row).toMicros());
      }
    }
    types[channel] = BIGINT();
    children[channel] = std::move(micros);
    changed = true;
  }
  if (!changed) {
    return input;
  }
  return std::make_shared<RowVector>(
      input->pool(), ROW(rowType->names(), std::move(types)), input->nulls(), input->size(), std::move(children));
}

} // namespace gluten

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

#include "utils/BoltArrowUtils.h"

#include <arrow/buffer.h>

#include "memory/BoltColumnarBatch.h"
#include "utils/Common.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/arrow/Bridge.h"

namespace gluten {

using namespace bytedance;

void toArrowSchema(const bolt::TypePtr& rowType, bytedance::bolt::memory::MemoryPool* pool, struct ArrowSchema* out) {
  exportToArrow(bolt::BaseVector::create(rowType, 0, pool), *out, ArrowUtils::getBridgeOptions());
}

std::shared_ptr<arrow::Schema> toArrowSchema(const bolt::TypePtr& rowType, bytedance::bolt::memory::MemoryPool* pool) {
  ArrowSchema arrowSchema;
  toArrowSchema(rowType, pool, &arrowSchema);
  GLUTEN_ASSIGN_OR_THROW(auto outputSchema, arrow::ImportSchema(&arrowSchema));
  return outputSchema;
}

bolt::TypePtr fromArrowSchema(const std::shared_ptr<arrow::Schema>& schema) {
  ArrowSchema cSchema;
  GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*schema, &cSchema));
  bolt::TypePtr typePtr = bolt::importFromArrow(cSchema);
  // It should be bolt::importFromArrow's duty to release the imported arrow c schema.
  // Since exported Bolt type prt doesn't hold memory from the c schema.
  ArrowSchemaRelease(&cSchema); // otherwise the c schema leaks memory
  return typePtr;
}

arrow::Result<std::shared_ptr<ColumnarBatch>> recordBatch2BoltColumnarBatch(const arrow::RecordBatch& rb) {
  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  RETURN_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
  auto vp = bolt::importFromArrowAsOwner(
      arrowSchema, arrowArray, ArrowUtils::getBridgeOptions(), gluten::defaultLeafBoltMemoryPool().get());
  return std::make_shared<BoltColumnarBatch>(std::dynamic_pointer_cast<bolt::RowVector>(vp));
}

bytedance::bolt::common::CompressionKind arrowCompressionTypeToBolt(arrow::Compression::type type) {
  switch (type) {
    case arrow::Compression::UNCOMPRESSED:
      return bytedance::bolt::common::CompressionKind::CompressionKind_NONE;
    case arrow::Compression::LZ4_FRAME:
      return bytedance::bolt::common::CompressionKind::CompressionKind_LZ4;
    case arrow::Compression::ZSTD:
      return bytedance::bolt::common::CompressionKind::CompressionKind_ZSTD;
    case arrow::Compression::GZIP:
      return bytedance::bolt::common::CompressionKind::CompressionKind_GZIP;
    case arrow::Compression::SNAPPY:
      return bytedance::bolt::common::CompressionKind::CompressionKind_SNAPPY;
    case arrow::Compression::LZO:
      return bytedance::bolt::common::CompressionKind::CompressionKind_LZO;
    default:
      BOLT_UNSUPPORTED("Unsupported arrow compression type {}", arrow::util::Codec::GetCodecAsString(type));
  }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> toArrowBuffer(
    bytedance::bolt::BufferPtr buffer,
    arrow::MemoryPool* pool) {
  if (buffer == nullptr) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto arrowBuffer, arrow::AllocateResizableBuffer(buffer->size(), pool));
  gluten::fastCopy(arrowBuffer->mutable_data(), buffer->as<void>(), buffer->size());
  return arrowBuffer;
}

} // namespace gluten

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
#include "BoltColumnarBatch.h"
#include "compute/BoltRuntime.h"
#include "utils/Timer.h"
#include "utils/BoltArrowUtils.h"
#include "bolt/row/UnsafeRowFast.h"
#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"

namespace gluten {

using namespace bytedance;
using namespace bytedance::bolt;

namespace {

RowVectorPtr makeRowVector(
    bolt::memory::MemoryPool* pool,
    int32_t numRows,
    std::vector<std::string> childNames,
    BufferPtr nulls,
    const std::vector<VectorPtr>& children) {
  std::vector<std::shared_ptr<const Type>> childTypes;
  childTypes.resize(children.size());
  for (int i = 0; i < children.size(); i++) {
    childTypes[i] = children[i]->type();
  }
  auto rowType = ROW(std::move(childNames), std::move(childTypes));
  return std::make_shared<RowVector>(pool, rowType, nulls, numRows, std::move(children));
}
} // namespace

void BoltColumnarBatch::ensureFlattened() {
  if (flattened_) {
    return;
  }
  ScopedTimer timer(&exportNanos_);
  for (auto& child : rowVector_->children()) {
    bytedance::bolt::BaseVector::flattenVector(child);
    if (child->isLazy()) {
      child = child->as<bytedance::bolt::LazyVector>()->loadedVectorShared();
      BOLT_DCHECK_NOT_NULL(child);
    }
    // In case of output from Limit, RowVector size can be smaller than its children size.
    if (child->size() > rowVector_->size()) {
      child = child->slice(0, rowVector_->size());
    }
  }
  flattened_ = true;
}

std::shared_ptr<ArrowSchema> BoltColumnarBatch::exportArrowSchema() {
  auto out = std::make_shared<ArrowSchema>();
  ensureFlattened();
  bolt::exportToArrow(rowVector_, *out, ArrowUtils::getBridgeOptions());
  return out;
}

std::shared_ptr<ArrowArray> BoltColumnarBatch::exportArrowArray() {
  auto out = std::make_shared<ArrowArray>();
  ensureFlattened();
  bolt::exportToArrow(rowVector_, *out, rowVector_->pool(), ArrowUtils::getBridgeOptions());
  return out;
}

int64_t BoltColumnarBatch::numBytes() {
  ensureFlattened();
  return rowVector_->estimateFlatSize();
}

bolt::RowVectorPtr BoltColumnarBatch::getRowVector() const {
  return rowVector_;
}

bolt::RowVectorPtr BoltColumnarBatch::getFlattenedRowVector() {
  ensureFlattened();
  return rowVector_;
}

std::shared_ptr<BoltColumnarBatch> BoltColumnarBatch::from(
    bytedance::bolt::memory::MemoryPool* pool,
    std::shared_ptr<ColumnarBatch> cb) {
  if (cb->getType() == "bolt") {
    return std::dynamic_pointer_cast<BoltColumnarBatch>(cb);
  }
  auto vp = bolt::importFromArrowAsOwner(
      *cb->exportArrowSchema(), *cb->exportArrowArray(), ArrowUtils::getBridgeOptions(), pool);
  return std::make_shared<BoltColumnarBatch>(std::dynamic_pointer_cast<bolt::RowVector>(vp));
}

std::shared_ptr<BoltColumnarBatch> BoltColumnarBatch::compose(
    bytedance::bolt::memory::MemoryPool* pool,
    const std::vector<std::shared_ptr<ColumnarBatch>>& batches) {
  GLUTEN_CHECK(!batches.empty(), "No batches to compose");

  int32_t numRows = -1;
  for (const auto& batch : batches) {
    GLUTEN_CHECK(batch->getType() == kType, "At least one of the input batches is not in Bolt format");
    if (numRows == -1) {
      numRows = batch->numRows();
      continue;
    }
    if (batch->numRows() != numRows) {
      throw GlutenException("Mismatched row counts among the input batches during composing columnar batches");
    }
    auto vb = std::dynamic_pointer_cast<BoltColumnarBatch>(batch);
    auto rv = vb->getRowVector();
    GLUTEN_CHECK(rv->nulls() == nullptr, "Vectors to compose contain null bits");
  }

  GLUTEN_CHECK(numRows > 0, "Illegal state");

  std::vector<std::string> childNames;
  std::vector<VectorPtr> children;
  for (const auto& batch : batches) {
    auto vb = std::dynamic_pointer_cast<BoltColumnarBatch>(batch);
    auto rv = vb->getRowVector();
    for (const std::string& name : rv->type()->asRow().names()) {
      childNames.push_back(name);
    }
    for (const VectorPtr& vec : rv->children()) {
      children.push_back(vec);
    }
  }
  RowVectorPtr outRv = makeRowVector(pool, numRows, std::move(childNames), nullptr, std::move(children));
  return std::make_shared<BoltColumnarBatch>(outRv);
}

std::shared_ptr<BoltColumnarBatch> BoltColumnarBatch::select(
    bytedance::bolt::memory::MemoryPool* pool,
    const std::vector<int32_t>& columnIndices) {
  std::vector<std::string> childNames;
  std::vector<VectorPtr> childVectors;
  childNames.reserve(columnIndices.size());
  childVectors.reserve(columnIndices.size());
  auto type = bytedance::bolt::asRowType(rowVector_->type());

  for (uint32_t i = 0; i < columnIndices.size(); i++) {
    auto index = columnIndices[i];
    auto child = rowVector_->childAt(index);
    childNames.push_back(type->nameOf(index));
    childVectors.push_back(child);
  }

  auto rowVector = makeRowVector(pool, numRows(), std::move(childNames), rowVector_->nulls(), std::move(childVectors));
  return std::make_shared<BoltColumnarBatch>(rowVector);
}

std::vector<char> BoltColumnarBatch::toUnsafeRow(int32_t rowId) const {
  auto fast = std::make_unique<bytedance::bolt::row::UnsafeRowFast>(rowVector_);
  auto size = fast->rowSize(rowId);
  std::vector<char> bytes(size);
  std::memset(bytes.data(), 0, bytes.size());
  fast->serialize(0, bytes.data());
  return bytes;
}

} // namespace gluten

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

#include <memory>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/record_batch.h"
#include "memory/ColumnarBatch.h"
#include "utils/Exception.h"

#include <arrow/type.h>

namespace gluten {
ColumnarBatch::ColumnarBatch(int32_t numColumns, int32_t numRows)
    : numColumns_(numColumns), numRows_(numRows), exportNanos_(0) {}

int32_t ColumnarBatch::numColumns() const {
  return numColumns_;
}

int32_t ColumnarBatch::numRows() const {
  return numRows_;
}

int64_t ColumnarBatch::getExportNanos() const {
  return exportNanos_;
}

std::vector<char> ColumnarBatch::toUnsafeRow(int32_t rowId) const {
  throw gluten::GlutenException("Not implemented toUnsafeRow for ColumnarBatch");
}

std::ostream& operator<<(std::ostream& os, const ColumnarBatch& columnarBatch) {
  return os << "NumColumns: " << std::to_string(columnarBatch.numColumns())
            << "NumRows: " << std::to_string(columnarBatch.numRows());
}

std::shared_ptr<ColumnarBatch> createZeroColumnBatch(int32_t numRows) {
  return std::make_shared<ArrowColumnarBatch>(arrow::RecordBatch::Make(
      std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>()),
      numRows,
      std::vector<std::shared_ptr<arrow::Array>>()));
}

ArrowColumnarBatch::ArrowColumnarBatch(std::shared_ptr<arrow::RecordBatch> batch)
    : ColumnarBatch(batch->num_columns(), batch->num_rows()), batch_(std::move(batch)) {}

std::string ArrowColumnarBatch::getType() const {
  return "arrow";
}

int64_t ArrowColumnarBatch::numBytes() {
  throw gluten::GlutenException("Not implemented GetBytes for ArrowColumnarBatch");
}

arrow::RecordBatch* ArrowColumnarBatch::getRecordBatch() const {
  return batch_.get();
}

std::shared_ptr<ArrowSchema> ArrowColumnarBatch::exportArrowSchema() {
  auto cSchema = std::make_shared<ArrowSchema>();
  GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*batch_->schema(), cSchema.get()));
  return cSchema;
}

std::shared_ptr<ArrowArray> ArrowColumnarBatch::exportArrowArray() {
  auto cArray = std::make_shared<ArrowArray>();
  GLUTEN_THROW_NOT_OK(arrow::ExportRecordBatch(*batch_, cArray.get()));
  return cArray;
}

std::vector<char> ArrowColumnarBatch::toUnsafeRow(int32_t rowId) const {
  throw gluten::GlutenException("#toUnsafeRow of ArrowColumnarBatch is not implemented");
}

ArrowCStructColumnarBatch::ArrowCStructColumnarBatch(
    std::unique_ptr<ArrowSchema> cSchema,
    std::unique_ptr<ArrowArray> cArray)
    : ColumnarBatch(cArray->n_children, cArray->length) {
  ArrowSchemaMove(cSchema.get(), cSchema_.get());
  ArrowArrayMove(cArray.get(), cArray_.get());
}

ArrowCStructColumnarBatch::~ArrowCStructColumnarBatch() {
  ArrowSchemaRelease(cSchema_.get());
  ArrowArrayRelease(cArray_.get());
}

std::string ArrowCStructColumnarBatch::getType() const {
  return "arrow_array";
}

int64_t ArrowCStructColumnarBatch::numBytes() {
  if (!numBytes_.has_value()) {
    int64_t bytes = cArray_->n_buffers;
    for (int64_t i = 0; i < cArray_->n_children; ++i) {
      bytes += cArray_->children[i]->n_buffers;
    }
    numBytes_ = bytes;
  }
  return numBytes_.value();
}

std::shared_ptr<ArrowSchema> ArrowCStructColumnarBatch::exportArrowSchema() {
  return cSchema_;
}

std::shared_ptr<ArrowArray> ArrowCStructColumnarBatch::exportArrowArray() {
  return cArray_;
}

std::vector<char> ArrowCStructColumnarBatch::toUnsafeRow(int32_t rowId) const {
  throw gluten::GlutenException("#toUnsafeRow of ArrowCStructColumnarBatch is not implemented");
}

} // namespace gluten

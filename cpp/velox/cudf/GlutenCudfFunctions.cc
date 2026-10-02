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

#include "cudf/GlutenCudfFunctions.h"

#include "operators/functions/RowConstructorWithNull.h"
#include "velox/common/base/Exceptions.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/table/table_view.hpp>

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace gluten {
namespace {

using facebook::velox::core::TypedExprPtr;
using facebook::velox::cudf_velox::ColumnOrView;
using facebook::velox::cudf_velox::CudfFunction;

class RowConstructorWithNullFunction final : public CudfFunction {
 public:
  explicit RowConstructorWithNullFunction(std::shared_ptr<CudfFunction> rowConstructor)
      : rowConstructor_(std::move(rowConstructor)) {
    VELOX_CHECK_NOT_NULL(rowConstructor_);
  }

  ColumnOrView eval(
      std::vector<ColumnOrView>& inputColumns,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr) const override {
    auto result = rowConstructor_->eval(inputColumns, stream, mr);
    auto* ownedResult = std::get_if<std::unique_ptr<cudf::column>>(&result);
    VELOX_CHECK_NOT_NULL(ownedResult, "row_constructor must return an owning cuDF column");

    auto structColumn = std::move(*ownedResult);
    VELOX_CHECK_NOT_NULL(structColumn);
    VELOX_CHECK(structColumn->type().id() == cudf::type_id::STRUCT, "row_constructor must return a cuDF STRUCT column");

    const auto numRows = structColumn->size();
    auto contents = structColumn->release();
    std::vector<cudf::column_view> childViews;
    childViews.reserve(contents.children.size());
    for (const auto& child : contents.children) {
      childViews.push_back(child->view());
    }

    auto [nullMask, nullCount] = cudf::bitmask_and(cudf::table_view{childViews}, stream, mr);
    // Rebuild through the struct factory instead of setting the parent mask in
    // place. The factory superimposes parent nulls on every child, which is
    // required when aggregate finalization reads a child column directly.
    return cudf::make_structs_column(numRows, std::move(contents.children), nullCount, std::move(nullMask), stream, mr);
  }

 private:
  std::shared_ptr<CudfFunction> rowConstructor_;
};

bool canEvaluateRowConstructorWithNull(const TypedExprPtr& expr) {
  return expr->type()->isRow() && !expr->inputs().empty() &&
      std::any_of(
             expr->inputs().begin(), expr->inputs().end(), [](const auto& input) { return !input->isConstantKind(); });
}

} // namespace

void registerGlutenCudfFunctions() {
  facebook::velox::cudf_velox::registerCudfFunction(
      RowConstructorWithNullCallToSpecialForm::kRowConstructorWithNull,
      [](const std::string&,
         const TypedExprPtr& expr,
         facebook::velox::memory::MemoryPool* pool) -> std::shared_ptr<CudfFunction> {
        auto rowConstructor = facebook::velox::cudf_velox::createCudfFunction("row_constructor", expr, pool);
        VELOX_CHECK_NOT_NULL(rowConstructor, "cuDF row_constructor must be registered first");
        return std::make_shared<RowConstructorWithNullFunction>(std::move(rowConstructor));
      },
      {},
      false,
      canEvaluateRowConstructorWithNull);
}

} // namespace gluten

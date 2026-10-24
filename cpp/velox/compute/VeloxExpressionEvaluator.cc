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
#include "VeloxExpressionEvaluator.h"

#include "compute/ProtobufUtils.h"
#include "substrait/SubstraitToVeloxExpr.h"
#include "substrait/extended_expression.pb.h"
#include "utils/Exception.h"

namespace gluten {
using namespace facebook::velox;

VeloxExpressionEvaluator::VeloxExpressionEvaluator(
    std::shared_ptr<memory::MemoryPool> pool,
    std::shared_ptr<core::QueryCtx> queryCtx,
    const uint8_t* data,
    int32_t size)
    : pool_(std::move(pool)), queryCtx_(std::move(queryCtx)), execCtx_(pool_.get(), queryCtx_.get()) {
  ::substrait::ExtendedExpression expression;
  GLUTEN_CHECK(data != nullptr && size > 0, "ExtendedExpression must not be empty");
  GLUTEN_CHECK(parseProtobuf(data, size, &expression), "Invalid ExtendedExpression protobuf");
  GLUTEN_CHECK(
      expression.has_base_schema() && expression.base_schema().has_struct_(),
      "ExtendedExpression requires base_schema");
  GLUTEN_CHECK(expression.referred_expr_size() == 1, "Expected exactly one referred scalar expression");
  const auto& reference = expression.referred_expr(0);
  GLUTEN_CHECK(reference.has_expression(), "Aggregate measures are not supported by the expression evaluator");
  GLUTEN_CHECK(
      reference.output_names_size() == 1 && !reference.output_names(0).empty(),
      "Expected exactly one non-empty output name");
  GLUTEN_CHECK(
      !expression.advanced_extensions().has_enhancement(), "ExtendedExpression enhancements are not supported");

  std::unordered_map<uint64_t, std::string> functions;
  for (const auto& extension : expression.extensions()) {
    GLUTEN_CHECK(extension.has_extension_function(), "Only scalar function extensions are supported");
    const auto& function = extension.extension_function();
    GLUTEN_CHECK(!function.name().empty(), "Function extension name must not be empty");
    GLUTEN_CHECK(
        functions.emplace(function.function_anchor(), function.name()).second, "Duplicate function extension anchor");
  }
  auto inputTypes = SubstraitParser::parseNamedStruct(expression.base_schema());
  auto inputNames = SubstraitParser::makeNames("input", inputTypes.size());
  inputType_ = ROW(std::move(inputNames), std::move(inputTypes));
  SubstraitVeloxExprConverter converter(pool_.get(), functions);
  auto typed = converter.toVeloxExpr(reference.expression(), inputType_);
  GLUTEN_CHECK(typed != nullptr, "Expression conversion returned no expression");
  outputType_ = ROW({reference.output_names(0)}, {typed->type()});
  expressions_ = exec::makeExprSetFromFlag(std::vector<core::TypedExprPtr>{std::move(typed)}, &execCtx_);
}

VeloxExpressionEvaluator::VeloxExpressionEvaluator(
    std::shared_ptr<memory::MemoryPool> pool,
    std::shared_ptr<core::QueryCtx> queryCtx,
    RowTypePtr inputType,
    RowTypePtr outputType,
    std::vector<core::TypedExprPtr> expressions)
    : pool_(std::move(pool)),
      queryCtx_(std::move(queryCtx)),
      execCtx_(pool_.get(), queryCtx_.get()),
      inputType_(std::move(inputType)),
      outputType_(std::move(outputType)),
      expressions_(exec::makeExprSetFromFlag(std::move(expressions), &execCtx_)) {}

std::shared_ptr<VeloxColumnarBatch> VeloxExpressionEvaluator::evaluate(const std::shared_ptr<ColumnarBatch>& input) {
  GLUTEN_CHECK(input != nullptr, "Input columnar batch must not be null");
  auto batch = std::dynamic_pointer_cast<VeloxColumnarBatch>(input);
  GLUTEN_CHECK(batch != nullptr, "Expression input must be a native Velox batch");
  auto row = batch->getRowVector();
  GLUTEN_CHECK(row != nullptr, "Input columnar batch contains no row vector");
  GLUTEN_CHECK(
      row->type()->equivalent(*inputType_),
      "Expression input schema mismatch: expected " + inputType_->toString() + ", got " + row->type()->toString());
  GLUTEN_CHECK(row->size() == input->numRows(), "Input columnar batch row count mismatch");

  auto columns = row->children();
  for (size_t i = 0; i < columns.size(); ++i) {
    if (*columns[i]->type() != *inputType_->childAt(i)) {
      // Normalize nested field names without retyping shared input vectors.
      columns[i] = BaseVector::copy(*columns[i], pool_.get());
      columns[i]->setType(inputType_->childAt(i));
    }
  }
  RowVector boundInput(pool_.get(), inputType_, row->nulls(), row->size(), std::move(columns));
  exec::EvalCtx context(&execCtx_, expressions_.get(), &boundInput);
  exec::LocalSelectivityVector rows(execCtx_, row->size());
  rows->setAll();
  std::vector<VectorPtr> result(outputType_->size());
  expressions_->eval(*rows, context, result);
  for (const auto& column : result) {
    GLUTEN_CHECK(column != nullptr, "Native expression returned no result");
    GLUTEN_CHECK(column->size() >= row->size(), "Native expression returned too few rows");
  }

  // Each output owns its vector, including references to unchanged input strings.
  return std::make_shared<VeloxColumnarBatch>(
      std::make_shared<RowVector>(pool_.get(), outputType_, nullptr, row->size(), std::move(result)));
}

} // namespace gluten

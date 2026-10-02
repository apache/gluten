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
#include "operators/functions/overlay/Elt.h"

#include "velox/common/base/Status.h"
#include "velox/common/base/VeloxException.h"
#include "velox/expression/EvalCtx.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"

using namespace facebook::velox;

namespace gluten {
namespace {

/// Spark's elt(n, input1, input2, ...) returns the n-th input, 1-based.
///
/// The inputs are all VARCHAR or all VARBINARY, so a single implementation
/// covers both: the result is copied verbatim out of the selected input, which
/// also lets the copy share the input's string buffers.
class EltFunction final : public exec::VectorFunction {
 public:
  explicit EltFunction(bool ansiEnabled) : ansiEnabled_{ansiEnabled} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // args[0] holds the 1-based index, args[1:] hold the candidate inputs.
    VELOX_CHECK_GE(args.size(), 2, "elt expects an index and at least one input.");
    const auto numInputs = static_cast<int32_t>(args.size()) - 1;

    exec::LocalDecodedVector indexHolder(context, *args[0], rows);
    const auto* indexes = indexHolder.get();

    context.ensureWritable(rows, outputType, result);
    // Rows that end up selecting no input keep this NULL: a NULL index, or an
    // out-of-range index with ANSI mode off.
    rows.applyToSelected([&](vector_size_t row) { result->setNull(row, true); });

    // Group the rows by the input they select, so that each input is copied in
    // a single pass. Most rows share the same index in practice, e.g. when the
    // index is a constant.
    std::vector<std::unique_ptr<SelectivityVector>> inputRows(numInputs);
    rows.applyToSelected([&](vector_size_t row) {
      if (indexes->isNullAt(row)) {
        return;
      }
      const auto index = indexes->valueAt<int32_t>(row);
      if (index < 1 || index > numInputs) {
        if (ansiEnabled_) {
          context.setStatus(row, invalidIndexStatus(index, numInputs));
        }
        return;
      }
      auto& selected = inputRows[index - 1];
      if (selected == nullptr) {
        selected = std::make_unique<SelectivityVector>(rows.end(), false);
      }
      selected->setValid(row, true);
    });

    for (int32_t i = 0; i < numInputs; ++i) {
      if (inputRows[i] != nullptr) {
        inputRows[i]->updateBounds();
        // Copies the values and the nulls, so a NULL in the selected input
        // becomes a NULL result.
        result->copy(args[i + 1].get(), *inputRows[i], nullptr);
      }
    }
  }

 private:
  static Status invalidIndexStatus(int32_t index, int32_t numInputs) {
    if (threadSkipErrorDetails()) {
      return Status::UserError();
    }
    return Status::UserError("The index is out of bounds for elt. index: {}, number of inputs: {}", index, numInputs);
  }

  const bool ansiEnabled_;
};

} // namespace

std::vector<exec::FunctionSignaturePtr> eltSignatures() {
  return {
      exec::FunctionSignatureBuilder().returnType("varchar").argumentType("integer").variableArity("varchar").build(),
      exec::FunctionSignatureBuilder()
          .returnType("varbinary")
          .argumentType("integer")
          .variableArity("varbinary")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeElt(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& /*inputArgs*/,
    const core::QueryConfig& config) {
  return std::make_shared<EltFunction>(functions::sparksql::SparkQueryConfig{config}.ansiEnabled());
}

} // namespace gluten

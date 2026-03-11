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

#pragma once

#include <unordered_map>
#include "SubstraitToVeloxPlan.h"
#include "velox/core/QueryCtx.h"

using namespace facebook;

namespace gluten {

/// This class is used to validate whether the computing of
/// a Substrait plan is supported in Velox.
class SubstraitToVeloxPlanValidator {
 public:
  SubstraitToVeloxPlanValidator(memory::MemoryPool* pool) {
    std::unordered_map<std::string, std::string> configs{
        {velox::core::QueryConfig::kSparkPartitionId, "0"}, {velox::core::QueryConfig::kSessionTimezone, "GMT"}};
    veloxCfg_ = std::make_shared<facebook::velox::config::ConfigBase>(std::move(configs));
    planConverter_ = std::make_unique<SubstraitToVeloxPlanConverter>(
        pool, veloxCfg_.get(), std::vector<std::shared_ptr<ResultIterator>>{}, std::nullopt, std::nullopt, true);
    queryCtx_ = velox::core::QueryCtx::create(nullptr, velox::core::QueryConfig(veloxCfg_->rawConfigs()));
    // An execution context used for function validation.
    execCtx_ = std::make_unique<velox::core::ExecCtx>(pool, queryCtx_.get());
  }

  /// Used to validate whether the computing of this Plan is supported.
  bool validate(const ::substrait::Plan& plan);

  /// Used to validate whether the computing of this Expression is supported.
  bool validate(
      const ::substrait::Expression& expression,
      const RowTypePtr& inputType,
      std::unordered_map<uint64_t, std::string> functionMappings);

  const std::vector<std::string>& getValidateLog() const {
    return validateLog_;
  }

 private:
  /// Used to validate whether the computing of this Write is supported.
  bool validate(const ::substrait::WriteRel& writeRel);

  /// Used to validate whether the computing of this Limit is supported.
  bool validate(const ::substrait::FetchRel& fetchRel);

  /// Used to validate whether the computing of this TopN is supported.
  bool validate(const ::substrait::TopNRel& topNRel);

  /// Used to validate whether the computing of this Expand is supported.
  bool validate(const ::substrait::ExpandRel& expandRel);

  /// Used to validate whether the computing of this Generate is supported.
  bool validate(const ::substrait::GenerateRel& generateRel);

  /// Used to validate whether the computing of this Sort is supported.
  bool validate(const ::substrait::SortRel& sortRel);

  /// Used to validate whether the computing of this Window is supported.
  bool validate(const ::substrait::WindowRel& windowRel);

  /// Used to validate whether the computing of this WindowGroupLimit is supported.
  bool validate(const ::substrait::WindowGroupLimitRel& windowGroupLimitRel);

  /// Used to validate whether the computing of this Set is supported.
  bool validate(const ::substrait::SetRel& setRel);

  /// Used to validate whether the computing of this Aggregation is supported.
  bool validate(const ::substrait::AggregateRel& aggRel);

  /// Used to validate whether the computing of this Project is supported.
  bool validate(const ::substrait::ProjectRel& projectRel);

  /// Used to validate whether the computing of this Filter is supported.
  bool validate(const ::substrait::FilterRel& filterRel);

  /// Used to validate Join.
  bool validate(const ::substrait::JoinRel& joinRel);

  /// Used to validate Cartesian product.
  bool validate(const ::substrait::CrossRel& crossRel);

  /// Used to validate whether the computing of this Read is supported.
  bool validate(const ::substrait::ReadRel& readRel);

  /// Used to validate whether the computing of this Rel is supported.
  bool validate(const ::substrait::Rel& rel);

  /// Used to validate whether the computing of this RelRoot is supported.
  bool validate(const ::substrait::RelRoot& relRoot);

  std::shared_ptr<velox::core::QueryCtx> queryCtx_;

  /// An execution context used for function validation.
  std::unique_ptr<core::ExecCtx> execCtx_;

  std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg_{nullptr};

  /// A converter used to convert Substrait plan into Velox's plan node.
  std::unique_ptr<SubstraitToVeloxPlanConverter> planConverter_{nullptr};

  /// An expression converter used to convert Substrait representations into
  /// Velox expressions.
  SubstraitVeloxExprConverter* exprConverter_ = nullptr;

  std::vector<std::string> validateLog_;

  /// Used to get types from advanced extension and validate them, then convert to a Velox type that has arbitrary
  /// levels of nesting.
  bool parseVeloxType(const ::substrait::extensions::AdvancedExtension& extension, TypePtr& out);

  /// Flattens a Velox type with single level of nesting into a std::vector of child types.
  bool flattenSingleLevel(const TypePtr& type, std::vector<TypePtr>& out);

  /// Flattens a Velox type with two level of nesting into a dual-nested std::vector of child types.
  bool flattenDualLevel(const TypePtr& type, std::vector<std::vector<TypePtr>>& out);

  /// Validate aggregate rel.
  bool validateAggRelFunctionType(const ::substrait::AggregateRel& substraitAgg);

  /// Validate the round scalar function.
  bool validateRound(const ::substrait::Expression::ScalarFunction& scalarFunction, const RowTypePtr& inputType);

  /// Validate extract function.
  bool validateExtractExpr(const std::vector<core::TypedExprPtr>& params);

  /// Validates regex functions.
  /// Ensures the second pattern argument is a literal string.
  /// Check if the pattern can pass with RE2 compilation.
  bool validateRegexExpr(const std::string& name, const ::substrait::Expression::ScalarFunction& scalarFunction);

  /// Validate Substrait scarlar function.
  bool validateScalarFunction(
      const ::substrait::Expression::ScalarFunction& scalarFunction,
      const RowTypePtr& inputType);

  /// Validate Substrait Cast expression.
  bool validateCast(const ::substrait::Expression::Cast& castExpr, const RowTypePtr& inputType);

  /// Validate Substrait expression.
  bool validateExpression(const ::substrait::Expression& expression, const RowTypePtr& inputType);

  /// Validate Substrait if-then expression.
  bool validateIfThen(const ::substrait::Expression_IfThen& ifThen, const RowTypePtr& inputType);

  /// Validate Substrait IN expression.
  bool validateSingularOrList(
      const ::substrait::Expression::SingularOrList& singularOrList,
      const RowTypePtr& inputType);

  /// Add necessary log for fallback
  void logValidateMsg(const std::string& log) {
    validateLog_.emplace_back(log);
  }

  bool isAllowedCast(const TypePtr& fromType, const TypePtr& toType);
};

} // namespace gluten

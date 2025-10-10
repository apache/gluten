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

#include "bolt/core/PlanNode.h"

#include "SubstraitExtensionCollector.h"
#include "BoltToSubstraitType.h"
#include "substrait/algebra.pb.h"
#include "bolt/vector/ConstantVector.h"

namespace gluten {

class BoltToSubstraitExprConvertor {
 public:
  explicit BoltToSubstraitExprConvertor(const SubstraitExtensionCollectorPtr& extensionCollector)
      : extensionCollector_(extensionCollector) {}

  /// Convert Bolt Expression to Substrait Expression.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @param expr Bolt expression needed to be converted.
  /// @param inputType The input row Type of the current processed node,
  /// which also equals the output row type of the previous node of the current.
  /// @return A pointer to Substrait expression object allocated on the arena
  /// and representing the input Bolt expression.
  const ::substrait::Expression&
  toSubstraitExpr(google::protobuf::Arena& arena, const core::TypedExprPtr& expr, const RowTypePtr& inputType);

  /// Convert Bolt Constant Expression to Substrait
  /// Literal Expression.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @param constExpr Bolt Constant expression needed to be converted.
  /// @param litValue The Struct that returned literal expression belong to.
  /// @return A pointer to Substrait Literal expression object allocated on
  /// the arena and representing the input Bolt Constant expression.
  const ::substrait::Expression_Literal& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::ConstantTypedExpr>& constExpr,
      ::substrait::Expression_Literal_Struct* litValue = nullptr);

  /// Convert Bolt FieldAccessTypedExpr to Substrait FieldReference Expression.
  const ::substrait::Expression_FieldReference& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::FieldAccessTypedExpr>& fieldExpr,
      const RowTypePtr& inputType);

  const ::substrait::Expression_FieldReference& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::DereferenceTypedExpr>& derefExpr,
      const RowTypePtr& inputType);

  /// Convert Bolt vector to Substrait literal.
  const ::substrait::Expression_Literal& toSubstraitLiteral(
      google::protobuf::Arena& arena,
      const bolt::VectorPtr& vectorValue,
      ::substrait::Expression_Literal_Struct* litValue);

 private:
  /// Convert Bolt Cast Expression to Substrait Cast Expression.
  const ::substrait::Expression_Cast& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::CastTypedExpr>& castExpr,
      const RowTypePtr& inputType);

  /// Convert Bolt CallTypedExpr Expression to Substrait Expression.
  const ::substrait::Expression& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::CallTypedExpr>& callTypeExpr,
      const RowTypePtr& inputType);

  /// Convert Bolt variant to Substrait Literal Expression.
  const ::substrait::Expression_Literal& toSubstraitLiteral(
      google::protobuf::Arena& arena,
      const bolt::variant& variantValue);

  /// Convert values in Bolt array vector to Substrait Literal List.
  const ::substrait::Expression_Literal_List&
  toSubstraitLiteralList(google::protobuf::Arena& arena, const ArrayVector* arrayVector, vector_size_t row);

  /// Convert an empty Bolt array vector to Substrait Literal Empty List.
  const ::substrait::Type_List& toSubstraitLiteralEmptyList(google::protobuf::Arena& arena, const bolt::TypePtr& type);

  /// Convert values in Bolt complex vector to Substrait Literal.
  const ::substrait::Expression_Literal& toSubstraitLiteralComplex(
      google::protobuf::Arena& arena,
      const std::shared_ptr<ConstantVector<ComplexType>>& constantVector);

  BoltToSubstraitTypeConvertorPtr typeConvertor_;

  SubstraitExtensionCollectorPtr extensionCollector_;
};

} // namespace gluten

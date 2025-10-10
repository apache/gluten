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

#include "SubstraitToBoltExpr.h"

#include "TypeUtils.h"
#include "utils/ConfigExtractor.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VariantToVector.h"
#include "bolt/type/Timestamp.h"

using namespace bytedance::bolt;

namespace {
ArrayVectorPtr makeArrayVector(const VectorPtr& elements) {
  BufferPtr offsets = allocateOffsets(1, elements->pool());
  BufferPtr sizes = allocateOffsets(1, elements->pool());
  sizes->asMutable<vector_size_t>()[0] = elements->size();

  return std::make_shared<ArrayVector>(elements->pool(), ARRAY(elements->type()), nullptr, 1, offsets, sizes, elements);
}

MapVectorPtr makeMapVector(const VectorPtr& keyVector, const VectorPtr& valueVector) {
  BufferPtr offsets = allocateOffsets(1, keyVector->pool());
  BufferPtr sizes = allocateOffsets(1, keyVector->pool());
  sizes->asMutable<vector_size_t>()[0] = keyVector->size();

  return std::make_shared<MapVector>(
      keyVector->pool(),
      MAP(keyVector->type(), valueVector->type()),
      nullptr,
      1,
      offsets,
      sizes,
      keyVector,
      valueVector);
}

RowVectorPtr makeRowVector(
    const std::vector<VectorPtr>& children,
    std::vector<std::string>&& names,
    size_t length,
    bytedance::bolt::memory::MemoryPool* pool) {
  std::vector<std::shared_ptr<const Type>> types;
  types.resize(children.size());
  for (int i = 0; i < children.size(); i++) {
    types[i] = children[i]->type();
  }
  auto rowType = ROW(std::move(names), std::move(types));
  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), length, children);
}

ArrayVectorPtr makeEmptyArrayVector(memory::MemoryPool* pool, const TypePtr& elementType) {
  BufferPtr offsets = allocateOffsets(1, pool);
  BufferPtr sizes = allocateOffsets(1, pool);
  return std::make_shared<ArrayVector>(pool, ARRAY(elementType), nullptr, 1, offsets, sizes, nullptr);
}

MapVectorPtr makeEmptyMapVector(memory::MemoryPool* pool, const TypePtr& keyType, const TypePtr& valueType) {
  BufferPtr offsets = allocateOffsets(1, pool);
  BufferPtr sizes = allocateOffsets(1, pool);
  return std::make_shared<MapVector>(pool, MAP(keyType, valueType), nullptr, 1, offsets, sizes, nullptr, nullptr);
}

RowVectorPtr makeEmptyRowVector(memory::MemoryPool* pool) {
  return makeRowVector({}, {}, 0, pool);
}

template <typename T>
void setLiteralValue(const ::substrait::Expression::Literal& literal, FlatVector<T>* vector, vector_size_t index) {
  if (literal.has_null()) {
    vector->setNull(index, true);
  } else {
    vector->set(index, gluten::SubstraitParser::getLiteralValue<T>(literal));
  }
}

template <TypeKind kind>
VectorPtr constructFlatVector(
    std::function<::substrait::Expression::Literal(vector_size_t /* idx */)> elementAt,
    const vector_size_t size,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  BOLT_CHECK(type->isPrimitiveType());
  auto vector = BaseVector::create(type, size, pool);
  using T = typename TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<T>>();

  for (int i = 0; i < size; i++) {
    auto element = elementAt(i);
    setLiteralValue(element, flatVector, i);
  }
  return vector;
}

TypePtr getScalarType(const ::substrait::Expression::Literal& literal) {
  auto typeCase = literal.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return BOOLEAN();
    case ::substrait::Expression_Literal::LiteralTypeCase::kI8:
      return TINYINT();
    case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
      return SMALLINT();
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return INTEGER();
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return BIGINT();
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
      return REAL();
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return DOUBLE();
    case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      auto precision = literal.decimal().precision();
      auto scale = literal.decimal().scale();
      auto type = DECIMAL(precision, scale);
      return type;
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
      return DATE();
    case ::substrait::Expression_Literal::LiteralTypeCase::kTimestampTz:
      return TIMESTAMP();
    case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
      return TIMESTAMP();
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
      return VARCHAR();
    case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
      return VARCHAR();
    case ::substrait::Expression_Literal::LiteralTypeCase::kBinary:
      return VARBINARY();
    case ::substrait::Expression_Literal::LiteralTypeCase::kIntervalYearToMonth:
      return INTERVAL_YEAR_MONTH();
    default:
      return nullptr;
  }
}

/// Whether is try cast.
bool isTryCast(::substrait::Expression::Cast::FailureBehavior failureBehavior) {
  switch (failureBehavior) {
    case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_UNSPECIFIED:
    case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION:
      return false;
    case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL:
      return true;
    default:
      BOLT_NYI("The given failure behavior is NOT supported: '{}'", std::to_string(failureBehavior));
  }
}

template <TypeKind kind>
VectorPtr constructFlatVectorForStruct(
    const ::substrait::Expression::Literal& child,
    const vector_size_t size,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  BOLT_CHECK(type->isPrimitiveType());
  auto vector = BaseVector::create(type, size, pool);
  using T = typename TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<T>>();
  setLiteralValue(child, flatVector, 0);
  return vector;
}

template <TypeKind kind>
std::shared_ptr<core::ConstantTypedExpr> constructConstantVector(
    const ::substrait::Expression::Literal& substraitLit,
    const TypePtr& type) {
  BOLT_CHECK(type->isPrimitiveType());
  if (substraitLit.has_binary()) {
    return std::make_shared<core::ConstantTypedExpr>(
        type, variant::binary(gluten::SubstraitParser::getLiteralValue<StringView>(substraitLit)));
  } else {
    using T = typename TypeTraits<kind>::NativeType;
    return std::make_shared<core::ConstantTypedExpr>(
        type, variant(gluten::SubstraitParser::getLiteralValue<T>(substraitLit)));
  }
}

core::FieldAccessTypedExprPtr
makeFieldAccessExpr(const std::string& name, const TypePtr& type, core::FieldAccessTypedExprPtr input) {
  if (input) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, input, name);
  }

  return std::make_shared<core::FieldAccessTypedExpr>(type, name);
}

} // namespace


namespace gluten {


SubstraitBoltExprConverter::SubstraitBoltExprConverter(
      memory::MemoryPool* pool,
      const std::unordered_map<uint64_t, std::string>& functionMap,
      const std::unordered_map<std::string, std::string>& confMap)
      : pool_(pool), functionMap_(functionMap), confMap_(confMap) {
    useIcuRegex_ = useIcuRegex(confMap);
}

std::shared_ptr<const core::FieldAccessTypedExpr> SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::FieldReference& substraitField,
    const RowTypePtr& inputType) {
  auto typeCase = substraitField.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::FieldReference::ReferenceTypeCase::kDirectReference: {
      const auto& directRef = substraitField.direct_reference();
      core::FieldAccessTypedExprPtr fieldAccess{nullptr};
      const auto* tmp = &directRef.struct_field();

      auto inputColumnType = inputType;
      for (;;) {
        auto idx = tmp->field();
        fieldAccess = makeFieldAccessExpr(inputColumnType->nameOf(idx), inputColumnType->childAt(idx), fieldAccess);

        if (!tmp->has_child()) {
          break;
        }

        inputColumnType = asRowType(inputColumnType->childAt(idx));
        tmp = &tmp->child().struct_field();
      }
      return fieldAccess;
    }
    default:
      BOLT_NYI("Substrait conversion not supported for Reference '{}'", std::to_string(typeCase));
  }
}

core::TypedExprPtr SubstraitBoltExprConverter::toExtractExpr(
    const std::vector<core::TypedExprPtr>& params,
    const TypePtr& outputType) {
  BOLT_CHECK_EQ(params.size(), 2);
  auto functionArg = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(params[0]);
  if (functionArg) {
    // Get the function argument.
    auto variant = functionArg->value();
    if (!variant.hasValue()) {
      BOLT_FAIL("Value expected in variant.");
    }
    // The first parameter specifies extracting from which field.
    std::string from = variant.value<std::string>();

    // The second parameter is the function parameter.
    std::vector<core::TypedExprPtr> exprParams;
    exprParams.reserve(1);
    exprParams.emplace_back(params[1]);
    auto iter = extractDatetimeFunctionMap_.find(from);
    if (iter != extractDatetimeFunctionMap_.end()) {
      return std::make_shared<const core::CallTypedExpr>(outputType, std::move(exprParams), iter->second);
    } else {
      BOLT_NYI("Extract from {} not supported.", from);
    }
  }
  BOLT_FAIL("Constant is expected to be the first parameter in extract.");
}

core::TypedExprPtr SubstraitBoltExprConverter::toLambdaExpr(
    const ::substrait::Expression::ScalarFunction& substraitFunc,
    const RowTypePtr& inputType) {
  // Arguments names and types.
  std::vector<std::string> argumentNames;
  BOLT_CHECK_GT(substraitFunc.arguments().size(), 1, "lambda should have at least 2 args.");
  argumentNames.reserve(substraitFunc.arguments().size() - 1);
  std::vector<TypePtr> argumentTypes;
  argumentTypes.reserve(substraitFunc.arguments().size() - 1);
  for (int i = 1; i < substraitFunc.arguments().size(); i++) {
    auto arg = substraitFunc.arguments(i).value();
    CHECK(arg.has_scalar_function());
    const auto& boltFunction =
        SubstraitParser::findBoltFunction(functionMap_, arg.scalar_function().function_reference(), useIcuRegex_);
    CHECK_EQ(boltFunction, "namedlambdavariable");
    argumentNames.emplace_back(arg.scalar_function().arguments(0).value().literal().string());
    argumentTypes.emplace_back(SubstraitParser::parseType(arg.scalar_function().output_type()));
  }
  auto rowType = ROW(std::move(argumentNames), std::move(argumentTypes));
  // Arg[0] -> function.
  auto lambda =
      std::make_shared<core::LambdaTypedExpr>(rowType, toBoltExpr(substraitFunc.arguments(0).value(), inputType));
  return lambda;
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::ScalarFunction& substraitFunc,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> params;
  params.reserve(substraitFunc.arguments().size());
  for (const auto& sArg : substraitFunc.arguments()) {
    params.emplace_back(toBoltExpr(sArg.value(), inputType));
  }
  const auto& boltFunction = SubstraitParser::findBoltFunction(functionMap_, substraitFunc.function_reference(), useIcuRegex_);
  const auto& outputType = SubstraitParser::parseType(substraitFunc.output_type());

  if (boltFunction == "lambdafunction") {
    return toLambdaExpr(substraitFunc, inputType);
  }
  if (boltFunction == "namedlambdavariable") {
    return makeFieldAccessExpr(substraitFunc.arguments(0).value().literal().string(), outputType, nullptr);
  }
  if (boltFunction == "extract") {
    return toExtractExpr(std::move(params), outputType);
  } else {
    return std::make_shared<const core::CallTypedExpr>(outputType, std::move(params), boltFunction);
  }
}

std::shared_ptr<const core::ConstantTypedExpr> SubstraitBoltExprConverter::literalsToConstantExpr(
    const std::vector<::substrait::Expression::Literal>& literals) {
  std::vector<variant> variants;
  variants.reserve(literals.size());
  BOLT_CHECK_GE(literals.size(), 0, "List should have at least one item.");
  std::optional<TypePtr> literalType;
  for (const auto& literal : literals) {
    auto boltVariant = toBoltExpr(literal);
    if (!literalType.has_value()) {
      literalType = boltVariant->type();
    }
    variants.emplace_back(boltVariant->value());
  }
  BOLT_CHECK(literalType.has_value(), "Type expected.");
  auto varArray = variant::array(variants);
  ArrayVectorPtr arrayVector = bytedance::bolt::core::variantArrayToVector(ARRAY(literalType.value()), varArray.array(), pool_);
  // Wrap the array vector into constant vector.
  auto constantVector = BaseVector::wrapInConstant(1 /*length*/, 0 /*index*/, arrayVector);
  return std::make_shared<const core::ConstantTypedExpr>(constantVector);
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::SingularOrList& singularOrList,
    const RowTypePtr& inputType) {
  BOLT_CHECK(singularOrList.options_size() > 0, "At least one option is expected.");
  auto options = singularOrList.options();
  std::vector<::substrait::Expression::Literal> literals;
  literals.reserve(options.size());
  for (const auto& option : options) {
    BOLT_CHECK(option.has_literal(), "Option is expected as Literal.");
    literals.emplace_back(option.literal());
  }

  std::vector<core::TypedExprPtr> params;
  params.reserve(2);
  // First param is the value, second param is the list.
  params.emplace_back(toBoltExpr(singularOrList.value(), inputType));
  params.emplace_back(literalsToConstantExpr(literals));
  return std::make_shared<const core::CallTypedExpr>(BOOLEAN(), std::move(params), "in");
}

std::shared_ptr<const core::ConstantTypedExpr> SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::Literal& substraitLit) {
  auto typeCase = substraitLit.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kList: {
      auto constantVector = BaseVector::wrapInConstant(1, 0, literalsToArrayVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kEmptyList: {
      auto elementType = SubstraitParser::parseType(substraitLit.empty_list().type());
      auto constantVector = BaseVector::wrapInConstant(1, 0, makeEmptyArrayVector(pool_, elementType));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kMap: {
      auto constantVector = BaseVector::wrapInConstant(1, 0, literalsToMapVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kEmptyMap: {
      auto keyType = SubstraitParser::parseType(substraitLit.empty_map().key());
      auto valueType = SubstraitParser::parseType(substraitLit.empty_map().value());
      auto constantVector = BaseVector::wrapInConstant(1, 0, makeEmptyMapVector(pool_, keyType, valueType));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kStruct: {
      auto constantVector = BaseVector::wrapInConstant(1, 0, literalsToRowVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto boltType = SubstraitParser::parseType(substraitLit.null());
      if (boltType->isShortDecimal()) {
        return std::make_shared<core::ConstantTypedExpr>(boltType, variant::null(TypeKind::BIGINT));
      }
      if (boltType->isLongDecimal()) {
        return std::make_shared<core::ConstantTypedExpr>(boltType, variant::null(TypeKind::HUGEINT));
      }
      return std::make_shared<core::ConstantTypedExpr>(boltType, variant::null(boltType->kind()));
    }
    default:
      auto boltType = getScalarType(substraitLit);
      if (boltType) {
        auto kind = boltType->kind();
        return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(constructConstantVector, kind, substraitLit, boltType);
      }
      BOLT_NYI("Substrait conversion not supported for type case '{}'", std::to_string(typeCase));
  }
}

ArrayVectorPtr SubstraitBoltExprConverter::literalsToArrayVector(const ::substrait::Expression::Literal& literal) {
  auto childSize = literal.list().values().size();
  BOLT_CHECK_GT(childSize, 0, "there should be at least 1 value in list literal.");
  auto childLiteral = literal.list().values(0);
  auto elementAtFunc = [&](vector_size_t idx) { return literal.list().values(idx); };
  auto childVector = literalsToVector(childLiteral, childSize, elementAtFunc);
  return makeArrayVector(childVector);
}

MapVectorPtr SubstraitBoltExprConverter::literalsToMapVector(const ::substrait::Expression::Literal& literal) {
  auto childSize = literal.map().key_values().size();
  BOLT_CHECK_GT(childSize, 0, "there should be at least 1 value in map literal.");
  auto& keyLiteral = literal.map().key_values(0).key();
  auto& valueLiteral = literal.map().key_values(0).value();
  auto keyAtFunc = [&](vector_size_t idx) { return literal.map().key_values(idx).key(); };
  auto valueAtFunc = [&](vector_size_t idx) { return literal.map().key_values(idx).value(); };
  auto keyVector = literalsToVector(keyLiteral, childSize, keyAtFunc);
  auto valueVector = literalsToVector(valueLiteral, childSize, valueAtFunc);
  return makeMapVector(keyVector, valueVector);
}

VectorPtr SubstraitBoltExprConverter::literalsToVector(
    const ::substrait::Expression::Literal& childLiteral,
    vector_size_t childSize,
    std::function<::substrait::Expression::Literal(vector_size_t /* idx */)> elementAtFunc) {
  auto childTypeCase = childLiteral.literal_type_case();
  switch (childTypeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto boltType = SubstraitParser::parseType(childLiteral.null());
      auto kind = boltType->kind();
      return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          constructFlatVector, kind, elementAtFunc, childSize, boltType, pool_);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond:
      return constructFlatVector<TypeKind::BIGINT>(elementAtFunc, childSize, INTERVAL_DAY_TIME(), pool_);
    // Handle EmptyList and List together since the children could be either case.
    case ::substrait::Expression_Literal::LiteralTypeCase::kEmptyList:
    case ::substrait::Expression_Literal::LiteralTypeCase::kList: {
      ArrayVectorPtr elements;
      for (int i = 0; i < childSize; i++) {
        auto child = elementAtFunc(i);
        auto childType = child.literal_type_case();
        ArrayVectorPtr grandVector;

        if (childType == ::substrait::Expression_Literal::LiteralTypeCase::kEmptyList) {
          auto elementType = SubstraitParser::parseType(child.empty_list().type());
          grandVector = makeEmptyArrayVector(pool_, elementType);
        } else {
          grandVector = literalsToArrayVector(child);
        }
        if (!elements) {
          elements = grandVector;
        } else {
          elements->append(grandVector.get());
        }
      }
      return elements;
    }
    // Handle EmptyMap and Map together since the children could be either case.
    case ::substrait::Expression_Literal::LiteralTypeCase::kEmptyMap:
    case ::substrait::Expression_Literal::LiteralTypeCase::kMap: {
      MapVectorPtr mapVector;
      for (int i = 0; i < childSize; i++) {
        auto child = elementAtFunc(i);
        auto childType = child.literal_type_case();
        MapVectorPtr grandVector;

        if (childType == ::substrait::Expression_Literal::LiteralTypeCase::kEmptyMap) {
          auto keyType = SubstraitParser::parseType(child.empty_map().key());
          auto valueType = SubstraitParser::parseType(child.empty_map().value());
          grandVector = makeEmptyMapVector(pool_, keyType, valueType);
        } else {
          grandVector = literalsToMapVector(child);
        }
        if (!mapVector) {
          mapVector = grandVector;
        } else {
          mapVector->append(grandVector.get());
        }
      }
      return mapVector;
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kStruct: {
      RowVectorPtr rowVector;
      for (int i = 0; i < childSize; i++) {
        auto element = elementAtFunc(i);
        RowVectorPtr grandVector = literalsToRowVector(element);
        if (!rowVector) {
          rowVector = grandVector;
        } else {
          rowVector->append(grandVector.get());
        }
      }
      return rowVector;
    }
    default:
      auto boltType = getScalarType(elementAtFunc(0));
      if (boltType) {
        auto kind = boltType->kind();
        return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            constructFlatVector, kind, elementAtFunc, childSize, boltType, pool_);
      }
      BOLT_NYI("literals not supported for type case '{}'", std::to_string(childTypeCase));
  }
}

RowVectorPtr SubstraitBoltExprConverter::literalsToRowVector(const ::substrait::Expression::Literal& structLiteral) {
  if (structLiteral.has_null()) {
    BOLT_NYI("NULL for struct type is not supported.");
  }
  auto numFields = structLiteral.struct_().fields().size();
  if (numFields == 0) {
    return makeEmptyRowVector(pool_);
  }
  std::vector<VectorPtr> vectors;
  std::vector<std::string> names;
  vectors.reserve(numFields);
  names.reserve(numFields);
  for (auto i = 0; i < numFields; ++i) {
    const auto& child = structLiteral.struct_().fields(i);
    const auto& name = structLiteral.struct_().names(i);
    names.push_back(name);
    auto typeCase = child.literal_type_case();
    switch (typeCase) {
      case ::substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond: {
        vectors.emplace_back(constructFlatVectorForStruct<TypeKind::BIGINT>(child, 1, INTERVAL_DAY_TIME(), pool_));
        break;
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
        auto boltType = SubstraitParser::parseType(child.null());
        auto kind = boltType->kind();
        auto vecPtr =
            BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(constructFlatVectorForStruct, kind, child, 1, boltType, pool_);
        vectors.emplace_back(vecPtr);
        break;
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kList: {
        vectors.emplace_back(literalsToArrayVector(child));
        break;
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kMap: {
        vectors.emplace_back(literalsToMapVector(child));
        break;
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kStruct: {
        vectors.emplace_back(literalsToRowVector(child));
        break;
      }
      default:
        auto boltType = getScalarType(child);
        if (boltType) {
          auto kind = boltType->kind();
          auto vecPtr =
              BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(constructFlatVectorForStruct, kind, child, 1, boltType, pool_);
          vectors.emplace_back(vecPtr);
        } else {
          BOLT_NYI("literalsToRowVector not supported for type case '{}'", std::to_string(typeCase));
        }
    }
  }
  return makeRowVector(vectors, std::move(names), 1, pool_);
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::Cast& castExpr,
    const RowTypePtr& inputType) {
  auto type = SubstraitParser::parseType(castExpr.type());
  std::vector<core::TypedExprPtr> inputs{toBoltExpr(castExpr.input(), inputType)};
  return std::make_shared<core::CastTypedExpr>(type, inputs, isTryCast(castExpr.failure_behavior()));
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression::IfThen& ifThenExpr,
    const RowTypePtr& inputType) {
  BOLT_CHECK(ifThenExpr.ifs().size() > 0, "If clause expected.");

  // Params are concatenated conditions and results with an optional "else" at
  // the end, e.g. {condition1, result1, condition2, result2,..else}
  std::vector<core::TypedExprPtr> params;
  // If and then expressions are in pairs.
  params.reserve(ifThenExpr.ifs().size() * 2);
  std::optional<TypePtr> outputType;
  for (const auto& ifThen : ifThenExpr.ifs()) {
    params.emplace_back(toBoltExpr(ifThen.if_(), inputType));
    const auto& thenExpr = toBoltExpr(ifThen.then(), inputType);
    // Get output type from the first then expression.
    if (!outputType.has_value()) {
      outputType = thenExpr->type();
    }
    params.emplace_back(thenExpr);
  }

  if (ifThenExpr.has_else_()) {
    params.reserve(1);
    params.emplace_back(toBoltExpr(ifThenExpr.else_(), inputType));
  }

  BOLT_CHECK(outputType.has_value(), "Output type should be set.");
  if (ifThenExpr.ifs().size() == 1) {
    // If there is only one if-then clause, use if expression.
    return std::make_shared<const core::CallTypedExpr>(outputType.value(), std::move(params), "if");
  }
  return std::make_shared<const core::CallTypedExpr>(outputType.value(), std::move(params), "switch");
}

core::TypedExprPtr SubstraitBoltExprConverter::toBoltExpr(
    const ::substrait::Expression& substraitExpr,
    const RowTypePtr& inputType) {
  auto typeCase = substraitExpr.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kLiteral:
      return toBoltExpr(substraitExpr.literal());
    case ::substrait::Expression::RexTypeCase::kScalarFunction:
      return toBoltExpr(substraitExpr.scalar_function(), inputType);
    case ::substrait::Expression::RexTypeCase::kSelection:
      return toBoltExpr(substraitExpr.selection(), inputType);
    case ::substrait::Expression::RexTypeCase::kCast:
      return toBoltExpr(substraitExpr.cast(), inputType);
    case ::substrait::Expression::RexTypeCase::kIfThen:
      return toBoltExpr(substraitExpr.if_then(), inputType);
    case ::substrait::Expression::RexTypeCase::kSingularOrList:
      return toBoltExpr(substraitExpr.singular_or_list(), inputType);
    default:
      BOLT_NYI("Substrait conversion not supported for Expression '{}'", std::to_string(typeCase));
  }
}

std::unordered_map<std::string, std::string> SubstraitBoltExprConverter::extractDatetimeFunctionMap_ = {
    {"MILLISECOND", "millisecond"},
    {"SECOND", "second"},
    {"MINUTE", "minute"},
    {"HOUR", "hour"},
    {"DAY", "day"},
    {"DAY_OF_WEEK", "dayofweek"},
    {"WEEK_DAY", "weekday"},
    {"DAY_OF_YEAR", "dayofyear"},
    {"MONTH", "month"},
    {"QUARTER", "quarter"},
    {"YEAR", "year"},
    {"WEEK_OF_YEAR", "week_of_year"},
    {"YEAR_OF_WEEK", "year_of_week"}};

} // namespace gluten

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
package org.apache.gluten.rexnode.functions;

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.TypeUtils;
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.List;
import java.util.stream.Collectors;

abstract class BaseRexCallConverter implements RexCallConverter {
  protected final String functionName;

  public BaseRexCallConverter(String functionName) {
    this.functionName = functionName;
  }

  protected List<TypedExpr> getParams(RexCall callNode, RexConversionContext context) {
    return RexNodeConverter.toTypedExpr(callNode.getOperands(), context);
  }

  protected Type getResultType(RexCall callNode) {
    return RexNodeConverter.toType(callNode.getType());
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    // Default implementation assumes all RexCall nodes are supported.
    // Subclasses can override this method to provide specific support checks.
    return ValidationResult.success();
  }

  protected String getFunctionProtoTypeName(RexCall callNode) {
    String operandTypeNames =
        callNode.getOperands().stream()
            .map(arg -> arg.getType().toString())
            .collect(Collectors.joining(", "));
    return String.format("(%s) -> %s", operandTypeNames, callNode.getType().toString());
  }
}

class DefaultRexCallConverter extends BaseRexCallConverter {
  public DefaultRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);

    // TODO: cast don't support input and result has same type. Refine it.
    if ("cast".equals(functionName)) {
      TypedExpr sourceExpr = params.get(0);
      Type sourceType = sourceExpr.getReturnType();

      if (sourceType instanceof TimestampType && resultType instanceof TimestampType) {
        return sourceExpr;
      }
    } else if ("unix_timestamp".equals(functionName)) {
      // TODO: this is a trick here for q12, refine it.
      resultType = new BigIntType();
    }
    return new CallTypedExpr(resultType, params, functionName);
  }
}

/**
 * Converter for the one-operand "-" RexCall (Flink's minusPrefix / SQL -x). The name maps to the
 * flinksql negate function; the result type equals the operand type, so no cast is needed.
 */
class UnaryMinusRexCallConverter extends BaseRexCallConverter {
  public UnaryMinusRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 1) {
      return ValidationResult.failure(
          "Unary minus requires exactly one operand, but found: "
              + getFunctionProtoTypeName(callNode));
    }
    // Numeric operand only: the interval form of unary minus would
    // otherwise be accepted by the arity guard and fail later at
    // toType (no interval type mapping); reject it at planning time.
    try {
      if (!org.apache.gluten.rexnode.TypeUtils.isNumericType(
          org.apache.gluten.rexnode.RexNodeConverter.toType(
              callNode.getOperands().get(0).getType()))) {
        return ValidationResult.failure(
            "Unary minus requires a numeric operand, but found: "
                + getFunctionProtoTypeName(callNode));
      }
    } catch (RuntimeException e) {
      // Operand types without a velox mapping (e.g. intervals) are
      // rejected at planning time rather than blowing up in toType.
      return ValidationResult.failure(
          "Unary minus requires a numeric operand, but found: "
              + getFunctionProtoTypeName(callNode));
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);
    return new CallTypedExpr(resultType, params, functionName);
  }
}

/**
 * Converter for Flink functions whose decimal result type differs from the operand type by a
 * value-dependent precision/scale change (e.g. ROUND(x, d) narrows the scale to d). The velox-side
 * function stays same-typed (its result type cannot depend on an argument value), so the plan emits
 * the same-typed call followed by a cast to Flink's result type. Rounding at the original scale
 * leaves only zero digits below the target scale, so the cast is an exact rescale with no double
 * rounding.
 */
class DecimalPostCastRexCallConverter extends BaseRexCallConverter {
  public DecimalPostCastRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);
    if (resultType instanceof DecimalType && !callNode.getOperands().isEmpty()) {
      Type inputType = RexNodeConverter.toType(callNode.getOperands().get(0).getType());
      if (!inputType.equals(resultType)) {
        TypedExpr sameTypeCall = new CallTypedExpr(inputType, params, functionName);
        return CastTypedExpr.create(resultType, sameTypeCall, false);
      }
    }
    return new CallTypedExpr(resultType, params, functionName);
  }
}

class BasicArithmeticOperatorRexCallConverter extends BaseRexCallConverter {
  public BasicArithmeticOperatorRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    boolean typesValidate =
        callNode.getOperands().stream()
            .allMatch(param -> TypeUtils.isNumericType(RexNodeConverter.toType(param.getType())));
    if (!typesValidate) {
      String message =
          String.format(
              "Arithmetic operation '%s' requires numeric operands, but found: %s",
              functionName, getFunctionProtoTypeName(callNode));
      return ValidationResult.failure(message);
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    // If types are different, align them
    List<TypedExpr> alignedParams =
        TypeUtils.promoteTypeForArithmeticExpressions(params.get(0), params.get(1));
    Type resultType = getResultType(callNode);
    return new CallTypedExpr(resultType, alignedParams, functionName);
  }
}

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
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

class DecimalArithmeticOperatorRexCallConverters extends BaseRexCallConverter {

  private boolean needAlignParameterTypes = false;

  public DecimalArithmeticOperatorRexCallConverters(String functionName) {
    super(functionName);
  }

  public DecimalArithmeticOperatorRexCallConverters(
      String functionName, boolean needAlignParameterTypes) {
    this(functionName);
    this.needAlignParameterTypes = needAlignParameterTypes;
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    // Binary arithmetic: a one-operand "-" is the unary minus handled by
    // UnaryMinusRexCallConverter.
    if (callNode.getOperands().size() != 2) {
      return ValidationResult.failure(
          "Decimal arithmetic requires exactly two operands, but found: "
              + getFunctionProtoTypeName(callNode));
    }
    Type resultType = getResultType(callNode);
    List<Type> operandsType =
        callNode.getOperands().stream()
            .map(RexNode::getType)
            .map(RexNodeConverter::toType)
            .collect(Collectors.toList());
    // Check if the callNode is a decimal operation.
    if (resultType instanceof DecimalType
        || operandsType.stream().anyMatch(type -> type instanceof DecimalType)) {
      return ValidationResult.success();
    }
    return ValidationResult.failure(
        String.format(
            "Decimal operation requires operands to be of decimal type, but found: %s",
            getFunctionProtoTypeName(callNode)));
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);

    List<TypedExpr> castedParams =
        params.stream()
            .map(param -> castExprToDecimalType(param, resultType))
            .collect(Collectors.toList());
    if (needAlignParameterTypes) {
      return new CallTypedExpr(resultType, TypeUtils.promoteTypes(castedParams), functionName);
    } else {
      return new CallTypedExpr(resultType, castedParams, functionName);
    }
  }
  // If the type is not decimal, convert it to decimal type.
  private TypedExpr castExprToDecimalType(TypedExpr expr, Type functionResultType) {
    Type returnType = expr.getReturnType();
    if (returnType instanceof IntegerType) {
      // Cast BigInt to DecimalType.
      return CastTypedExpr.create(new DecimalType(10, 0), expr, false);
    } else if (returnType instanceof BigIntType) {
      // Cast Integer to DecimalType
      return CastTypedExpr.create(new DecimalType(19, 0), expr, false);
    } else if (returnType instanceof DecimalType) {
      if (functionResultType instanceof DecimalType) {
        // If the return type is also DecimalType, no need to cast.
        return expr;
      } else if (functionResultType instanceof DoubleType) {
        // The result is of type double when a decimal is operated with a double.
        return CastTypedExpr.create(new DoubleType(), expr, false);
      } else if (functionResultType instanceof BooleanType) {
        // For comparison, the result type is Boolean
        return expr;
      }
      throw new FlinkRuntimeException(
          "Not supported type for decimal conversion: " + functionResultType.getClass().getName());
    } else if (returnType instanceof DoubleType) {
      return expr;
    } else {
      throw new FlinkRuntimeException(
          "Not supported type for decimal conversion: " + returnType.getClass().getName());
    }
  }
}

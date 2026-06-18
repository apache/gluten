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
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.List;
import java.util.stream.Collectors;

class SubtractRexCallConverter extends BaseRexCallConverter {

  public SubtractRexCallConverter() {
    super("subtract");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    // Subtraction operation is supported for numeric types.
    List<Type> paramTypes =
        callNode.getOperands().stream()
            .map(param -> RexNodeConverter.toType(param.getType()))
            .collect(Collectors.toList());
    boolean validate =
        callNode.getOperands().size() == 2
            && (paramTypes.stream().allMatch(TypeUtils::isNumericType)
                || (paramTypes.get(0) instanceof TimestampType
                    && paramTypes.get(1) instanceof BigIntType));
    if (!validate) {
      String message =
          String.format(
              "Subtraction operation requires exactly two numeric operands or timestamp - numeric, but found: %s",
              getFunctionProtoTypeName(callNode));
      return ValidationResult.failure(message);
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);

    if (params.get(0).getReturnType() instanceof TimestampType
        && params.get(1).getReturnType() instanceof BigIntType) {

      Type bigIntType = new BigIntType();
      TypedExpr castExpr = new CallTypedExpr(bigIntType, List.of(params.get(0)), "cast");

      List<TypedExpr> newParams = List.of(castExpr, params.get(1));
      return new CallTypedExpr(bigIntType, newParams, functionName);
    }

    List<TypedExpr> alignedParams =
        TypeUtils.promoteTypeForArithmeticExpressions(params.get(0), params.get(1));
    Type resultType = getResultType(callNode);
    return new CallTypedExpr(resultType, alignedParams, functionName);
  }
}

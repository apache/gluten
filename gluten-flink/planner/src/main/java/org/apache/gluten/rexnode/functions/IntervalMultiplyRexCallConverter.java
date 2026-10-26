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
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts interval * numeric (TIMESTAMPADD with a column amount rewrites to {@code ts + (interval
 * * n)}) to the velox flinksql multiply. The numeric operand is cast to BIGINT and the interval is
 * passed on the left.
 */
public class IntervalMultiplyRexCallConverter extends BaseRexCallConverter {

  public IntervalMultiplyRexCallConverter() {
    super("multiply");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 2) {
      return ValidationResult.failure(
          String.format(
              "Interval multiply expects 2 operands, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    List<Type> paramTypes =
        callNode.getOperands().stream()
            .map(param -> RexNodeConverter.toType(param.getType()))
            .collect(Collectors.toList());
    boolean firstInterval = IntervalArithmeticRexCallConverter.isInterval(paramTypes.get(0));
    boolean secondInterval = IntervalArithmeticRexCallConverter.isInterval(paramTypes.get(1));
    boolean suitable =
        (firstInterval && TypeUtils.isNumericType(paramTypes.get(1)))
            || (secondInterval && TypeUtils.isNumericType(paramTypes.get(0)));
    if (!suitable) {
      return ValidationResult.failure(
          String.format(
              "Interval multiply requires an interval and a numeric operand, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);
    // Normalize to (interval, BIGINT): the velox signatures take the numeric
    // operand as BIGINT and keep interval on the left.
    TypedExpr left = params.get(0);
    TypedExpr right = params.get(1);
    if (IntervalArithmeticRexCallConverter.isInterval(right.getReturnType())) {
      TypedExpr tmp = left;
      left = right;
      right = tmp;
    }
    if (!(right.getReturnType() instanceof BigIntType)) {
      right = CastTypedExpr.create(new BigIntType(), right, false);
    }
    return new CallTypedExpr(resultType, new ArrayList<>(List.of(left, right)), functionName);
  }
}

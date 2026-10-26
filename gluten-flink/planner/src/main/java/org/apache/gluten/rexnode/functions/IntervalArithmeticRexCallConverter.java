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
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.IntervalDayTimeType;
import io.github.zhztheplayer.velox4j.type.IntervalYearMonthType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts Flink timestamp/date +/- interval arithmetic to the velox flinksql plus/minus functions.
 * TIMESTAMPADD is rewritten by Calcite into this shape, so is handwritten {@code ts + INTERVAL 'n'
 * UNIT}.
 */
public class IntervalArithmeticRexCallConverter extends BaseRexCallConverter {

  // Whether the interval may appear on the left (plus is commutative, minus is not).
  private final boolean allowIntervalLeft;

  public IntervalArithmeticRexCallConverter(String functionName, boolean allowIntervalLeft) {
    super(functionName);
    this.allowIntervalLeft = allowIntervalLeft;
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 2) {
      return ValidationResult.failure(
          String.format(
              "Interval arithmetic expects 2 operands, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    List<Type> paramTypes =
        callNode.getOperands().stream()
            .map(param -> RexNodeConverter.toType(param.getType()))
            .collect(Collectors.toList());
    boolean firstDateTime = isDateTime(paramTypes.get(0));
    boolean secondDateTime = isDateTime(paramTypes.get(1));
    boolean firstInterval = isInterval(paramTypes.get(0));
    boolean secondInterval = isInterval(paramTypes.get(1));
    boolean suitable =
        (firstDateTime && secondInterval) || (allowIntervalLeft && firstInterval && secondDateTime);
    if (!suitable) {
      return ValidationResult.failure(
          String.format(
              "Interval arithmetic requires a timestamp/date and an interval operand, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    // Normalize to (datetime, interval): the velox signatures only register
    // the datetime-on-left order, so an interval-on-left plus is flipped
    // here (plus is commutative for these types).
    if (isInterval(params.get(0).getReturnType())) {
      params = List.of(params.get(1), params.get(0));
    }
    return new CallTypedExpr(getResultType(callNode), params, functionName);
  }

  static boolean isDateTime(Type type) {
    return type instanceof TimestampType || type instanceof DateType;
  }

  static boolean isInterval(Type type) {
    return type instanceof IntervalDayTimeType || type instanceof IntervalYearMonthType;
  }
}

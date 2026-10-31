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
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.IntervalDayTimeType;
import io.github.zhztheplayer.velox4j.type.IntervalYearMonthType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
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
    boolean firstInterval =
        isInterval(paramTypes.get(0)) || isIntervalOperand(callNode.getOperands().get(0));
    boolean secondInterval =
        isInterval(paramTypes.get(1)) || isIntervalOperand(callNode.getOperands().get(1));
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
    boolean firstIsInterval =
        isInterval(params.get(0).getReturnType())
            || isIntervalOperand(callNode.getOperands().get(0));
    if (firstIsInterval) {
      params = List.of(params.get(1), params.get(0));
    }

    // Re-type the interval operand from BigIntType to the proper interval
    // velox type. LogicalTypeConverter maps intervals to BigIntType globally;
    // we override here so velox resolves our flinksql signatures.
    RexNode intervalRexNode =
        firstIsInterval ? callNode.getOperands().get(0) : callNode.getOperands().get(1);
    TypedExpr intervalExpr = retypeInterval(params.get(1), intervalRexNode);
    List<TypedExpr> newParams = new ArrayList<>();
    newParams.add(params.get(0));
    newParams.add(intervalExpr);
    params = newParams;
    // Flink's TIMESTAMPADD with sub-day units on a DATE returns a TIMESTAMP
    // (e.g. TIMESTAMPADD(HOUR, 5, DATE '2025-03-15') = TIMESTAMP
    // '2025-03-15 05:00:00'). Velox only has plus(Date, IntervalDayTime) ->
    // Date, so when the expected result type is TIMESTAMP and the datetime
    // operand is a DATE, cast it to TIMESTAMP before the plus/minus call.
    // The cast is pure arithmetic (days * micros_per_day, no timezone) and
    // semantically equivalent to Flink's result.
    if (params.get(0).getReturnType() instanceof DateType
        && getResultType(callNode) instanceof TimestampType) {
      params =
          List.of(CastTypedExpr.create(new TimestampType(), params.get(0), false), params.get(1));
    }
    return new CallTypedExpr(getResultType(callNode), params, functionName);
  }

  static boolean isDateTime(Type type) {
    return type instanceof TimestampType || type instanceof DateType;
  }

  static boolean isInterval(Type type) {
    return type instanceof IntervalDayTimeType || type instanceof IntervalYearMonthType;
  }

  /** Checks the Calcite SqlTypeName for interval types (catches BigInt-mapped intervals). */
  static boolean isIntervalOperand(RexNode node) {
    SqlTypeName name = node.getType().getSqlTypeName();
    return name == SqlTypeName.INTERVAL_YEAR
        || name == SqlTypeName.INTERVAL_MONTH
        || name == SqlTypeName.INTERVAL_YEAR_MONTH
        || name == SqlTypeName.INTERVAL_DAY
        || name == SqlTypeName.INTERVAL_DAY_HOUR
        || name == SqlTypeName.INTERVAL_DAY_MINUTE
        || name == SqlTypeName.INTERVAL_DAY_SECOND
        || name == SqlTypeName.INTERVAL_HOUR
        || name == SqlTypeName.INTERVAL_HOUR_MINUTE
        || name == SqlTypeName.INTERVAL_HOUR_SECOND
        || name == SqlTypeName.INTERVAL_MINUTE
        || name == SqlTypeName.INTERVAL_MINUTE_SECOND
        || name == SqlTypeName.INTERVAL_SECOND;
  }

  static Type getIntervalVeloxType(RexNode node) {
    SqlTypeName name = node.getType().getSqlTypeName();
    if (name == SqlTypeName.INTERVAL_YEAR
        || name == SqlTypeName.INTERVAL_MONTH
        || name == SqlTypeName.INTERVAL_YEAR_MONTH) {
      return new IntervalYearMonthType();
    }
    return new IntervalDayTimeType();
  }

  static TypedExpr retypeInterval(TypedExpr converted, RexNode rexNode) {
    Type intervalType = getIntervalVeloxType(rexNode);
    if (converted.getReturnType().getClass().equals(intervalType.getClass())) {
      return converted;
    }
    if (rexNode instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) rexNode;
      if (intervalType instanceof IntervalYearMonthType) {
        int months = literal.getValueAs(java.math.BigDecimal.class).unscaledValue().intValueExact();
        return new ConstantTypedExpr(intervalType, new IntegerValue(months), null);
      } else {
        long millis =
            literal.getValueAs(java.math.BigDecimal.class).unscaledValue().longValueExact();
        return new ConstantTypedExpr(intervalType, new BigIntValue(millis), null);
      }
    }
    return CastTypedExpr.create(intervalType, converted, false);
  }
}

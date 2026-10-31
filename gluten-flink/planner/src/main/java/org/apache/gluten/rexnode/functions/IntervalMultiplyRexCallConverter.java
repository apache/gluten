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
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts interval * numeric (TIMESTAMPADD with a column amount rewrites to {@code ts + (interval
 * * n)}) to the velox flinksql multiply. The numeric operand is cast to BIGINT and the interval is
 * passed on the left. The output type is the interval velox type (IntervalDayTime /
 * IntervalYearMonth), not the BigIntType the global LogicalTypeConverter maps day-time intervals
 * to, so the result can feed the velox flinksql plus/minus signatures directly.
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
    // Day-time intervals map to BigIntType globally, so also check the
    // Calcite type to catch them.
    boolean firstInterval =
        IntervalArithmeticRexCallConverter.isInterval(paramTypes.get(0))
            || IntervalArithmeticRexCallConverter.isIntervalOperand(callNode.getOperands().get(0));
    boolean secondInterval =
        IntervalArithmeticRexCallConverter.isInterval(paramTypes.get(1))
            || IntervalArithmeticRexCallConverter.isIntervalOperand(callNode.getOperands().get(1));
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
    // Normalize to (interval, BIGINT): the velox signatures take the numeric
    // operand as BIGINT and keep interval on the left.
    boolean firstIsInterval =
        IntervalArithmeticRexCallConverter.isInterval(params.get(0).getReturnType())
            || IntervalArithmeticRexCallConverter.isIntervalOperand(callNode.getOperands().get(0));
    TypedExpr interval = params.get(firstIsInterval ? 0 : 1);
    TypedExpr numeric = params.get(firstIsInterval ? 1 : 0);
    RexNode intervalRexNode = callNode.getOperands().get(firstIsInterval ? 0 : 1);
    // Re-type the interval operand from BigIntType to the proper interval
    // velox type (literals are rebuilt as interval constants, other
    // expressions are cast).
    interval = IntervalArithmeticRexCallConverter.retypeInterval(interval, intervalRexNode);
    if (!(numeric.getReturnType() instanceof BigIntType)) {
      numeric = CastTypedExpr.create(new BigIntType(), numeric, false);
    }
    Type resultType = IntervalArithmeticRexCallConverter.getIntervalVeloxType(callNode);
    return new CallTypedExpr(resultType, new ArrayList<>(List.of(interval, numeric)), functionName);
  }
}

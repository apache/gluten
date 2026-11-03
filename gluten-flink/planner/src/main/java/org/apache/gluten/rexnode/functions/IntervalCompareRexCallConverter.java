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
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Converts interval-to-interval comparisons (e.g. {@code INTERVAL '1' HOUR * n > INTERVAL '2'
 * HOUR}) to the velox comparison functions. The sparksql baseline already registers the
 * greaterthan/lessthan/equalto/greaterthanorequal/lessthanorequal signatures for interval day to
 * second and interval year to month pairs, so only the operand typing lives here: day-time
 * intervals are mapped to BIGINT by the global type converter, so the operands are re-typed to the
 * proper interval velox types first. NOT EQUAL has no interval signature; it is emitted as
 * not(equalto(a, b)), which keeps Flink's three-valued NULL semantics.
 */
public class IntervalCompareRexCallConverter extends BaseRexCallConverter {

  private final boolean notEqual;

  public IntervalCompareRexCallConverter(String functionName) {
    super(functionName);
    this.notEqual = false;
  }

  private IntervalCompareRexCallConverter() {
    super("equalto");
    this.notEqual = true;
  }

  /** The converter for the NOT EQUAL operator; emits not(equalto(a, b)). */
  static IntervalCompareRexCallConverter notEqual() {
    return new IntervalCompareRexCallConverter();
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 2) {
      return ValidationResult.failure(
          String.format(
              "Interval comparison expects 2 operands, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    List<RexNode> operands = callNode.getOperands();
    boolean bothInterval =
        IntervalArithmeticRexCallConverter.isIntervalOperand(operands.get(0))
            && IntervalArithmeticRexCallConverter.isIntervalOperand(operands.get(1));
    if (!bothInterval) {
      return ValidationResult.failure(
          String.format(
              "Interval comparison requires two interval operands, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    List<RexNode> operands = callNode.getOperands();
    TypedExpr left =
        IntervalArithmeticRexCallConverter.retypeInterval(params.get(0), operands.get(0));
    TypedExpr right =
        IntervalArithmeticRexCallConverter.retypeInterval(params.get(1), operands.get(1));
    Type resultType = getResultType(callNode);
    CallTypedExpr comparison = new CallTypedExpr(resultType, List.of(left, right), functionName);
    if (!notEqual) {
      return comparison;
    }
    return new CallTypedExpr(new BooleanType(), List.of(comparison), "not");
  }
}

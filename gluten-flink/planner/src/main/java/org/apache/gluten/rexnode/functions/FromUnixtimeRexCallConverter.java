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
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.SmallIntType;
import io.github.zhztheplayer.velox4j.type.TinyIntType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;

import org.apache.calcite.rex.RexCall;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts Flink FROM_UNIXTIME to the velox flinksql from_unixtime function.
 *
 * <p>Flink SQL accepts the whole INTEGER family (TINYINT/SMALLINT/INT/BIGINT) for the first operand
 * and evaluates it as long; the velox flinksql function only registers the bigint signature, so
 * non-bigint first operands are cast to BIGINT — mirroring Flink codegen, which maps every integral
 * type to the long-based builtin method.
 */
public class FromUnixtimeRexCallConverter extends BaseRexCallConverter {

  public FromUnixtimeRexCallConverter() {
    super("from_unixtime");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    List<Type> operandTypes =
        callNode.getOperands().stream()
            .map(param -> RexNodeConverter.toType(param.getType()))
            .collect(Collectors.toList());
    if (operandTypes.isEmpty() || operandTypes.size() > 2) {
      return ValidationResult.failure(
          String.format(
              "FROM_UNIXTIME expects 1 or 2 operands, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    if (!isIntegralType(operandTypes.get(0))) {
      return ValidationResult.failure(
          String.format(
              "FROM_UNIXTIME requires an integral first operand, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    if (operandTypes.size() == 2 && !(operandTypes.get(1) instanceof VarCharType)) {
      return ValidationResult.failure(
          String.format(
              "FROM_UNIXTIME format operand must be a string, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    List<TypedExpr> callParams = new ArrayList<>();
    TypedExpr unixtime = params.get(0);
    if (!(unixtime.getReturnType() instanceof BigIntType)) {
      unixtime = CastTypedExpr.create(new BigIntType(), unixtime, false);
    }
    callParams.add(unixtime);
    if (params.size() == 2) {
      callParams.add(params.get(1));
    }
    return new CallTypedExpr(getResultType(callNode), callParams, functionName);
  }

  private static boolean isIntegralType(Type type) {
    return type instanceof TinyIntType
        || type instanceof SmallIntType
        || type instanceof IntegerType
        || type instanceof BigIntType;
  }
}

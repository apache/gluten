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
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.List;

/**
 * Converter for functions whose velox flinksql registrations cover scalar types only. Operands
 * (BINARY, ARRAY, MAP, ROW) are rejected here so conversion fails loudly at planning time instead
 * of failing at runtime with an unresolved velox signature.
 */
class ScalarOnlyRexCallConverter extends BaseRexCallConverter {

  public ScalarOnlyRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    boolean allScalar =
        callNode.getOperands().stream()
            .allMatch(arg -> TypeUtils.isScalarType(RexNodeConverter.toType(arg.getType())));
    if (!allScalar) {
      return ValidationResult.failure(
          String.format(
              "Scalar-only operation requires all operands to be scalar types, but found: %s",
              getFunctionProtoTypeName(callNode)));
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

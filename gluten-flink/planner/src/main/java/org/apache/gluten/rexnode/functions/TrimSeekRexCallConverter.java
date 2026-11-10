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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Converts the Flink two-argument LTRIM/RTRIM to the velox flinksql functions, routing by the
 * constancy of the seek.
 *
 * <p>Flink's {@code BinaryStringDataUtil.isSpaceString} keeps the Java object of a plan constant
 * (character-set walk: {@code ltrim}/{@code rtrim}) while a runtime-sourced seek — a column or a
 * computed expression — is segment-backed and only its first byte is checked, degrading a seek that
 * starts with a space to space-only trimming ({@code ltrim_runtime}/{@code rtrim_runtime}). See
 * also {@link TrimRexCallConverter}.
 *
 * <p>The one-argument form ({@code LTRIM(s)}) maps to the plain function directly.
 */
class TrimSeekRexCallConverter extends BaseRexCallConverter {

  TrimSeekRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() > 2) {
      return ValidationResult.failure("LTRIM/RTRIM take at most two operands");
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<RexNode> operands = callNode.getOperands();
    String name = functionName;
    if (operands.size() == 2 && !TrimSeekFlavor.isConstantSeek(operands.get(1))) {
      name = name + "_runtime";
    }
    return new CallTypedExpr(getResultType(callNode), getParams(callNode, context), name);
  }
}

final class TrimSeekFlavor {
  private TrimSeekFlavor() {}

  /** A RexLiteral seek maps to a velox constant; anything else is runtime-sourced. */
  static boolean isConstantSeek(RexNode seek) {
    return seek instanceof RexLiteral;
  }
}

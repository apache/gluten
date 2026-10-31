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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlTrimFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts Flink TRIM to the velox flinksql trim family.
 *
 * <p>Calcite normalizes every TRIM call to three operands {@code (flag, seek, str)}: {@code
 * TRIM(s)} desugars to {@code (BOTH, ' ', s)}, and {@code TRIM(LEADING 'x' FROM c)} to {@code
 * (LEADING, 'x', c)}. The flag is a RexLiteral carrying a {@link SqlTrimFunction.Flag}.
 *
 * <p>The velox flinksql signatures take the Flink argument order {@code (str, seek)}, so the
 * operands are reordered to {@code (operand[2], operand[1])} and the flag selects the target
 * function: LEADING → {@code ltrim}, TRAILING → {@code rtrim}, BOTH → {@code trim}. All three
 * accept the optional second argument (a character set, not a prefix).
 *
 * <p>The seek picks the flavor by constancy, mirroring Flink's {@code
 * BinaryStringDataUtil.isSpaceString}: a plan constant keeps its Java object and takes the
 * character-set walk ({@code trim}/{@code ltrim}/{@code rtrim}); a runtime-sourced seek (column or
 * computed expression) is segment-backed and Flink checks only its first byte, degrading a seek
 * that starts with a space to space-only trimming ({@code trim_runtime} et al.).
 *
 * <p>A non-literal flag (a pathological plan) is rejected by {@link #isSuitable}.
 */
public class TrimRexCallConverter extends BaseRexCallConverter {

  public TrimRexCallConverter() {
    super("trim");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    List<RexNode> operands = callNode.getOperands();
    if (operands.size() != 3 || !(operands.get(0) instanceof RexLiteral)) {
      return ValidationResult.failure("TRIM requires a literal trim flag");
    }
    if (((RexLiteral) operands.get(0)).getValueAs(SqlTrimFunction.Flag.class) == null) {
      return ValidationResult.failure("TRIM requires a literal trim flag");
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<RexNode> operands = callNode.getOperands();
    SqlTrimFunction.Flag flag =
        ((RexLiteral) operands.get(0)).getValueAs(SqlTrimFunction.Flag.class);
    String functionName;
    switch (flag) {
      case LEADING:
        functionName = "ltrim";
        break;
      case TRAILING:
        functionName = "rtrim";
        break;
      default:
        functionName = "trim";
        break;
    }
    // A runtime-sourced seek takes the *_runtime flavor (Flink's isSpaceString
    // degrades a seek starting with a space to space-only trimming).
    if (!TrimSeekFlavor.isConstantSeek(operands.get(1))) {
      functionName = functionName + "_runtime";
    }
    // Flink/velox argument order: (str, seek) = (operand[2], operand[1]).
    List<TypedExpr> params = new ArrayList<>();
    params.add(RexNodeConverter.toTypedExpr(operands.get(2), context));
    params.add(RexNodeConverter.toTypedExpr(operands.get(1), context));
    return new CallTypedExpr(getResultType(callNode), params, functionName);
  }
}

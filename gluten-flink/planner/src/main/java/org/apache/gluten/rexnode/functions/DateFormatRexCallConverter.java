/*
 * Licensed to the Apache Software Foundation (ASF) (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Converts Flink DATE_FORMAT to the matching velox flinksql entry.
 *
 * <p>The two Flink timestamp flavors render differently and keep different velox mappings:
 *
 * <ul>
 *   <li>DATE_FORMAT over TIMESTAMP (no timezone): the wall clock maps to the flinksql
 *       date_format entry (wall-clock rendering, session timezone does NOT apply).
 *   <li>DATE_FORMAT over TIMESTAMP_LTZ: the planner's type mapping erases the LTZ flavor to a
 *       velox TIMESTAMP whose value stays the true-UTC instant; the call maps by name to the
 *       flinksql date_format_ltz entry, which renders the instant in the session timezone
 *       (Flink's semantics for the LTZ type).
 * </ul>
 */
public class DateFormatRexCallConverter extends BaseRexCallConverter {
  private static final String LTZ_FUNCTION_NAME = "date_format_ltz";

  public DateFormatRexCallConverter() {
    super("date_format");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    List<RexNode> operands = callNode.getOperands();
    if (operands.size() != 2) {
      return ValidationResult.failure(
          String.format(
              "DATE_FORMAT expects 2 operands, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    SqlTypeName first = operands.get(0).getType().getSqlTypeName();
    if (first != SqlTypeName.TIMESTAMP
        && first != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return ValidationResult.failure(
          String.format(
              "DATE_FORMAT requires a timestamp operand, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    SqlTypeName second = operands.get(1).getType().getSqlTypeName();
    if (second != SqlTypeName.CHAR && second != SqlTypeName.VARCHAR) {
      return ValidationResult.failure(
          String.format(
              "DATE_FORMAT format operand must be a string, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);
    SqlTypeName first = callNode.getOperands().get(0).getType().getSqlTypeName();
    String functionName =
        first == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            ? LTZ_FUNCTION_NAME
            : this.functionName;
    return new CallTypedExpr(resultType, params, functionName);
  }
}

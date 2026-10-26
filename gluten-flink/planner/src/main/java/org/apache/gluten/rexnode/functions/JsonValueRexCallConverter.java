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

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;

import java.util.ArrayList;
import java.util.List;

/**
 * Converter for Flink's JSON_VALUE to velox's flinksql json_value.
 *
 * <p>RexCall operand layout (measured against native Flink 1.19.2):
 *
 * <ul>
 *   <li>[0] json input expression
 *   <li>[1] path literal
 *   <li>[2..] alternating handler triplets: {@code (behavior, [default], ON EMPTY|ON ERROR)} where
 *       behavior is a SYMBOL among NULL / ERROR / DEFAULT. DEFAULT is followed by its literal
 *       default value.
 * </ul>
 *
 * <p>RETURNING is represented by the RexCall {@code getType()}; json_value returns VARCHAR in
 * velox, so a non-VARCHAR RETURNING is wrapped in a CAST.
 */
public class JsonValueRexCallConverter extends BaseRexCallConverter {

  private static final String FUNCTION_NAME = "json_value";
  private static final int ON_NULL = 0;
  private static final int ON_ERROR = 1;
  private static final int ON_DEFAULT = 2;
  private static final VarCharType VARCHAR_TYPE = new VarCharType();
  private static final IntegerType INT_TYPE = new IntegerType();

  public JsonValueRexCallConverter() {
    super(FUNCTION_NAME);
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<RexNode> operands = callNode.getOperands();
    // [0] input json, [1] path literal
    TypedExpr jsonExpr = RexNodeConverter.toTypedExpr(operands.get(0), context);
    String path = literalValue(operands.get(1));

    int onEmpty = ON_NULL;
    String onEmptyDefault = "";
    int onError = ON_NULL;
    String onErrorDefault = "";

    int i = 2;
    while (i < operands.size()) {
      String behavior = symbolValue(operands.get(i));
      i++;
      String defaultVal = "";
      if ("DEFAULT".equals(behavior)) {
        defaultVal = literalValue(operands.get(i));
        i++;
      }
      String ctx = symbolValue(operands.get(i));
      i++;
      int enc = behaviorToEnum(behavior);
      if ("ON EMPTY".equals(ctx)) {
        onEmpty = enc;
        onEmptyDefault = defaultVal;
      } else {
        onError = enc;
        onErrorDefault = defaultVal;
      }
    }

    List<TypedExpr> params = new ArrayList<>();
    params.add(jsonExpr);
    params.add(strConst(path));
    params.add(intConst(onEmpty));
    params.add(strConst(onEmptyDefault));
    params.add(intConst(onError));
    params.add(strConst(onErrorDefault));

    CallTypedExpr jsonValue = new CallTypedExpr(VARCHAR_TYPE, params, FUNCTION_NAME);

    Type resultType = RexNodeConverter.toType(callNode.getType());
    if (resultType instanceof VarCharType) {
      return jsonValue;
    }
    // RETURNING a non-VARCHAR type: cast the extracted VARCHAR.
    return new CallTypedExpr(resultType, List.of(jsonValue), "cast");
  }

  private static int behaviorToEnum(String behavior) {
    switch (behavior) {
      case "NULL":
        return ON_NULL;
      case "ERROR":
        return ON_ERROR;
      case "DEFAULT":
        return ON_DEFAULT;
      default:
        throw new RuntimeException("Unsupported JSON_VALUE behavior: " + behavior);
    }
  }

  private static String symbolValue(RexNode node) {
    if (node instanceof RexLiteral) {
      Object value = ((RexLiteral) node).getValue();
      return String.valueOf(value);
    }
    throw new RuntimeException("Expected a literal symbol for JSON_VALUE handler, got: " + node);
  }

  private static String literalValue(RexNode node) {
    if (node instanceof RexLiteral) {
      Object value = ((RexLiteral) node).getValue();
      if (value instanceof NlsString) {
        return ((NlsString) value).getValue();
      }
      return String.valueOf(value);
    }
    throw new RuntimeException("Expected a literal for JSON_VALUE, got: " + node);
  }

  private static ConstantTypedExpr strConst(String value) {
    return new ConstantTypedExpr(VARCHAR_TYPE, new VarCharValue(value), null);
  }

  private static ConstantTypedExpr intConst(int value) {
    return new ConstantTypedExpr(INT_TYPE, new IntegerValue(value), null);
  }
}

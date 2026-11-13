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
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;
import io.github.zhztheplayer.velox4j.variant.DoubleValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

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
 * <p>RETURNING is represented by the RexCall {@code getType()}. velox registers one json_value
 * signature per RETURNING type, so the default values and result carry that same type — no separate
 * CAST wrapping is needed.
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
    Type resultType = RexNodeConverter.toType(callNode.getType());

    // [0] input json, [1] path literal (always a VARCHAR).
    TypedExpr jsonExpr = RexNodeConverter.toTypedExpr(operands.get(0), context);
    VarCharValue path = pathValue(operands.get(1));

    int onEmpty = ON_NULL;
    Variant onEmptyDefault = nullVariant();
    int onError = ON_NULL;
    Variant onErrorDefault = nullVariant();

    int i = 2;
    while (i < operands.size()) {
      checkIndex(i, operands.size(), "handler behavior");
      String behavior = symbolValue(operands.get(i));
      i++;
      Variant defaultVal = nullVariant();
      if ("DEFAULT".equals(behavior)) {
        checkIndex(i, operands.size(), "DEFAULT value");
        defaultVal = defaultVariant(operands.get(i), resultType);
        i++;
      }
      checkIndex(i, operands.size(), "handler context (ON EMPTY / ON ERROR)");
      String ctx = symbolValue(operands.get(i));
      i++;
      int enc = behaviorToEnum(behavior);
      if ("ON EMPTY".equals(ctx)) {
        onEmpty = enc;
        onEmptyDefault = defaultVal;
      } else if ("ON ERROR".equals(ctx)) {
        onError = enc;
        onErrorDefault = defaultVal;
      } else {
        throw new RuntimeException("Unknown JSON_VALUE handler context: " + ctx);
      }
    }

    List<TypedExpr> params = new ArrayList<>();
    params.add(jsonExpr);
    params.add(constant(path, VARCHAR_TYPE));
    params.add(intConst(onEmpty));
    params.add(constant(onEmptyDefault, resultType));
    params.add(intConst(onError));
    params.add(constant(onErrorDefault, resultType));

    return new CallTypedExpr(resultType, params, FUNCTION_NAME);
  }

  private static void checkIndex(int i, int size, String what) {
    if (i >= size) {
      throw new RuntimeException("JSON_VALUE: missing " + what + " at operand " + i);
    }
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

  private static VarCharValue pathValue(RexNode node) {
    if (node instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) node;
      Object value = literal.getValue();
      if (value instanceof NlsString) {
        return new VarCharValue(((NlsString) value).getValue());
      }
      return new VarCharValue(String.valueOf(value));
    }
    throw new RuntimeException("Expected a literal path for JSON_VALUE, got: " + node);
  }

  // Converts a DEFAULT literal into the variant carrying the RETURNING type; null
  // information is retained as a typed null instead of being flattened to text.
  private static Variant defaultVariant(RexNode node, Type resultType) {
    RexLiteral literal = (RexLiteral) node;
    if (literal.getValue() == null || literal.isNull()) {
      return nullVariant();
    }
    Object value = literal.getValue();
    if (resultType instanceof VarCharType) {
      if (value instanceof NlsString) {
        return new VarCharValue(((NlsString) value).getValue());
      }
      return new VarCharValue(String.valueOf(value));
    }
    if (resultType instanceof IntegerType) {
      return new IntegerValue(((Number) value).intValue());
    }
    if (resultType instanceof BooleanType) {
      return new BooleanValue((Boolean) value);
    }
    if (resultType instanceof DoubleType) {
      return new DoubleValue(((Number) value).doubleValue());
    }
    throw new RuntimeException("Unsupported JSON_VALUE RETURNING type: " + resultType);
  }

  private static Variant nullVariant() {
    // A typed null needs no per-type carrier: the velox-side ConstantTypedExpr
    // renders a null variant as a null constant of the EXPRESSION type (the
    // variant kind is irrelevant on the null path). VarCharValue is the natural
    // null-accepting carrier and serializes with an explicit "value": null via
    // the @JsonInclude(ALWAYS) on the Variant base class.
    return new VarCharValue(null);
  }

  private static ConstantTypedExpr constant(Variant value, Type type) {
    return new ConstantTypedExpr(type, value, null);
  }

  private static ConstantTypedExpr intConst(int value) {
    return new ConstantTypedExpr(INT_TYPE, new IntegerValue(value), null);
  }
}

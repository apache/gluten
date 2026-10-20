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
package org.apache.gluten.substrait.expression;

import org.apache.gluten.exception.GlutenNotSupportException;
import org.apache.gluten.expression.ConverterUtils;
import org.apache.gluten.substrait.type.*;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;

import java.util.List;
import java.util.Map;

/** Contains helper functions for constructing substrait relations. */
public class ExpressionBuilder {
  private ExpressionBuilder() {}

  public static NullLiteralNode makeNullLiteral(TypeNode typeNode) {
    return new NullLiteralNode(typeNode);
  }

  public static BooleanLiteralNode makeBooleanLiteral(Boolean vBool) {
    return new BooleanLiteralNode(vBool);
  }

  public static BooleanLiteralNode makeBooleanLiteral(Boolean vBool, TypeNode typeNode) {
    return new BooleanLiteralNode(vBool, typeNode);
  }

  public static IntLiteralNode makeIntLiteral(Integer vInt) {
    return new IntLiteralNode(vInt);
  }

  public static IntLiteralNode makeIntLiteral(Integer vInt, TypeNode typeNode) {
    return new IntLiteralNode(vInt, typeNode);
  }

  public static ByteLiteralNode makeByteLiteral(Byte vByte) {
    return new ByteLiteralNode(vByte);
  }

  public static ByteLiteralNode makeByteLiteral(Byte vByte, TypeNode typeNode) {
    return new ByteLiteralNode(vByte, typeNode);
  }

  public static ShortLiteralNode makeShortLiteral(Short vShort) {
    return new ShortLiteralNode(vShort);
  }

  public static ShortLiteralNode makeShortLiteral(Short vShort, TypeNode typeNode) {
    return new ShortLiteralNode(vShort, typeNode);
  }

  public static LongLiteralNode makeLongLiteral(Long vLong) {
    return new LongLiteralNode(vLong);
  }

  public static LongLiteralNode makeLongLiteral(Long vLong, TypeNode typeNode) {
    return new LongLiteralNode(vLong, typeNode);
  }

  public static DoubleLiteralNode makeDoubleLiteral(Double vDouble) {
    return new DoubleLiteralNode(vDouble);
  }

  public static DoubleLiteralNode makeDoubleLiteral(Double vDouble, TypeNode typeNode) {
    return new DoubleLiteralNode(vDouble, typeNode);
  }

  public static FloatLiteralNode makeFloatLiteral(Float vFloat) {
    return new FloatLiteralNode(vFloat);
  }

  public static FloatLiteralNode makeFloatLiteral(Float vFloat, TypeNode typeNode) {
    return new FloatLiteralNode(vFloat, typeNode);
  }

  public static DateLiteralNode makeDateLiteral(Integer vDate) {
    return new DateLiteralNode(vDate);
  }

  public static DateLiteralNode makeDateLiteral(Integer vDate, TypeNode typeNode) {
    return new DateLiteralNode(vDate, typeNode);
  }

  public static TimestampLiteralNode makeTimestampLiteral(Long vTimestamp) {
    return new TimestampLiteralNode(vTimestamp);
  }

  public static TimestampLiteralNode makeTimestampLiteral(Long vTimestamp, TypeNode typeNode) {
    return new TimestampLiteralNode(vTimestamp, typeNode);
  }

  public static StringLiteralNode makeStringLiteral(String vString) {
    return new StringLiteralNode(vString);
  }

  public static StringLiteralNode makeStringLiteral(String vString, TypeNode typeNode) {
    return new StringLiteralNode(vString, typeNode);
  }

  public static BinaryLiteralNode makeBinaryLiteral(byte[] vBytes) {
    return new BinaryLiteralNode(vBytes);
  }

  public static BinaryLiteralNode makeBinaryLiteral(byte[] vBytes, TypeNode typeNode) {
    return new BinaryLiteralNode(vBytes, typeNode);
  }

  public static DecimalLiteralNode makeDecimalLiteral(Decimal vDecimal) {
    return new DecimalLiteralNode(vDecimal);
  }

  public static DecimalLiteralNode makeDecimalLiteral(Decimal vDecimal, TypeNode typeNode) {
    return new DecimalLiteralNode(vDecimal, typeNode);
  }

  public static ListLiteralNode makeListLiteral(ArrayData array, TypeNode typeNode) {
    return new ListLiteralNode(array, typeNode);
  }

  public static MapLiteralNode makeMapLiteral(MapData map, TypeNode typeNode) {
    return new MapLiteralNode(map, typeNode);
  }

  public static StructLiteralNode makeStructLiteral(InternalRow row, TypeNode typeNode) {
    return new StructLiteralNode(row, typeNode);
  }

  public static LiteralNode makeLiteral(Object obj, TypeNode typeNode) {
    if (obj == null) {
      return makeNullLiteral(typeNode);
    }
    if (typeNode instanceof BooleanTypeNode) {
      return makeBooleanLiteral((Boolean) obj, typeNode);
    }
    if (typeNode instanceof I8TypeNode) {
      return makeByteLiteral((Byte) obj, typeNode);
    }
    if (typeNode instanceof I16TypeNode) {
      return makeShortLiteral((Short) obj, typeNode);
    }
    if (typeNode instanceof I32TypeNode) {
      return makeIntLiteral((Integer) obj, typeNode);
    }
    if (typeNode instanceof I64TypeNode) {
      return makeLongLiteral((Long) obj, typeNode);
    }
    if (typeNode instanceof FP32TypeNode) {
      return makeFloatLiteral((Float) obj, typeNode);
    }
    if (typeNode instanceof FP64TypeNode) {
      return makeDoubleLiteral((Double) obj, typeNode);
    }
    if (typeNode instanceof DateTypeNode) {
      return makeDateLiteral((Integer) obj, typeNode);
    }
    if (typeNode instanceof TimestampTypeNode) {
      return makeTimestampLiteral((Long) obj, typeNode);
    }
    if (typeNode instanceof StringTypeNode) {
      return makeStringLiteral(obj.toString(), typeNode);
    }
    if (typeNode instanceof BinaryTypeNode) {
      return makeBinaryLiteral((byte[]) obj, typeNode);
    }
    if (typeNode instanceof DecimalTypeNode) {
      Decimal decimal = (Decimal) obj;
      checkDecimalScale(decimal.scale());
      return makeDecimalLiteral(decimal, typeNode);
    }
    if (typeNode instanceof ListNode) {
      return makeListLiteral((ArrayData) obj, typeNode);
    }
    if (typeNode instanceof MapNode) {
      return makeMapLiteral((MapData) obj, typeNode);
    }
    if (typeNode instanceof StructNode) {
      return makeStructLiteral((InternalRow) obj, typeNode);
    }
    throw new GlutenNotSupportException(
        String.format(
            "Type not supported: %s, obj: %s, class: %s",
            typeNode.toString(), obj.toString(), obj.getClass().toString()));
  }

  public static LiteralNode makeLiteral(Object obj, DataType dataType, Boolean nullable) {
    TypeNode typeNode = ConverterUtils.getTypeNode(dataType, nullable);
    return makeLiteral(obj, typeNode);
  }

  public static void checkDecimalScale(int scale) {
    if (scale < 0) {
      // Substrait don't support decimal type with negative scale.
      throw new UnsupportedOperationException(
          String.format("DecimalType with negative scale not supported: %s.", scale));
    }
  }

  public static ScalarFunctionNode makeScalarFunction(
      Long functionId, List<ExpressionNode> expressionNodes, TypeNode typeNode) {
    return new ScalarFunctionNode(functionId, expressionNodes, typeNode);
  }

  public static SelectionNode makeSelection(Integer fieldIdx) {
    return new SelectionNode(fieldIdx);
  }

  public static SelectionNode makeSelection(Integer fieldIdx, Integer childFieldIdx) {
    return new SelectionNode(fieldIdx, childFieldIdx);
  }

  public static AggregateFunctionNode makeAggregateFunction(
      Long functionId,
      List<ExpressionNode> expressionNodes,
      String phase,
      TypeNode outputTypeNode) {
    return new AggregateFunctionNode(functionId, expressionNodes, phase, outputTypeNode);
  }

  public static CastNode makeCast(
      TypeNode typeNode, ExpressionNode expressionNode, boolean isTryCast) {
    return new CastNode(typeNode, expressionNode, isTryCast);
  }

  public static CastNode makeCast(
      TypeNode typeNode, ExpressionNode expressionNode, CastNode.CastMode castMode) {
    return new CastNode(typeNode, expressionNode, castMode);
  }

  public static StringMapNode makeStringMap(Map<String, String> values) {
    return new StringMapNode(values);
  }

  public static SingularOrListNode makeSingularOrListNode(
      ExpressionNode value, List<ExpressionNode> expressionNodes) {
    return new SingularOrListNode(value, expressionNodes);
  }

  public static WindowFunctionNode makeWindowFunction(
      Integer functionId,
      List<ExpressionNode> expressionNodes,
      String columnName,
      TypeNode outputTypeNode,
      Expression upperBound,
      Expression lowerBound,
      String frameType,
      List<Attribute> originalInputAttributes) {
    return makeWindowFunction(
        functionId,
        expressionNodes,
        columnName,
        outputTypeNode,
        upperBound,
        lowerBound,
        frameType,
        false,
        originalInputAttributes);
  }

  public static WindowFunctionNode makeWindowFunction(
      Integer functionId,
      List<ExpressionNode> expressionNodes,
      String columnName,
      TypeNode outputTypeNode,
      Expression upperBound,
      Expression lowerBound,
      String frameType,
      boolean ignoreNulls,
      List<Attribute> originalInputAttributes) {
    return new WindowFunctionNode(
        functionId,
        expressionNodes,
        columnName,
        outputTypeNode,
        upperBound,
        lowerBound,
        frameType,
        ignoreNulls,
        originalInputAttributes);
  }
}

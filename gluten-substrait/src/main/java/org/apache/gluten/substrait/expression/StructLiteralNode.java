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

import org.apache.gluten.substrait.type.*;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;
import org.apache.spark.sql.catalyst.InternalRow;

public class StructLiteralNode extends LiteralNodeWithValue<InternalRow> {
  public StructLiteralNode(InternalRow row, TypeNode typeNode) {
    super(row, typeNode);
  }

  public LiteralNode getFieldLiteral(int index) {
    InternalRow value = getValue();
    TypeNode type = ((StructNode) getTypeNode()).getFieldTypes().get(index);
    if (value.isNullAt(index)) {
      return ExpressionBuilder.makeNullLiteral(type);
    }

    if (type instanceof BooleanTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getBoolean(index), type);
    }
    if (type instanceof I8TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getByte(index), type);
    }
    if (type instanceof I16TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getShort(index), type);
    }
    if (type instanceof I32TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getInt(index), type);
    }
    if (type instanceof I64TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getLong(index), type);
    }
    if (type instanceof FP32TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getFloat(index), type);
    }
    if (type instanceof FP64TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getDouble(index), type);
    }
    if (type instanceof DateTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getInt(index), type);
    }
    if (type instanceof TimestampTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getLong(index), type);
    }
    if (type instanceof TimeTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getLong(index), type);
    }
    if (type instanceof StringTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getUTF8String(index), type);
    }
    if (type instanceof BinaryTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getBinary(index), type);
    }
    if (type instanceof DecimalTypeNode) {
      return ExpressionBuilder.makeLiteral(
          value.getDecimal(
              index, ((DecimalTypeNode) type).precision, ((DecimalTypeNode) type).scale),
          type);
    }
    if (type instanceof ListNode) {
      return ExpressionBuilder.makeLiteral(value.getArray(index), type);
    }
    if (type instanceof MapNode) {
      return ExpressionBuilder.makeLiteral(value.getMap(index), type);
    }
    if (type instanceof StructNode) {
      return ExpressionBuilder.makeLiteral(
          value.getStruct(index, ((StructNode) type).getFieldTypes().size()), type);
    }
    if (type instanceof NothingNode) {
      return ExpressionBuilder.makeNullLiteral(type);
    }
    throw new UnsupportedOperationException(
        type.toString() + " is not supported in getFieldLiteral.");
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, InternalRow row) {
    Expression.Literal.Struct.Builder structBuilder = Expression.Literal.Struct.newBuilder();
    StructNode structNode = (StructNode) getTypeNode();
    for (int i = 0; i < structNode.getFieldTypes().size(); ++i) {
      structBuilder.addFields(getFieldLiteral(i).getLiteral());
      structBuilder.addNames(structNode.getNames().get(i));
    }
    literalBuilder.setStruct(structBuilder.build());
  }
}

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
package org.apache.gluten.rexnode;

import org.apache.gluten.rexnode.functions.RexCallConverter;
import org.apache.gluten.rexnode.functions.RexCallConverterFactory;
import org.apache.gluten.util.LogicalTypeConverter;

import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.ArrayValue;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;
import io.github.zhztheplayer.velox4j.variant.DoubleValue;
import io.github.zhztheplayer.velox4j.variant.HugeIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.SmallIntValue;
import io.github.zhztheplayer.velox4j.variant.TinyIntValue;
import io.github.zhztheplayer.velox4j.variant.VarBinaryValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.calcite.shaded.com.google.common.collect.Range;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Convertor to convert RexNode to velox TypedExpr */
public class RexNodeConverter {

  public static TypedExpr toTypedExpr(RexNode rexNode, RexConversionContext context) {
    if (rexNode instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) rexNode;
      return new ConstantTypedExpr(toType(literal.getType()), toVariant(literal), null);
    } else if (rexNode instanceof RexCall) {
      RexCall rexCall = (RexCall) rexNode;
      RexCallConverter converter = RexCallConverterFactory.getConverter(rexCall, context);
      return converter.toTypedExpr(rexCall, context);
    } else if (rexNode instanceof RexInputRef) {
      RexInputRef inputRef = (RexInputRef) rexNode;
      List<String> inputAttributes = context.getInputAttributeNames();
      Preconditions.checkArgument(
          inputAttributes.size() > inputRef.getIndex(),
          "InputRef index " + inputRef.getIndex() + " not in " + inputAttributes);
      return FieldAccessTypedExpr.create(
          toType(inputRef.getType()), inputAttributes.get(inputRef.getIndex()));
    } else if (rexNode instanceof RexFieldAccess) {
      RexFieldAccess fieldAccess = (RexFieldAccess) rexNode;
      return FieldAccessTypedExpr.create(
          toTypedExpr(fieldAccess.getReferenceExpr(), context), fieldAccess.getField().getName());
    } else {
      throw new RuntimeException("Unrecognized RexNode: " + rexNode.getClass().getName());
    }
  }

  public static List<TypedExpr> toTypedExpr(List<RexNode> rexNodes, RexConversionContext context) {
    return rexNodes.stream()
        .map(rexNode -> toTypedExpr(rexNode, context))
        .collect(Collectors.toList());
  }

  // TODO: use LogicalRelDataTypeConverter
  public static Type toType(RelDataType relDataType) {
    return LogicalTypeConverter.toVLType(FlinkTypeFactory.toLogicalType(relDataType));
  }

  public static Variant toVariant(RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case BOOLEAN:
        return new BooleanValue((boolean) literal.getValue());
      case TINYINT:
        return new TinyIntValue(Integer.valueOf(literal.getValue().toString()));
      case SMALLINT:
        return new SmallIntValue(Integer.valueOf(literal.getValue().toString()));
      case INTEGER:
        return new IntegerValue(Integer.valueOf(literal.getValue().toString()));
      case BIGINT:
        return new BigIntValue(Long.valueOf(literal.getValue().toString()));
      case DOUBLE:
        return new DoubleValue(Double.valueOf(literal.getValue().toString()));
      case VARCHAR:
        return new VarCharValue(literal.getValueAs(String.class));
      case CHAR:
        // For CHAR, we convert it to VARCHAR
        return new VarCharValue(literal.getValueAs(String.class));
      case BINARY:
        return VarBinaryValue.create(
            literal.getValue().toString().getBytes(StandardCharsets.UTF_8));
      case DECIMAL:
      case INTERVAL_SECOND:
        // interval is used as decimal.
        // TODO: fix precision check
        BigDecimal bigDecimal = literal.getValueAs(BigDecimal.class);
        if (bigDecimal.precision() <= 18) {
          return new BigIntValue(bigDecimal.unscaledValue().longValueExact());
        } else {
          return new HugeIntValue(bigDecimal.unscaledValue());
        }
      case SYMBOL:
        return new VarCharValue(literal.getValue().toString());
      default:
        throw new RuntimeException(
            "Unsupported rex node type: " + literal.getType().getSqlTypeName());
    }
  }

  public static TypedExpr toTypedExpr(Set<Range> ranges, RelDataType relDataType) {
    List<Variant> values = new ArrayList<>(ranges.size());
    for (Range range : ranges) {
      if (range.lowerEndpoint() != range.upperEndpoint()) {
        throw new RuntimeException("Not support multi ranges " + range);
      }
      values.add(toVariant(range.lowerEndpoint(), relDataType.getSqlTypeName()));
    }
    Variant arrayValue = new ArrayValue(values);
    return new ConstantTypedExpr(ArrayType.create(toType(relDataType)), arrayValue, null);
  }

  public static List<TypedExpr> toTypedExpr(Range range, RelDataType relDataType) {
    List<TypedExpr> results = new ArrayList<>(2);
    Type resType = toType(relDataType);
    results.add(
        new ConstantTypedExpr(
            resType, toVariant(range.lowerEndpoint(), relDataType.getSqlTypeName()), null));
    results.add(
        new ConstantTypedExpr(
            resType, toVariant(range.upperEndpoint(), relDataType.getSqlTypeName()), null));
    return results;
  }

  public static Variant toVariant(Comparable comparable, SqlTypeName typeName) {
    switch (typeName) {
      case INTEGER:
        return new IntegerValue(((BigDecimal) comparable).intValue());
      case VARCHAR:
        if (comparable instanceof NlsString) {
          return new VarCharValue(((NlsString) comparable).getValue());
        }
        return new VarCharValue(comparable.toString());
      case CHAR:
        return new VarCharValue(((NlsString) comparable).getValue());
      default:
        throw new RuntimeException("Unsupported range type: " + typeName);
    }
  }
}

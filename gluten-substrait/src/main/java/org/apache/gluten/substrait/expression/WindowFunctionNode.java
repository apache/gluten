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

import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.expression.ExpressionConverter;
import org.apache.gluten.substrait.SubstraitContext;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.ConsistentPartitionWindowRel;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.FunctionOption;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.PreComputeRangeFrameBound;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConverters;

public class WindowFunctionNode implements Serializable {
  private final Integer functionId;
  private final List<ExpressionNode> expressionNodes = new ArrayList<>();

  private final String columnName;
  private final TypeNode outputTypeNode;

  private final org.apache.spark.sql.catalyst.expressions.Expression upperBound;

  private final org.apache.spark.sql.catalyst.expressions.Expression lowerBound;

  private final String frameType;

  private final boolean ignoreNulls;

  private final List<Attribute> originalInputAttributes;

  WindowFunctionNode(
      Integer functionId,
      List<ExpressionNode> expressionNodes,
      String columnName,
      TypeNode outputTypeNode,
      org.apache.spark.sql.catalyst.expressions.Expression upperBound,
      org.apache.spark.sql.catalyst.expressions.Expression lowerBound,
      String frameType,
      boolean ignoreNulls,
      List<Attribute> originalInputAttributes) {
    this.functionId = functionId;
    this.expressionNodes.addAll(expressionNodes);
    this.columnName = columnName;
    this.outputTypeNode = outputTypeNode;
    this.upperBound = upperBound;
    this.lowerBound = lowerBound;
    this.frameType = frameType;
    this.ignoreNulls = ignoreNulls;
    this.originalInputAttributes = originalInputAttributes;
  }

  private Expression.WindowFunction.Bound.Builder setBound(
      Expression.WindowFunction.Bound.Builder builder,
      org.apache.spark.sql.catalyst.expressions.Expression boundType) {
    switch (boundType.sql()) {
      case ("CURRENT ROW"):
        Expression.WindowFunction.Bound.CurrentRow.Builder currentRowBuilder =
            Expression.WindowFunction.Bound.CurrentRow.newBuilder();
        builder.setCurrentRow(currentRowBuilder.build());
        break;
      case ("UNBOUNDED PRECEDING"):
      case ("UNBOUNDED FOLLOWING"):
        // Substrait 0.98 collapses the two unbounded bounds into a single `unbounded`; the
        // direction is inferred from position (lower bound = start of the partition, upper
        // bound = end of the partition).
        builder.setUnbounded(Expression.WindowFunction.Bound.Unbounded.newBuilder().build());
        break;
      default:
        if (boundType instanceof PreComputeRangeFrameBound) {
          // Used only when backend is velox and frame type is RANGE.
          if (!frameType.equals("RANGE")) {
            throw new GlutenException(
                "Only Range frame supports PreComputeRangeFrameBound, but got " + frameType);
          }
          ExpressionNode refNode =
              ExpressionConverter.replaceWithExpressionTransformer(
                      ((PreComputeRangeFrameBound) boundType).child().toAttribute(),
                      JavaConverters.asScalaIteratorConverter(originalInputAttributes.iterator())
                          .asScala()
                          .toSeq())
                  .doTransform(new SubstraitContext());
          Long offset = Long.valueOf(boundType.eval(null).toString());
          if (offset < 0) {
            Expression.WindowFunction.Bound.Preceding.Builder refPrecedingBuilder =
                Expression.WindowFunction.Bound.Preceding.newBuilder();
            refPrecedingBuilder.setRef(refNode.toProtobuf());
            builder.setPreceding(refPrecedingBuilder.build());
          } else {
            Expression.WindowFunction.Bound.Following.Builder refFollowingBuilder =
                Expression.WindowFunction.Bound.Following.newBuilder();
            refFollowingBuilder.setRef(refNode.toProtobuf());
            builder.setFollowing(refFollowingBuilder.build());
          }
        } else if (boundType.foldable()) {
          // Used when
          // 1. Velox backend and frame type is ROW
          // 2. Clickhouse backend
          Long offset = Long.valueOf(boundType.eval(null).toString());
          if (offset < 0) {
            Expression.WindowFunction.Bound.Preceding.Builder offsetPrecedingBuilder =
                Expression.WindowFunction.Bound.Preceding.newBuilder();
            offsetPrecedingBuilder.setOffset(0 - offset);
            builder.setPreceding(offsetPrecedingBuilder.build());
          } else {
            Expression.WindowFunction.Bound.Following.Builder offsetFollowingBuilder =
                Expression.WindowFunction.Bound.Following.newBuilder();
            offsetFollowingBuilder.setOffset(offset);
            builder.setFollowing(offsetFollowingBuilder.build());
          }
        } else {
          throw new UnsupportedOperationException(
              "Unsupported Window Function Frame Bound Type: " + boundType);
        }
    }
    return builder;
  }

  private Expression.WindowFunction.BoundsType getBoundsType(String type) {
    Expression.WindowFunction.BoundsType boundsType;
    switch (type) {
      case ("ROWS"):
        boundsType = Expression.WindowFunction.BoundsType.BOUNDS_TYPE_ROWS;
        break;
      case ("RANGE"):
        boundsType = Expression.WindowFunction.BoundsType.BOUNDS_TYPE_RANGE;
        break;
      default:
        throw new UnsupportedOperationException("Only support ROWS and RANGE Frame type.");
    }
    return boundsType;
  }

  public ConsistentPartitionWindowRel.WindowRelFunction toProtobuf() {
    ConsistentPartitionWindowRel.WindowRelFunction.Builder windowBuilder =
        ConsistentPartitionWindowRel.WindowRelFunction.newBuilder();
    windowBuilder.setFunctionReference(functionId);
    if (ignoreNulls) {
      FunctionOption option = FunctionOption.newBuilder().setName("ignoreNulls").build();
      windowBuilder.addOptions(option);
    }
    for (ExpressionNode expressionNode : expressionNodes) {
      FunctionArgument.Builder functionArgument = FunctionArgument.newBuilder();
      functionArgument.setValue(expressionNode.toProtobuf());
      windowBuilder.addArguments(functionArgument.build());
    }
    windowBuilder.setOutputType(outputTypeNode.toProtobuf());

    windowBuilder.setColumnName(columnName);
    // Set upper bound and lower bound
    Expression.WindowFunction.Bound.Builder lowerBoundBuilder =
        Expression.WindowFunction.Bound.newBuilder();

    Expression.WindowFunction.Bound.Builder upperBoundBuilder =
        Expression.WindowFunction.Bound.newBuilder();
    windowBuilder.setLowerBound(setBound(lowerBoundBuilder, lowerBound).build());
    windowBuilder.setUpperBound(setBound(upperBoundBuilder, upperBound).build());
    windowBuilder.setBoundsType(getBoundsType(frameType));
    return windowBuilder.build();
  }
}

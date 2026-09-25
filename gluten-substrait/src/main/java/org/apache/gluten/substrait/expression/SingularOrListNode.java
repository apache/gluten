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

import io.substrait.proto.Expression;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SingularOrListNode implements ExpressionNode, Serializable {
  private final ExpressionNode value;
  private final List<ExpressionNode> listNodes = new ArrayList<>();
  // rawValues and dataType allow delaying literal node construction until toProtobuf()
  private final List<Object> rawValues;
  private final DataType dataType;

  SingularOrListNode(ExpressionNode value, List<ExpressionNode> listNodes) {
    this.value = value;
    this.listNodes.addAll(listNodes);
    this.rawValues = null;
    this.dataType = null;
  }

  SingularOrListNode(ExpressionNode value, List<Object> rawValues, DataType dataType) {
    this.value = value;
    this.rawValues = new ArrayList<>(rawValues);
    this.dataType = dataType;
  }

  @Override
  public Expression toProtobuf() {
    Expression.SingularOrList.Builder builder = Expression.SingularOrList.newBuilder();
    builder.setValue(value.toProtobuf());
    if (!listNodes.isEmpty()) {
      for (ExpressionNode expressionNode : listNodes) {
        builder.addOptions(expressionNode.toProtobuf());
      }
    } else if (rawValues != null) {
      for (Object obj : rawValues) {
        // construct a temporary LiteralNode and convert to protobuf to avoid keeping
        // many LiteralNode objects in memory at once. Use per-value nullability.
        LiteralNode literalNode =
            (LiteralNode) ExpressionBuilder.makeLiteral(obj, dataType, obj == null);
        builder.addOptions(literalNode.toProtobuf());
      }
    }
    Expression.Builder expressionBuilder = Expression.newBuilder();
    expressionBuilder.setSingularOrList(builder);
    return expressionBuilder.build();
  }
}

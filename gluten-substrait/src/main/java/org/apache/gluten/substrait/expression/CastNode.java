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

import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;

import java.io.Serializable;

public class CastNode implements ExpressionNode, Serializable {
  private final TypeNode typeNode;
  private final ExpressionNode expressionNode;

  // Substrait Cast FailureBehavior:
  // 0 = UNSPECIFIED (Spark LEGACY: allow overflow/truncation)
  // 1 = RETURN_NULL (Spark TRY: return null on failure)
  // 2 = THROW_EXCEPTION (Spark ANSI: throw on overflow)
  public final int failureBehavior;

  CastNode(TypeNode typeNode, ExpressionNode expressionNode, int failureBehavior) {
    this.typeNode = typeNode;
    this.expressionNode = expressionNode;
    this.failureBehavior = failureBehavior;
  }

  @Override
  public Expression toProtobuf() {
    Expression.Cast.Builder castBuilder = Expression.Cast.newBuilder();
    castBuilder.setType(typeNode.toProtobuf());
    castBuilder.setInput(expressionNode.toProtobuf());
    castBuilder.setFailureBehaviorValue(failureBehavior);
    Expression.Builder builder = Expression.newBuilder();
    builder.setCast(castBuilder.build());
    return builder.build();
  }
}

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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.substrait.expression.ExpressionNode;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;

import io.substrait.proto.NestedLoopJoinRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NestedLoopJoinRelNode implements RelNode, Serializable {
  private final RelNode left;
  private final RelNode right;
  private final NestedLoopJoinRel.JoinType joinType;
  private final ExpressionNode expression;
  private final AdvancedExtensionNode extensionNode;

  NestedLoopJoinRelNode(
      RelNode left,
      RelNode right,
      NestedLoopJoinRel.JoinType joinType,
      ExpressionNode expression,
      AdvancedExtensionNode extensionNode) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.expression = expression;
    this.extensionNode = extensionNode;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    NestedLoopJoinRel.Builder nestedLoopJoinRelBuilder = NestedLoopJoinRel.newBuilder();
    nestedLoopJoinRelBuilder.setCommon(relCommonBuilder.build());

    nestedLoopJoinRelBuilder.setType(joinType);

    if (left != null) {
      nestedLoopJoinRelBuilder.setLeft(left.toProtobuf());
    }
    if (right != null) {
      nestedLoopJoinRelBuilder.setRight(right.toProtobuf());
    }
    if (expression != null) {
      nestedLoopJoinRelBuilder.setExpression(expression.toProtobuf());
    }
    if (extensionNode != null) {
      nestedLoopJoinRelBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }
    return Rel.newBuilder().setNestedLoopJoin(nestedLoopJoinRelBuilder.build()).build();
  }

  @Override
  public List<RelNode> childNodes() {
    List<RelNode> children = new ArrayList<>();
    children.add(left);
    children.add(right);
    return children;
  }
}

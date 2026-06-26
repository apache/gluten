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

import io.substrait.proto.CrossRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CrossRelNode implements RelNode, Serializable {
  private final RelNode left;
  private final RelNode right;
  private final CrossRel.JoinType joinType;
  private final ExpressionNode expression;
  private final AdvancedExtensionNode extensionNode;

  CrossRelNode(
      RelNode left,
      RelNode right,
      CrossRel.JoinType joinType,
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

    CrossRel.Builder crossRelBuilder = CrossRel.newBuilder();
    crossRelBuilder.setCommon(relCommonBuilder.build());

    crossRelBuilder.setType(joinType);

    if (left != null) {
      crossRelBuilder.setLeft(left.toProtobuf());
    }
    if (right != null) {
      crossRelBuilder.setRight(right.toProtobuf());
    }
    if (expression != null) {
      crossRelBuilder.setExpression(expression.toProtobuf());
    }
    if (extensionNode != null) {
      crossRelBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }
    return Rel.newBuilder().setCross(crossRelBuilder.build()).build();
  }

  @Override
  public List<RelNode> childNode() {
    List<RelNode> children = new ArrayList<>();
    children.add(left);
    children.add(right);
    return children;
  }
}

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

import io.substrait.proto.JoinRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;

public class JoinRelNode implements RelNode, Serializable {
  private final RelNode left;
  private final RelNode right;
  private final JoinRel.JoinType joinType;
  private final ExpressionNode expression;
  private final ExpressionNode postJoinFilter;
  private final AdvancedExtensionNode extensionNode;
  private final String hashTableId;

  JoinRelNode(
      RelNode left,
      RelNode right,
      JoinRel.JoinType joinType,
      ExpressionNode expression,
      ExpressionNode postJoinFilter,
      String hashTableId,
      AdvancedExtensionNode extensionNode) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.expression = expression;
    this.postJoinFilter = postJoinFilter;
    this.hashTableId = hashTableId;
    this.extensionNode = extensionNode;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());
    JoinRel.Builder joinBuilder = JoinRel.newBuilder();

    joinBuilder.setType(joinType);

    if (left != null) {
      joinBuilder.setLeft(left.toProtobuf());
    }
    if (right != null) {
      joinBuilder.setRight(right.toProtobuf());
    }
    if (expression != null) {
      joinBuilder.setExpression(expression.toProtobuf());
    }
    if (postJoinFilter != null) {
      joinBuilder.setPostJoinFilter(postJoinFilter.toProtobuf());
    }
    if (extensionNode != null) {
      joinBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    joinBuilder.setHashTableId(hashTableId);

    return Rel.newBuilder().setJoin(joinBuilder.build()).build();
  }
}

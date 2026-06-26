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

import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.SortField;
import io.substrait.proto.WindowGroupLimitRel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WindowGroupLimitRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final List<ExpressionNode> partitionExpressions = new ArrayList<>();
  private final List<SortField> sorts = new ArrayList<>();
  private final AdvancedExtensionNode extensionNode;
  private final Integer limit;

  public WindowGroupLimitRelNode(
      RelNode input,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      Integer limit) {
    this.input = input;
    this.partitionExpressions.addAll(partitionExpressions);
    this.sorts.addAll(sorts);
    this.limit = limit;
    this.extensionNode = null;
  }

  public WindowGroupLimitRelNode(
      RelNode input,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      Integer limit,
      AdvancedExtensionNode extensionNode) {
    this.input = input;
    this.partitionExpressions.addAll(partitionExpressions);
    this.sorts.addAll(sorts);
    this.limit = limit;
    this.extensionNode = extensionNode;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    WindowGroupLimitRel.Builder windowBuilder = WindowGroupLimitRel.newBuilder();
    windowBuilder.setCommon(relCommonBuilder.build());
    if (input != null) {
      windowBuilder.setInput(input.toProtobuf());
    }

    for (int i = 0; i < partitionExpressions.size(); i++) {
      windowBuilder.addPartitionExpressions(i, partitionExpressions.get(i).toProtobuf());
    }

    for (int i = 0; i < sorts.size(); i++) {
      windowBuilder.addSorts(i, sorts.get(i));
    }

    windowBuilder.setLimit(limit);

    if (extensionNode != null) {
      windowBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }
    Rel.Builder builder = Rel.newBuilder();
    builder.setWindowGroupLimit(windowBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return Collections.singletonList(input);
  }
}

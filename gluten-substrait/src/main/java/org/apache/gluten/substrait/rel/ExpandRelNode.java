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

import io.substrait.proto.ExpandRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExpandRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final List<List<ExpressionNode>> projections = new ArrayList<>();

  private final AdvancedExtensionNode extensionNode;

  public ExpandRelNode(
      RelNode input, List<List<ExpressionNode>> projections, AdvancedExtensionNode extensionNode) {
    this.input = input;
    this.projections.addAll(projections);
    this.extensionNode = extensionNode;
  }

  public ExpandRelNode(RelNode input, List<List<ExpressionNode>> projections) {
    this.input = input;
    this.projections.addAll(projections);
    this.extensionNode = null;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    ExpandRel.Builder expandBuilder = ExpandRel.newBuilder();
    expandBuilder.setCommon(relCommonBuilder.build());

    if (input != null) {
      expandBuilder.setInput(input.toProtobuf());
    }

    for (List<ExpressionNode> projectList : projections) {
      ExpandRel.ExpandField.Builder expandFieldBuilder = ExpandRel.ExpandField.newBuilder();
      ExpandRel.SwitchingField.Builder switchingField = ExpandRel.SwitchingField.newBuilder();
      for (ExpressionNode exprNode : projectList) {
        switchingField.addDuplicates(exprNode.toProtobuf());
      }
      expandFieldBuilder.setSwitchingField(switchingField.build());
      expandBuilder.addFields(expandFieldBuilder.build());
    }

    if (extensionNode != null) {
      expandBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    Rel.Builder builder = Rel.newBuilder();
    builder.setExpand(expandBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return Collections.singletonList(input);
  }
}

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

import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProjectRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final List<ExpressionNode> expressionNodes = new ArrayList<>();
  private final AdvancedExtensionNode extensionNode;
  private final int emitStartIndex;

  ProjectRelNode(RelNode input, List<ExpressionNode> expressionNodes, int emitStartIndex) {
    this.input = input;
    this.expressionNodes.addAll(expressionNodes);
    this.extensionNode = null;
    this.emitStartIndex = emitStartIndex;
  }

  ProjectRelNode(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      AdvancedExtensionNode extensionNode,
      int emitStartIndex) {
    this.input = input;
    this.expressionNodes.addAll(expressionNodes);
    this.extensionNode = extensionNode;
    this.emitStartIndex = emitStartIndex;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    if (emitStartIndex < 0) {
      relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());
    } else {
      RelCommon.Emit.Builder emitBuilder = RelCommon.Emit.newBuilder();
      for (int i = 0; i < expressionNodes.size(); i++) {
        emitBuilder.addOutputMapping(i + emitStartIndex);
      }
      relCommonBuilder.setEmit(emitBuilder.build());
    }
    ProjectRel.Builder projectBuilder = ProjectRel.newBuilder();
    projectBuilder.setCommon(relCommonBuilder.build());
    if (input != null) {
      projectBuilder.setInput(input.toProtobuf());
    }
    for (ExpressionNode expressionNode : expressionNodes) {
      projectBuilder.addExpressions(expressionNode.toProtobuf());
    }
    if (extensionNode != null) {
      projectBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }
    Rel.Builder builder = Rel.newBuilder();
    builder.setProject(projectBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return Collections.singletonList(input);
  }
}

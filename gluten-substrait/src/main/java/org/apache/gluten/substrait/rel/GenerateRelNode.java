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

import io.substrait.proto.GenerateRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class GenerateRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final ExpressionNode generator;
  private final List<ExpressionNode> childOutput;
  private final AdvancedExtensionNode extensionNode;
  private final boolean outer;

  GenerateRelNode(
      RelNode input, ExpressionNode generator, List<ExpressionNode> childOutput, boolean outer) {
    this(input, generator, childOutput, null, outer);
  }

  GenerateRelNode(
      RelNode input,
      ExpressionNode generator,
      List<ExpressionNode> childOutput,
      AdvancedExtensionNode extensionNode,
      boolean outer) {
    this.input = input;
    this.generator = generator;
    this.childOutput = childOutput;
    this.extensionNode = extensionNode;
    this.outer = outer;
  }

  @Override
  public Rel toProtobuf() {
    GenerateRel.Builder generateRelBuilder = GenerateRel.newBuilder();

    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());
    generateRelBuilder.setCommon(relCommonBuilder.build());

    if (input != null) {
      generateRelBuilder.setInput(input.toProtobuf());
    }

    if (generator != null) {
      generateRelBuilder.setGenerator(generator.toProtobuf());
    }

    for (ExpressionNode node : childOutput) {
      generateRelBuilder.addChildOutput(node.toProtobuf());
    }

    generateRelBuilder.setOuter(outer);

    if (extensionNode != null) {
      generateRelBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    Rel.Builder relBuilder = Rel.newBuilder();
    relBuilder.setGenerate(generateRelBuilder.build());
    return relBuilder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return Collections.singletonList(input);
  }
}

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
package org.apache.gluten.substrait.plan;

import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.extensions.FunctionMappingNode;
import org.apache.gluten.substrait.rel.RelNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.RelRoot;

import java.io.Serializable;
import java.util.List;

public class PlanNode implements Serializable {
  private final List<FunctionMappingNode> mappingNodes;
  private final List<RelNode> relNodes;
  private final List<String> outNames;

  private TypeNode outputSchema = null;
  private AdvancedExtensionNode extension = null;

  PlanNode(
      List<FunctionMappingNode> mappingNodes,
      List<RelNode> relNodes,
      List<String> outNames,
      TypeNode outputSchema,
      AdvancedExtensionNode extension) {
    this.mappingNodes = mappingNodes;
    this.relNodes = relNodes;
    this.outNames = outNames;
    this.outputSchema = outputSchema;
    this.extension = extension;
  }

  public Plan toProtobuf() {
    Plan.Builder planBuilder = Plan.newBuilder();
    // add the extension functions
    for (FunctionMappingNode mappingNode : mappingNodes) {
      planBuilder.addExtensions(mappingNode.toProtobuf());
    }

    for (RelNode relNode : relNodes) {
      PlanRel.Builder planRelBuilder = PlanRel.newBuilder();

      RelRoot.Builder relRootBuilder = RelRoot.newBuilder();
      relRootBuilder.setInput(relNode.toProtobuf());
      for (String name : outNames) {
        relRootBuilder.addNames(name);
      }
      if (outputSchema != null) {
        relRootBuilder.setOutputSchema(outputSchema.toProtobuf().getStruct());
      }
      planRelBuilder.setRoot(relRootBuilder.build());

      planBuilder.addRelations(planRelBuilder.build());
    }

    if (extension != null) {
      planBuilder.setAdvancedExtensions(extension.toProtobuf());
    }
    return planBuilder.build();
  }

  public List<RelNode> getRelNodes() {
    return relNodes;
  }
}

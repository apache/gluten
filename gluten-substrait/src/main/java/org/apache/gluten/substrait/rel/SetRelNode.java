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

import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;

import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.SetRel;

import java.io.Serializable;
import java.util.List;

public class SetRelNode implements RelNode, Serializable {
  private final List<RelNode> inputs;
  private final SetRel.SetOp setOp;
  private final AdvancedExtensionNode extensionNode;

  public SetRelNode(List<RelNode> inputs, SetRel.SetOp setOp, AdvancedExtensionNode extensionNode) {
    this.inputs = inputs;
    this.setOp = setOp;
    this.extensionNode = extensionNode;
  }

  public SetRelNode(List<RelNode> inputs, SetRel.SetOp setOp) {
    this(inputs, setOp, null);
  }

  @Override
  public Rel toProtobuf() {
    final RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());
    final SetRel.Builder setBuilder = SetRel.newBuilder();
    setBuilder.setCommon(relCommonBuilder.build());
    if (inputs != null) {
      for (RelNode input : inputs) {
        setBuilder.addInputs(input.toProtobuf());
      }
    }
    setBuilder.setOp(setOp);
    if (extensionNode != null) {
      setBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }
    final Rel.Builder builder = Rel.newBuilder();
    builder.setSet(setBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return inputs;
  }
}

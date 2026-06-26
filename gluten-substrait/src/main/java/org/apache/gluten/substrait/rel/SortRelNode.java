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
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class SortRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final List<SortField> sorts;
  private final AdvancedExtensionNode extensionNode;

  public SortRelNode(RelNode input, List<SortField> sorts, AdvancedExtensionNode extensionNode) {
    this.input = input;
    this.sorts = sorts;
    this.extensionNode = extensionNode;
  }

  public SortRelNode(RelNode input, List<SortField> sorts) {
    this.input = input;
    this.sorts = sorts;
    this.extensionNode = null;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    SortRel.Builder sortBuilder = SortRel.newBuilder();
    sortBuilder.setCommon(relCommonBuilder.build());

    if (input != null) {
      sortBuilder.setInput(input.toProtobuf());
    }

    for (int i = 0; i < sorts.size(); i++) {
      sortBuilder.addSorts(i, sorts.get(i));
    }

    if (extensionNode != null) {
      sortBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    Rel.Builder builder = Rel.newBuilder();
    builder.setSort(sortBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return Collections.singletonList(input);
  }
}

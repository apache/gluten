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

import org.apache.gluten.substrait.expression.ExpressionBuilder;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;

import io.substrait.proto.FetchMode;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.SortField;
import io.substrait.proto.TopNRel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TopNNode implements RelNode, Serializable {
  private final RelNode input;
  private final Long count;
  private final List<SortField> sorts = new ArrayList<>();
  private final AdvancedExtensionNode extensionNode;

  public TopNNode(RelNode input, Long count, List<SortField> sorts) {
    this.input = input;
    this.count = count;
    this.sorts.addAll(sorts);
    this.extensionNode = null;
  }

  public TopNNode(
      RelNode input, Long count, List<SortField> sorts, AdvancedExtensionNode extensionNode) {
    this.input = input;
    this.count = count;
    this.sorts.addAll(sorts);
    this.extensionNode = extensionNode;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    TopNRel.Builder topNBuilder = TopNRel.newBuilder();
    topNBuilder.setCommon(relCommonBuilder.build());

    if (input != null) {
      topNBuilder.setInput(input.toProtobuf());
    }

    // Substrait's TopNRel expresses the row limit as an `Expression count`; the only Gluten
    // producer (TakeOrderedAndProject) supplies a literal Long, so wrap it as an i64 literal.
    // FetchMode has no meaningful proto3 default (FETCH_MODE_UNSPECIFIED is not a valid producer
    // choice), so set it explicitly; Gluten never emits WITH TIES.
    topNBuilder.setCount(ExpressionBuilder.makeLongLiteral(count).toProtobuf());
    topNBuilder.setMode(FetchMode.FETCH_MODE_ROWS_ONLY);

    for (int i = 0; i < sorts.size(); i++) {
      topNBuilder.addSorts(i, sorts.get(i));
    }

    if (extensionNode != null) {
      topNBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    Rel.Builder relBuilder = Rel.newBuilder();
    relBuilder.setTopN(topNBuilder.build());
    return relBuilder.build();
  }

  @Override
  public List<RelNode> childNodes() {
    return Collections.singletonList(input);
  }
}

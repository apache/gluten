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

import com.google.protobuf.StringValue;
import io.substrait.proto.AdvancedExtension;
import io.substrait.proto.FetchRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Substrait representation of Gluten's native GlutenStride operator.
 *
 * <p>The operator outputs every {@code stride}-th row from each input batch (rows at indices 0,
 * stride, 2*stride, …). It is deterministic, stateless, and has no equivalent in standard Spark or
 * Velox, making it a clean demonstration of the Gluten custom-operator mechanism.
 *
 * <h3>Wire encoding</h3>
 *
 * <p>We reuse {@link FetchRel} to avoid introducing a new proto type:
 *
 * <ul>
 *   <li>{@code FetchRel.offset} = {@code stride} (the step size, e.g. 2 means keep every other row)
 *   <li>{@code FetchRel.count} = 0 (unused)
 *   <li>{@code AdvancedExtension.optimization[0]} = {@code "isGlutenStride=1"} — the marker that
 *       tells the C++ dispatcher to build a {@code GlutenStrideNode} instead of a {@code LimitNode}
 * </ul>
 *
 * <h3>C++ side</h3>
 *
 * <p>{@code SubstraitToVeloxPlanConverter::toVeloxPlan(FetchRel&)} checks for {@code
 * isGlutenStride=1} and constructs {@code GlutenStrideNode(stride, child)}, which is executed by
 * {@code GlutenStrideOperator}.
 */
public class GlutenStrideRelNode implements RelNode, Serializable {

  /** Optimization marker recognized by the C++ Substrait converter. */
  private static final String MARKER = "isGlutenStride=1";

  private final RelNode input;
  private final long stride;
  // Present only in validation mode; carries input column types for the native validator.
  private final AdvancedExtensionNode extensionNode;

  GlutenStrideRelNode(RelNode input, long stride) {
    this.input = input;
    this.stride = stride;
    this.extensionNode = null;
  }

  GlutenStrideRelNode(RelNode input, long stride, AdvancedExtensionNode extensionNode) {
    this.input = input;
    this.stride = stride;
    this.extensionNode = extensionNode;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder =
        RelCommon.newBuilder().setDirect(RelCommon.Direct.newBuilder());

    // Build the marker optimization Any.
    com.google.protobuf.Any markerAny =
        com.google.protobuf.Any.pack(StringValue.newBuilder().setValue(MARKER).build());

    AdvancedExtension.Builder extBuilder = AdvancedExtension.newBuilder();
    extBuilder.addOptimization(markerAny);
    // Merge in the validation-mode enhancement if present.
    if (extensionNode != null) {
      AdvancedExtension baseExt = extensionNode.toProtobuf();
      if (baseExt.hasEnhancement()) {
        extBuilder.setEnhancement(baseExt.getEnhancement());
      }
    }

    FetchRel.Builder fetchBuilder =
        FetchRel.newBuilder()
            .setCommon(relCommonBuilder.build())
            .setOffset(stride) // stride is stored in offset
            .setCount(0L) // unused
            .setAdvancedExtension(extBuilder.build());

    if (input != null) {
      fetchBuilder.setInput(input.toProtobuf());
    }

    return Rel.newBuilder().setFetch(fetchBuilder.build()).build();
  }

  @Override
  public List<RelNode> childNodes() {
    return Collections.singletonList(input);
  }
}

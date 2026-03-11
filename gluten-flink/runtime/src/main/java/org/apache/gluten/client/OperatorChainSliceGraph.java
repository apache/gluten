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
package org.apache.gluten.client;

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OperatorChainSliceGraph {
  private Map<Integer, OperatorChainSlice> slices;

  public OperatorChainSliceGraph() {
    slices = new HashMap<>();
  }

  public void addSlice(Integer id, OperatorChainSlice chainSlice) {
    slices.put(id, chainSlice);
  }

  public OperatorChainSlice getSlice(Integer id) {
    return slices.get(id);
  }

  public OperatorChainSlice getSourceSlice() {
    List<OperatorChainSlice> sourceCandidates = new ArrayList<>();

    for (OperatorChainSlice chainSlice : slices.values()) {
      if (chainSlice.getInputs().isEmpty()) {
        sourceCandidates.add(chainSlice);
      }
    }

    Preconditions.checkState(
        sourceCandidates.size() == 1,
        "Expected exactly one source operator chain slice with empty inputs, but found %s",
        sourceCandidates.size());

    return sourceCandidates.get(0);
  }

  public Map<Integer, OperatorChainSlice> getSlices() {
    return slices;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (OperatorChainSlice chainSlice : slices.values()) {
      sb.append("Slice ID: ")
          .append(chainSlice.id())
          .append(", offloadable: ")
          .append(chainSlice.isOffloadable())
          .append("\n");
      sb.append("  Inputs: ").append(chainSlice.getInputs()).append("\n");
      sb.append("  Outputs: ").append(chainSlice.getOutputs()).append("\n");
      String operatorConfigs =
          chainSlice.getOperatorConfigs().stream()
              .map(config -> config.getOperatorName() + "(" + config.getVertexID() + ")")
              .collect(Collectors.joining(", "));
      sb.append("  Operator Configs: ").append(operatorConfigs).append("\n");
    }
    return sb.toString();
  }
}

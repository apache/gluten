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

import org.apache.gluten.util.Utils;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates an OperatorChainSliceGraph from a JobVertex by analyzing its operator chain structure.
 * The graph represents operator slices and their relationships, identifying which slices are
 * offloadable to Gluten.
 */
class OperatorChainSliceGraphGenerator {
  private OperatorChainSliceGraph chainSliceGraph;
  private final JobVertex jobVertex;
  private Map<Integer, StreamConfig> chainedConfigs;
  private final ClassLoader userClassLoader;

  public OperatorChainSliceGraphGenerator(JobVertex jobVertex, ClassLoader userClassLoader) {
    this.jobVertex = jobVertex;
    this.userClassLoader = userClassLoader;
  }

  /**
   * Generates and returns the OperatorChainSliceGraph for the JobVertex. The graph is generated
   * lazily on first call and cached.
   *
   * @return the generated OperatorChainSliceGraph
   */
  public OperatorChainSliceGraph getGraph() {
    if (chainSliceGraph == null) {
      generateGraph();
    }
    return chainSliceGraph;
  }

  /** Generates the OperatorChainSliceGraph by traversing the operator chain. */
  private void generateGraph() {
    chainSliceGraph = new OperatorChainSliceGraph();
    StreamConfig rootConfig = new StreamConfig(jobVertex.getConfiguration());

    chainedConfigs = collectChainedConfigs(rootConfig);
    OperatorChainSlice rootSlice = createRootSlice(rootConfig);
    chainSliceGraph.addSlice(rootSlice.id(), rootSlice);

    traverseOperatorChain(rootSlice, rootConfig);
  }

  /** Collects all chained operator configurations from the root configuration. */
  private Map<Integer, StreamConfig> collectChainedConfigs(StreamConfig rootConfig) {
    Map<Integer, StreamConfig> configs = new HashMap<>();
    rootConfig
        .getTransitiveChainedTaskConfigs(userClassLoader)
        .forEach((id, config) -> configs.put(id, new StreamConfig(config.getConfiguration())));
    configs.put(rootConfig.getVertexID(), rootConfig);
    return configs;
  }

  /** Creates the root operator chain slice. */
  private OperatorChainSlice createRootSlice(StreamConfig rootConfig) {
    OperatorChainSlice rootSlice = new OperatorChainSlice(rootConfig.getVertexID());
    rootSlice.setOffloadable(isOffloadableOperator(rootConfig));
    rootSlice.getOperatorConfigs().add(rootConfig);
    return rootSlice;
  }

  /** Traverses the operator chain recursively, creating slices for each operator. */
  private void traverseOperatorChain(OperatorChainSlice currentSlice, StreamConfig currentConfig) {
    List<StreamEdge> outputEdges = currentConfig.getChainedOutputs(userClassLoader);
    if (outputEdges == null || outputEdges.isEmpty()) {
      return;
    }

    for (StreamEdge edge : outputEdges) {
      Integer targetId = edge.getTargetId();
      StreamConfig childConfig = chainedConfigs.get(targetId);
      processChildOperator(currentSlice, childConfig);
    }
  }

  /**
   * Processes a child operator, creating a new slice if not already visited. Note: We don't
   * coalesce operators into the same velox plan at present. Each operator is a separate velox plan.
   */
  private void processChildOperator(OperatorChainSlice parentSlice, StreamConfig childConfig) {
    Integer childId = childConfig.getVertexID();
    OperatorChainSlice childSlice = chainSliceGraph.getSlice(childId);

    if (childSlice == null) {
      // First visit: create new slice and continue traversal
      childSlice = createChildSlice(childConfig);
      chainSliceGraph.addSlice(childId, childSlice);
      connectSlices(parentSlice, childSlice);
      traverseOperatorChain(childSlice, childConfig);
    } else {
      // Already visited: just connect the slices
      connectSlices(parentSlice, childSlice);
    }
  }

  /** Creates a new operator chain slice for a child operator. */
  private OperatorChainSlice createChildSlice(StreamConfig childConfig) {
    OperatorChainSlice childSlice = new OperatorChainSlice(childConfig.getVertexID());
    childSlice.setOffloadable(isOffloadableOperator(childConfig));
    childSlice.getOperatorConfigs().add(childConfig);
    return childSlice;
  }

  /** Connects two operator chain slices by adding their relationship. */
  private void connectSlices(OperatorChainSlice parentSlice, OperatorChainSlice childSlice) {
    parentSlice.getOutputs().add(childSlice.id());
    childSlice.getInputs().add(parentSlice.id());
  }

  /**
   * Checks if an operator is offloadable to Gluten.
   *
   * @param opConfig the operator configuration to check
   * @return true if the operator is offloadable, false otherwise
   */
  private boolean isOffloadableOperator(StreamConfig opConfig) {
    return Utils.getGlutenOperator(opConfig, userClassLoader).isPresent();
  }
}

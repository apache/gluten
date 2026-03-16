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

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.keyselector.GlutenKeySelector;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;
import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;
import org.apache.gluten.table.runtime.typeutils.GlutenStatefulRecordSerializer;
import org.apache.gluten.util.Utils;

import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
 * Generates an offloaded JobGraph by transforming offloadable operators to Gluten operators.
 * Main workflow:
 * 1. For each JobVertex, generate an OperatorChainSliceGraph to identify offloadable slices.
 * 2. Recursively visit chain slices: create offloaded operators for offloadable slices,
 *    keep original operators for unoffloadable ones.
 * 3. For offloadable operators: update input/output serializers and state partitioners
 *    based on upstream/downstream operator capabilities.
 * 4. Update stream edges and serialize all operator configurations.
 */
public class OffloadedJobGraphGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadedJobGraphGenerator.class);
  private final JobGraph jobGraph;
  private final ClassLoader userClassloader;
  private boolean hasGenerated = false;

  public OffloadedJobGraphGenerator(JobGraph jobGraph, ClassLoader userClassloader) {
    this.jobGraph = jobGraph;
    this.userClassloader = userClassloader;
  }

  public JobGraph generate() {
    if (hasGenerated) {
      throw new IllegalStateException("JobGraph has been generated.");
    }

    for (JobVertex jobVertex : jobGraph.getVertices()) {
      offloadJobVertex(jobVertex);
    }
    hasGenerated = true;
    return jobGraph;
  }

  private void offloadJobVertex(JobVertex jobVertex) {
    OperatorChainSliceGraphGenerator graphGenerator =
        new OperatorChainSliceGraphGenerator(jobVertex, userClassloader);
    OperatorChainSliceGraph chainSliceGraph = graphGenerator.getGraph();
    LOG.info("OperatorChainSliceGraph:\n{}", chainSliceGraph);

    OperatorChainSlice sourceChainSlice = chainSliceGraph.getSourceSlice();
    OperatorChainSliceGraph offloadedChainSliceGraph = new OperatorChainSliceGraph();
    visitAndOffloadChainOperators(
        sourceChainSlice, chainSliceGraph, offloadedChainSliceGraph, 0, jobVertex);
    visitAndUpdateStreamEdges(sourceChainSlice, chainSliceGraph, offloadedChainSliceGraph);
    serializeAllOperatorsConfigs(offloadedChainSliceGraph);

    StreamConfig sourceConfig = sourceChainSlice.getOperatorConfigs().get(0);
    StreamConfig offloadedSourceConfig =
        offloadedChainSliceGraph.getSlice(sourceChainSlice.id()).getOperatorConfigs().get(0);

    Map<Integer, StreamConfig> chainedConfigs =
        collectChainedConfigs(sourceChainSlice, offloadedChainSliceGraph);
    updateSourceConfigIfOffloadable(
        sourceConfig, offloadedSourceConfig, sourceChainSlice, chainedConfigs);
    sourceConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfigs);
    sourceConfig.serializeAllConfigs();
  }

  // Process and offload operator chain slices recursively
  private void visitAndOffloadChainOperators(
      OperatorChainSlice sourceChainSlice,
      OperatorChainSliceGraph sourceChainSliceGraph,
      OperatorChainSliceGraph offloadedChainSliceGraph,
      Integer chainIndex,
      JobVertex jobVertex) {
    OperatorChainSlice processedChainSlice;
    if (sourceChainSlice.isOffloadable()) {
      processedChainSlice =
          createOffloadedOperatorChainSlice(
              sourceChainSliceGraph, sourceChainSlice, chainIndex, jobVertex);
      chainIndex = chainIndex + 1;
    } else {
      processedChainSlice = createUnoffloadableOperatorChainSlice(sourceChainSlice, chainIndex);
      chainIndex = chainIndex + sourceChainSlice.getOperatorConfigs().size();
    }

    processedChainSlice.getInputs().addAll(sourceChainSlice.getInputs());
    processedChainSlice.getOutputs().addAll(sourceChainSlice.getOutputs());
    offloadedChainSliceGraph.addSlice(sourceChainSlice.id(), processedChainSlice);

    // Recursively process downstream chain slices
    for (Integer downstreamSliceId : sourceChainSlice.getOutputs()) {
      OperatorChainSlice downstreamSourceSlice = sourceChainSliceGraph.getSlice(downstreamSliceId);
      OperatorChainSlice downstreamProcessedSlice =
          offloadedChainSliceGraph.getSlice(downstreamSliceId);
      if (downstreamProcessedSlice == null) {
        visitAndOffloadChainOperators(
            downstreamSourceSlice,
            sourceChainSliceGraph,
            offloadedChainSliceGraph,
            chainIndex,
            jobVertex);
      }
    }
  }

  // Keep the original operator chain slice as is.
  private OperatorChainSlice createUnoffloadableOperatorChainSlice(
      OperatorChainSlice sourceChainSlice, Integer chainIndex) {
    OperatorChainSlice unoffloadableChainSlice = new OperatorChainSlice(sourceChainSlice.id());
    List<StreamConfig> operatorConfigs = sourceChainSlice.getOperatorConfigs();
    for (StreamConfig opConfig : operatorConfigs) {
      StreamConfig newOpConfig = new StreamConfig(new Configuration(opConfig.getConfiguration()));
      newOpConfig.setChainIndex(chainIndex);
      unoffloadableChainSlice.getOperatorConfigs().add(newOpConfig);
    }
    unoffloadableChainSlice.setOffloadable(false);
    return unoffloadableChainSlice;
  }

  // Create offloadable operator chain slice, and update the input/output channel serializers
  private OperatorChainSlice createOffloadedOperatorChainSlice(
      OperatorChainSliceGraph chainSliceGraph,
      OperatorChainSlice sourceChainSlice,
      Integer chainIndex,
      JobVertex jobVertex) {
    OperatorChainSlice offloadedChainSlice = new OperatorChainSlice(sourceChainSlice.id());
    List<StreamConfig> operatorConfigs = sourceChainSlice.getOperatorConfigs();

    // May coalesce multiple operators into one in the future.
    if (operatorConfigs.size() != 1) {
      throw new UnsupportedOperationException(
          "Only one operator is supported for offloaded operator chain slice.");
    }

    StreamConfig sourceOpConfig = operatorConfigs.get(0);
    GlutenOperator sourceOperator = Utils.getGlutenOperator(sourceOpConfig, userClassloader).get();
    StatefulPlanNode planNode = sourceOperator.getPlanNode();
    // Create a new operator config for the offloaded operator.
    StreamConfig offloadedOpConfig =
        new StreamConfig(new Configuration(sourceOpConfig.getConfiguration()));
    if (sourceOperator instanceof GlutenStreamSource) {
      boolean supportsVectorOutput =
          supportsVectorOutput(sourceChainSlice, chainSliceGraph, jobVertex);
      Class<?> outClass = supportsVectorOutput ? StatefulRecord.class : RowData.class;
      GlutenStreamSource newSourceOp =
          new GlutenStreamSource(
              new GlutenSourceFunction<>(
                  planNode,
                  sourceOperator.getOutputTypes(),
                  sourceOperator.getId(),
                  ((GlutenStreamSource) sourceOperator).getConnectorSplit(),
                  outClass));
      offloadedOpConfig.setStreamOperator(newSourceOp);
      if (supportsVectorOutput) {
        setOffloadedOutputSerializer(offloadedOpConfig, sourceOperator);
      }
    } else if (sourceOperator instanceof GlutenOneInputOperator) {
      createOffloadedOneInputOperator(
          sourceChainSlice,
          chainSliceGraph,
          jobVertex,
          planNode,
          (GlutenOneInputOperator<?, ?>) sourceOperator,
          sourceOpConfig,
          offloadedOpConfig);
    } else if (sourceOperator instanceof GlutenTwoInputOperator) {
      createOffloadedTwoInputOperator(
          sourceChainSlice,
          chainSliceGraph,
          jobVertex,
          planNode,
          (GlutenTwoInputOperator<?, ?>) sourceOperator,
          sourceOpConfig,
          offloadedOpConfig);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported operator type for offloading: " + sourceOperator.getClass().getName());
    }

    offloadedOpConfig.setChainIndex(chainIndex);
    offloadedChainSlice.getOperatorConfigs().add(offloadedOpConfig);
    offloadedChainSlice.setOffloadable(true);
    return offloadedChainSlice;
  }

  private void createOffloadedOneInputOperator(
      OperatorChainSlice sourceChainSlice,
      OperatorChainSliceGraph chainSliceGraph,
      JobVertex jobVertex,
      StatefulPlanNode planNode,
      GlutenOneInputOperator<?, ?> sourceOperator,
      StreamConfig sourceOpConfig,
      StreamConfig offloadedOpConfig) {
    boolean supportsVectorOutput =
        supportsVectorOutput(sourceChainSlice, chainSliceGraph, jobVertex);
    boolean supportsVectorInput = supportsVectorInput(sourceChainSlice, chainSliceGraph, jobVertex);
    Class<?> inClass = supportsVectorInput ? StatefulRecord.class : RowData.class;
    Class<?> outClass = supportsVectorOutput ? StatefulRecord.class : RowData.class;
    GlutenOneInputOperator<?, ?> newOneInputOp =
        sourceOperator.cloneWithInputOutputClasses(inClass, outClass);
    offloadedOpConfig.setStreamOperator(newOneInputOp);
    if (supportsVectorOutput) {
      setOffloadedOutputSerializer(offloadedOpConfig, sourceOperator);
    }
    if (supportsVectorInput) {
      setOffloadedInputSerializer(offloadedOpConfig, sourceOperator);
      setOffloadedStatePartitioner(
          sourceOpConfig, offloadedOpConfig, 0, sourceOperator.getDescription());
    }
  }

  private void createOffloadedTwoInputOperator(
      OperatorChainSlice sourceChainSlice,
      OperatorChainSliceGraph chainSliceGraph,
      JobVertex jobVertex,
      StatefulPlanNode planNode,
      GlutenTwoInputOperator<?, ?> sourceOperator,
      StreamConfig sourceOpConfig,
      StreamConfig offloadedOpConfig) {
    boolean supportsVectorOutput =
        supportsVectorOutput(sourceChainSlice, chainSliceGraph, jobVertex);
    boolean supportsVectorInput = supportsVectorInput(sourceChainSlice, chainSliceGraph, jobVertex);
    setOffloadedStatePartitioner(
        sourceOpConfig, offloadedOpConfig, 0, sourceOperator.getDescription());
    setOffloadedStatePartitioner(
        sourceOpConfig, offloadedOpConfig, 1, sourceOperator.getDescription());
    Class<?> inClass = supportsVectorInput ? StatefulRecord.class : RowData.class;
    Class<?> outClass = supportsVectorOutput ? StatefulRecord.class : RowData.class;
    GlutenTwoInputOperator<?, ?> newTwoInputOp =
        new GlutenTwoInputOperator<>(
            planNode,
            sourceOperator.getLeftId(),
            sourceOperator.getRightId(),
            sourceOperator.getLeftInputType(),
            sourceOperator.getRightInputType(),
            sourceOperator.getOutputTypes(),
            inClass,
            outClass);
    offloadedOpConfig.setStreamOperator(newTwoInputOp);
    offloadedOpConfig.setStatePartitioner(0, new GlutenKeySelector());
    offloadedOpConfig.setStatePartitioner(1, new GlutenKeySelector());
    if (supportsVectorOutput) {
      setOffloadedOutputSerializer(offloadedOpConfig, sourceOperator);
    }
    if (supportsVectorInput) {
      setOffloadedInputSerializersForTwoInputOperator(offloadedOpConfig, sourceOperator);
    }
  }

  private void setOffloadedOutputSerializer(StreamConfig opConfig, GlutenOperator operator) {
    RowType rowType = operator.getOutputTypes().entrySet().iterator().next().getValue();
    opConfig.setTypeSerializerOut(new GlutenStatefulRecordSerializer(rowType, operator.getId()));
  }

  private void setOffloadedInputSerializer(StreamConfig opConfig, GlutenOperator operator) {
    opConfig.setupNetworkInputs(
        new GlutenStatefulRecordSerializer(operator.getInputType(), operator.getId()));
  }

  private void setOffloadedInputSerializersForTwoInputOperator(
      StreamConfig opConfig, GlutenTwoInputOperator<?, ?> operator) {
    opConfig.setupNetworkInputs(
        new GlutenStatefulRecordSerializer(operator.getLeftInputType(), operator.getId()),
        new GlutenStatefulRecordSerializer(operator.getRightInputType(), operator.getId()));
  }

  private void setOffloadedStatePartitioner(
      StreamConfig sourceOpConfig,
      StreamConfig offloadedOpConfig,
      int inputIndex,
      String operatorDescription) {
    KeySelector<?, ?> keySelector = sourceOpConfig.getStatePartitioner(inputIndex, userClassloader);
    if (keySelector != null) {
      LOG.info(
          "State partitioner ({}) found in input {} of operator {}, change it to GlutenKeySelector.",
          keySelector.getClass().getName(),
          inputIndex,
          operatorDescription);
      offloadedOpConfig.setStatePartitioner(inputIndex, new GlutenKeySelector());
    }
  }

  private StreamConfig findLastOperatorInChain(JobVertex vertex) {
    StreamConfig rootStreamConfig = new StreamConfig(vertex.getConfiguration());
    Map<Integer, StreamConfig> chainedConfigs =
        rootStreamConfig.getTransitiveChainedTaskConfigs(userClassloader);
    chainedConfigs.put(rootStreamConfig.getVertexID(), rootStreamConfig);

    // Find the last operator (the one with no chained outputs)
    for (StreamConfig config : chainedConfigs.values()) {
      List<StreamEdge> chainedOutputs = config.getChainedOutputs(userClassloader);
      if (chainedOutputs == null || chainedOutputs.isEmpty()) {
        return config;
      }
    }

    // If no last operator found, use the root config
    return rootStreamConfig;
  }

  private StreamNode mockStreamNode(StreamConfig streamConfig) {
    return new StreamNode(
        streamConfig.getVertexID(),
        null,
        null,
        (StreamOperatorFactory<?>) streamConfig.getStreamOperatorFactory(userClassloader),
        streamConfig.getOperatorName(),
        null);
  }

  // Update stream edges when vertices have been changed due to offloading
  private void visitAndUpdateStreamEdges(
      OperatorChainSlice sourceChainSlice,
      OperatorChainSliceGraph sourceChainSliceGraph,
      OperatorChainSliceGraph offloadedChainSliceGraph) {
    OperatorChainSlice offloadedChainSlice =
        offloadedChainSliceGraph.getSlice(sourceChainSlice.id());
    if (offloadedChainSlice.isOffloadable()) {
      updateStreamEdgesForOffloadedSlice(
          sourceChainSlice, sourceChainSliceGraph, offloadedChainSliceGraph, offloadedChainSlice);
    }

    // Recursively update downstream chain slices
    for (Integer downstreamSliceId : sourceChainSlice.getOutputs()) {
      visitAndUpdateStreamEdges(
          sourceChainSliceGraph.getSlice(downstreamSliceId),
          sourceChainSliceGraph,
          offloadedChainSliceGraph);
    }
  }

  private void updateStreamEdgesForOffloadedSlice(
      OperatorChainSlice sourceChainSlice,
      OperatorChainSliceGraph sourceChainSliceGraph,
      OperatorChainSliceGraph offloadedChainSliceGraph,
      OperatorChainSlice offloadedChainSlice) {
    List<Integer> downstreamSliceIds = sourceChainSlice.getOutputs();
    if (downstreamSliceIds.isEmpty()) {
      StreamConfig offloadedOpConfig = offloadedChainSlice.getOperatorConfigs().get(0);
      offloadedOpConfig.setChainedOutputs(new ArrayList<>());
      return;
    }

    List<StreamEdge> newOutputEdges = new ArrayList<>();
    List<StreamConfig> sourceOperatorConfigs = sourceChainSlice.getOperatorConfigs();
    StreamConfig lastSourceOpConfig = sourceOperatorConfigs.get(sourceOperatorConfigs.size() - 1);
    List<StreamEdge> originalOutputEdges = lastSourceOpConfig.getChainedOutputs(userClassloader);

    for (int i = 0; i < downstreamSliceIds.size(); i++) {
      Integer downstreamSliceId = downstreamSliceIds.get(i);
      OperatorChainSlice downstreamOffloadedSlice =
          offloadedChainSliceGraph.getSlice(downstreamSliceId);
      StreamConfig downstreamOpConfig = downstreamOffloadedSlice.getOperatorConfigs().get(0);
      StreamEdge originalEdge = originalOutputEdges.get(i);
      StreamEdge newEdge = createStreamEdge(originalEdge, downstreamOpConfig, offloadedChainSlice);
      newOutputEdges.add(newEdge);
    }

    StreamConfig offloadedOpConfig = offloadedChainSlice.getOperatorConfigs().get(0);
    offloadedOpConfig.setChainedOutputs(newOutputEdges);
  }

  private StreamEdge createStreamEdge(
      StreamEdge originalEdge,
      StreamConfig downstreamOpConfig,
      OperatorChainSlice offloadedChainSlice) {
    StreamConfig sourceOpConfig = offloadedChainSlice.getOperatorConfigs().get(0);
    return new StreamEdge(
        mockStreamNode(sourceOpConfig),
        mockStreamNode(downstreamOpConfig),
        originalEdge.getTypeNumber(),
        originalEdge.getBufferTimeout(),
        originalEdge.getPartitioner(),
        originalEdge.getOutputTag(),
        originalEdge.getExchangeMode(),
        0, // default value
        originalEdge.getIntermediateDatasetIdToProduce());
  }

  private void serializeAllOperatorsConfigs(OperatorChainSliceGraph chainSliceGraph) {
    for (OperatorChainSlice chainSlice : chainSliceGraph.getSlices().values()) {
      for (StreamConfig opConfig : chainSlice.getOperatorConfigs()) {
        opConfig.serializeAllConfigs();
      }
    }
  }

  private Map<Integer, StreamConfig> collectChainedConfigs(
      OperatorChainSlice sourceChainSlice, OperatorChainSliceGraph offloadedChainSliceGraph) {
    Map<Integer, StreamConfig> chainedConfigs = new HashMap<>();
    if (!sourceChainSlice.isOffloadable()) {
      List<StreamConfig> operatorConfigs = sourceChainSlice.getOperatorConfigs();
      for (int i = 1; i < operatorConfigs.size(); i++) {
        StreamConfig opConfig = operatorConfigs.get(i);
        chainedConfigs.put(opConfig.getVertexID(), opConfig);
      }
    }
    for (OperatorChainSlice chainSlice : offloadedChainSliceGraph.getSlices().values()) {
      if (chainSlice.id().equals(sourceChainSlice.id())) {
        continue;
      }
      for (StreamConfig opConfig : chainSlice.getOperatorConfigs()) {
        chainedConfigs.put(opConfig.getVertexID(), opConfig);
      }
    }
    return chainedConfigs;
  }

  private void updateSourceConfigIfOffloadable(
      StreamConfig sourceConfig,
      StreamConfig offloadedSourceConfig,
      OperatorChainSlice sourceChainSlice,
      Map<Integer, StreamConfig> chainedConfigs) {
    if (sourceChainSlice.isOffloadable()) {
      // Update the first operator config
      sourceConfig.setStreamOperatorFactory(
          offloadedSourceConfig.getStreamOperatorFactory(userClassloader));
      sourceConfig.setChainedOutputs(offloadedSourceConfig.getChainedOutputs(userClassloader));
      sourceConfig.setTypeSerializerOut(
          offloadedSourceConfig.getTypeSerializerOut(userClassloader));
      sourceConfig.setInputs(offloadedSourceConfig.getInputs(userClassloader));
      updateStatePartitioners(sourceConfig, offloadedSourceConfig);
    }
  }

  private void updateStatePartitioners(StreamConfig sourceConfig, StreamConfig offloadedConfig) {
    KeySelector<?, ?> keySelector0 = offloadedConfig.getStatePartitioner(0, userClassloader);
    if (keySelector0 != null) {
      sourceConfig.setStatePartitioner(0, keySelector0);
    }
    KeySelector<?, ?> keySelector1 = offloadedConfig.getStatePartitioner(1, userClassloader);
    if (keySelector1 != null) {
      sourceConfig.setStatePartitioner(1, keySelector1);
    }
  }

  private boolean supportsVectorOutput(
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph, JobVertex jobVertex) {
    List<Integer> downstreamSliceIds = chainSlice.getOutputs();

    // If chainSlice has downstream in the operator chain, check these downstream
    if (!downstreamSliceIds.isEmpty()) {
      return checkDownstreamSlicesInChain(chainSliceGraph, downstreamSliceIds);
    } else {
      // If chainSlice has no downstream in the operator chain, check downstream JobVertex from
      // JobGraph level
      return checkDownstreamVerticesFromJobGraph(jobVertex);
    }
  }

  private boolean checkDownstreamSlicesInChain(
      OperatorChainSliceGraph chainSliceGraph, List<Integer> downstreamSliceIds) {
    for (Integer downstreamSliceId : downstreamSliceIds) {
      OperatorChainSlice downstreamSlice = chainSliceGraph.getSlice(downstreamSliceId);
      if (!downstreamSlice.isOffloadable()) {
        return false;
      }
      if (!downstreamSlice.getInputs().stream()
          .map(chainSliceGraph::getSlice)
          .allMatch(OperatorChainSlice::isOffloadable)) {
        return false;
      }
    }
    return true;
  }

  private boolean checkDownstreamVerticesFromJobGraph(JobVertex jobVertex) {
    List<JobVertex> downstreamVertices = getDownstreamJobVertices(jobVertex);
    if (downstreamVertices.isEmpty()) {
      // If there is no downstream JobVertex, can output RowVector (no downstream needs
      // conversion)
      return true;
    }
    // Check if all downstream vertices' operators are gluten operators
    for (JobVertex downstreamVertex : downstreamVertices) {
      StreamConfig downstreamStreamConfig = new StreamConfig(downstreamVertex.getConfiguration());
      Optional<GlutenOperator> glutenOperator =
          Utils.getGlutenOperator(downstreamStreamConfig, userClassloader);
      if (!glutenOperator.isPresent()) {
        return false;
      }
    }
    return true;
  }

  private boolean supportsVectorInput(
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph, JobVertex jobVertex) {
    List<Integer> upstreamSliceIds = chainSlice.getInputs();

    // If chainSlice has upstream in the operator chain, check these upstream
    if (!upstreamSliceIds.isEmpty()) {
      return checkUpstreamSlicesInChain(chainSliceGraph, upstreamSliceIds, jobVertex);
    } else {
      // If chainSlice has no upstream in the operator chain, check upstream JobVertex from
      // JobGraph level
      return checkUpstreamVerticesFromJobGraph(jobVertex);
    }
  }

  private boolean checkUpstreamSlicesInChain(
      OperatorChainSliceGraph chainSliceGraph,
      List<Integer> upstreamSliceIds,
      JobVertex jobVertex) {
    for (Integer upstreamSliceId : upstreamSliceIds) {
      OperatorChainSlice upstreamSlice = chainSliceGraph.getSlice(upstreamSliceId);
      if (!upstreamSlice.isOffloadable()) {
        return false;
      }
      if (!supportsVectorOutput(upstreamSlice, chainSliceGraph, jobVertex)) {
        return false;
      }
    }
    return true;
  }

  private boolean checkUpstreamVerticesFromJobGraph(JobVertex jobVertex) {
    List<JobVertex> upstreamVertices = getUpstreamJobVertices(jobVertex);
    if (upstreamVertices.isEmpty()) {
      // If there is no upstream JobVertex, can input RowVector (no upstream needs conversion)
      return true;
    }
    // Check if the last operator of all upstream vertices are gluten operators
    for (JobVertex upstreamVertex : upstreamVertices) {
      StreamConfig lastOperatorConfig = findLastOperatorInChain(upstreamVertex);
      Optional<GlutenOperator> glutenOperator =
          Utils.getGlutenOperator(lastOperatorConfig, userClassloader);
      if (!glutenOperator.isPresent()) {
        return false;
      }
    }
    return true;
  }

  /** Get all downstream JobVertices of a JobVertex. */
  private List<JobVertex> getDownstreamJobVertices(JobVertex vertex) {
    List<JobVertex> downstreamVertices = new ArrayList<>();
    for (IntermediateDataSet dataSet : vertex.getProducedDataSets()) {
      for (JobEdge edge : dataSet.getConsumers()) {
        JobVertex downstreamVertex = edge.getTarget();
        downstreamVertices.add(downstreamVertex);
      }
    }
    return downstreamVertices;
  }

  /** Get all upstream JobVertices of a JobVertex. */
  private List<JobVertex> getUpstreamJobVertices(JobVertex vertex) {
    List<JobVertex> upstreamVertices = new ArrayList<>();
    for (JobEdge edge : vertex.getInputs()) {
      JobVertex upstreamVertex = edge.getSource().getProducer();
      upstreamVertices.add(upstreamVertex);
    }
    return upstreamVertices;
  }
}

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
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.runtime.partitioner.GlutenKeyGroupStreamPartitioner;
import org.apache.gluten.table.runtime.keyselector.GlutenKeySelector;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.LocalPartitionNode;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamPartitionNode;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.HashDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This {@link ExecNode} represents a change of partitioning of the input elements for stream.
 *
 * <p>TODO Remove this class once FLINK-21224 is finished.
 */
@ExecNodeMetadata(
    name = "stream-exec-exchange",
    version = 1,
    producedTransformations = StreamExecExchange.EXCHANGE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecExchange extends CommonExecExchange implements StreamExecNode<RowData> {

  public static final String EXCHANGE_TRANSFORMATION = "exchange";

  public StreamExecExchange(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecExchange.class),
        ExecNodeContext.newPersistedConfig(StreamExecExchange.class, tableConfig),
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecExchange(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    checkArgument(inputProperties.size() == 1);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<RowData> inputTransform =
        (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);

    final StreamPartitioner partitioner;
    final int parallelism;
    final InputProperty inputProperty = getInputProperties().get(0);
    final InputProperty.DistributionType distributionType =
        inputProperty.getRequiredDistribution().getType();
    switch (distributionType) {
      case SINGLETON:
        partitioner = new GlobalPartitioner<>();
        parallelism = 1;
        break;
      case HASH:
        // TODO Eliminate duplicate keys
        int[] keys = ((HashDistribution) inputProperty.getRequiredDistribution()).getKeys();
        InternalTypeInfo<RowData> inputType =
            (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        KeySelector keySelector =
            KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), keys, inputType);
        // --- Begin Gluten-specific code changes ---
        OneInputTransformation oneInputTransform = (OneInputTransformation) inputTransform;
        if (oneInputTransform.getOperator() instanceof GlutenOperator) {
          // TODO: velox's parallelism need to be set here, as some nodes need it.
          // should set it when operator init.
          parallelism = inputTransform.getParallelism();
          keySelector = new GlutenKeySelector();
          final ExecEdge inputEdge = getInputEdges().get(0);
          io.github.zhztheplayer.velox4j.type.RowType glutenInputType =
              (io.github.zhztheplayer.velox4j.type.RowType)
                  LogicalTypeConverter.toVLType(inputEdge.getOutputType());
          io.github.zhztheplayer.velox4j.type.RowType outputType =
              (io.github.zhztheplayer.velox4j.type.RowType)
                  LogicalTypeConverter.toVLType(getOutputType());
          String id = PlanNodeIdGenerator.newId();
          List<Integer> keyIndexes = Arrays.stream(keys).boxed().collect(Collectors.toList());
          PartitionFunctionSpec partitionFunctionSpec =
              new HashPartitionFunctionSpec(glutenInputType, keyIndexes);
          PlanNode localPartition =
              new LocalPartitionNode(
                  id,
                  List.of(new EmptyNode(outputType)),
                  "REPARTITION",
                  false,
                  partitionFunctionSpec);
          PlanNode exchange = new StreamPartitionNode(id, localPartition, parallelism);
          final OneInputStreamOperator exchangeKeyGenerator =
              new GlutenOneInputOperator(
                  new StatefulPlanNode(id, exchange),
                  id,
                  glutenInputType,
                  Map.of(id, outputType),
                  RowData.class,
                  RowData.class,
                  "StreamExecExchange");
          inputTransform =
              ExecNodeUtil.createOneInputTransformation(
                  inputTransform,
                  new TransformationMetadata("exchange-hash", "Gluten exchange hash"),
                  exchangeKeyGenerator,
                  inputTransform.getOutputType(),
                  parallelism,
                  false);
          partitioner =
              new GlutenKeyGroupStreamPartitioner(keySelector, DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        } else {
          parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
          partitioner =
              new KeyGroupStreamPartitioner<>(keySelector, DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        }
        // --- End Gluten-specific code changes ---
        break;
      default:
        throw new TableException(String.format("%s is not supported now!", distributionType));
    }

    final Transformation<RowData> transformation =
        new PartitionTransformation<>(inputTransform, partitioner);
    createTransformationMeta(EXCHANGE_TRANSFORMATION, config).fill(transformation);
    transformation.setParallelism(parallelism);
    transformation.setOutputType(InternalTypeInfo.of(getOutputType()));
    return transformation;
  }
}

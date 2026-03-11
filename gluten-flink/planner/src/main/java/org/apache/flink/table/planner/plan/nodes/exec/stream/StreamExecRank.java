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

import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamRankNode;
import io.github.zhztheplayer.velox4j.plan.StreamTopNNode;
import io.github.zhztheplayer.velox4j.plan.TopNNode;
import io.github.zhztheplayer.velox4j.plan.TopNRowNumberNode;
import io.github.zhztheplayer.velox4j.sort.SortOrder;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.swing.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RANK_TOPN_CACHE_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for Rank. */
@ExecNodeMetadata(
    name = "stream-exec-rank",
    version = 1,
    consumedOptions = {"table.exec.rank.topn-cache-size"},
    producedTransformations = StreamExecRank.RANK_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecRank extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  public static final String RANK_TRANSFORMATION = "rank";

  public static final String FIELD_NAME_RANK_TYPE = "rankType";
  public static final String FIELD_NAME_PARTITION_SPEC = "partition";
  public static final String FIELD_NAME_SORT_SPEC = "orderBy";
  public static final String FIELD_NAME_RANK_RANG = "rankRange";
  public static final String FIELD_NAME_RANK_STRATEGY = "rankStrategy";
  public static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";
  public static final String FIELD_NAME_OUTPUT_RANK_NUMBER = "outputRowNumber";

  public static final String STATE_NAME = "rankState";

  @JsonProperty(FIELD_NAME_RANK_TYPE)
  private final RankType rankType;

  @JsonProperty(FIELD_NAME_PARTITION_SPEC)
  private final PartitionSpec partitionSpec;

  @JsonProperty(FIELD_NAME_SORT_SPEC)
  private final SortSpec sortSpec;

  @JsonProperty(FIELD_NAME_RANK_RANG)
  private final RankRange rankRange;

  @JsonProperty(FIELD_NAME_RANK_STRATEGY)
  private final RankProcessStrategy rankStrategy;

  @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER)
  private final boolean outputRankNumber;

  @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
  private final boolean generateUpdateBefore;

  @Nullable
  @JsonProperty(FIELD_NAME_STATE)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<StateMetadata> stateMetadataList;

  public StreamExecRank(
      ReadableConfig tableConfig,
      RankType rankType,
      PartitionSpec partitionSpec,
      SortSpec sortSpec,
      RankRange rankRange,
      RankProcessStrategy rankStrategy,
      boolean outputRankNumber,
      boolean generateUpdateBefore,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecRank.class),
        ExecNodeContext.newPersistedConfig(StreamExecRank.class, tableConfig),
        rankType,
        partitionSpec,
        sortSpec,
        rankRange,
        rankStrategy,
        outputRankNumber,
        generateUpdateBefore,
        StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME),
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecRank(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_RANK_TYPE) RankType rankType,
      @JsonProperty(FIELD_NAME_PARTITION_SPEC) PartitionSpec partitionSpec,
      @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
      @JsonProperty(FIELD_NAME_RANK_RANG) RankRange rankRange,
      @JsonProperty(FIELD_NAME_RANK_STRATEGY) RankProcessStrategy rankStrategy,
      @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER) boolean outputRankNumber,
      @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
      @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    checkArgument(inputProperties.size() == 1);
    this.rankType = checkNotNull(rankType);
    this.rankRange = checkNotNull(rankRange);
    this.rankStrategy = checkNotNull(rankStrategy);
    this.sortSpec = checkNotNull(sortSpec);
    this.partitionSpec = checkNotNull(partitionSpec);
    this.outputRankNumber = outputRankNumber;
    this.generateUpdateBefore = generateUpdateBefore;
    this.stateMetadataList = stateMetadataList;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    switch (rankType) {
      case ROW_NUMBER:
        break;
      case RANK:
        throw new TableException("RANK() on streaming table is not supported currently");
      case DENSE_RANK:
        throw new TableException("DENSE_RANK() on streaming table is not supported currently");
      default:
        throw new TableException(
            String.format("Streaming tables do not support %s rank function.", rankType));
    }

    ExecEdge inputEdge = getInputEdges().get(0);
    Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);

    RowType inputRowType = (RowType) inputEdge.getOutputType();
    InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.of(inputRowType);
    int[] sortFields = sortSpec.getFieldIndices();
    long cacheSize = config.get(TABLE_EXEC_RANK_TOPN_CACHE_SIZE);

    // --- Begin Gluten-specific code changes ---
    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);

    int[] partitionFields = partitionSpec.getFieldIndices();
    List<FieldAccessTypedExpr> sortKeys = Utils.generateFieldAccesses(inputType, sortFields);
    List<FieldAccessTypedExpr> partitionKeys =
        Utils.generateFieldAccesses(inputType, partitionFields);
    List<SortOrder> sortOrders = generateSortOrders(sortSpec);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    // TODO: velox RowNumber may not equal to flink
    int limit = 1;
    final PlanNode topNNode;
    if (outputRankNumber) {
      topNNode =
          new TopNRowNumberNode(
              PlanNodeIdGenerator.newId(),
              partitionKeys,
              sortKeys,
              sortOrders,
              null,
              limit,
              List.of(new EmptyNode(inputType)));
    } else {
      topNNode =
          new TopNNode(
              PlanNodeIdGenerator.newId(),
              sortKeys,
              sortOrders,
              limit,
              false,
              List.of(new EmptyNode(inputType)));
    }
    List<Integer> keyIndexes = Arrays.stream(partitionFields).boxed().collect(Collectors.toList());
    PartitionFunctionSpec keySelectorSpec = new HashPartitionFunctionSpec(inputType, keyIndexes);
    List<Integer> sortKeyIndexes = Arrays.stream(sortFields).boxed().collect(Collectors.toList());
    PartitionFunctionSpec sortKeySelectorSpec =
        new HashPartitionFunctionSpec(inputType, sortKeyIndexes);
    final PlanNode streamTopNNode =
        new StreamTopNNode(
            PlanNodeIdGenerator.newId(),
            List.of(new EmptyNode(inputType)), // sources
            topNNode,
            sortKeySelectorSpec,
            outputType,
            generateUpdateBefore,
            outputRankNumber,
            cacheSize);
    final PlanNode streamRankNode =
        new StreamRankNode(
            PlanNodeIdGenerator.newId(),
            List.of(new EmptyNode(inputType)), // sources
            keySelectorSpec,
            streamTopNNode,
            outputType);
    final OneInputStreamOperator operator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(streamRankNode.getId(), streamRankNode),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(streamRankNode.getId(), outputType),
            RowData.class,
            RowData.class,
            "StreamExecRank");
    // --- End Gluten-specific code changes ---

    OneInputTransformation<RowData, RowData> transform =
        ExecNodeUtil.createOneInputTransformation(
            inputTransform,
            createTransformationMeta(RANK_TRANSFORMATION, config),
            operator,
            InternalTypeInfo.of((RowType) getOutputType()),
            inputTransform.getParallelism(),
            false);

    // set KeyType and Selector for state
    RowDataKeySelector selector =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            partitionSpec.getFieldIndices(),
            inputRowTypeInfo);
    transform.setStateKeySelector(selector);
    transform.setStateKeyType(selector.getProducedType());
    return transform;
  }

  private List<SortOrder> generateSortOrders(SortSpec sortSpec) {
    final List<SortOrder> sortOrders = new ArrayList<>();
    boolean[] ascendingOrders = sortSpec.getAscendingOrders();
    boolean[] nullLasts = sortSpec.getNullsIsLast();
    checkArgument(ascendingOrders.length == nullLasts.length);
    for (int i = 0; i < ascendingOrders.length; i++) {
      sortOrders.add(new SortOrder(ascendingOrders[i], !nullLasts[i]));
    }
    return sortOrders;
  }
}

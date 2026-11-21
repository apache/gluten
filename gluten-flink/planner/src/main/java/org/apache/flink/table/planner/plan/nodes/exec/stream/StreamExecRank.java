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

import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.RankNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.KeyedStateSpec;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.HugeIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RealType;
import io.github.zhztheplayer.velox4j.type.SmallIntType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.TinyIntType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.type.VarbinaryType;

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
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

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
    RowType inputRowType = (RowType) inputEdge.getOutputType();

    // --- Begin Gluten-specific code changes ---
    // Fail loud guards, checked before any translation (design limits
    // L3/L4): only insert-only input (AppendFast) reaches the native
    // operator and the rank range must be constant.
    if (!(rankStrategy instanceof RankProcessStrategy.AppendFastStrategy)) {
      throw new TableException(
          String.format(
              "Gluten rank only supports the AppendFast (insert-only) strategy, got %s",
              rankStrategy));
    }
    if (!(rankRange instanceof ConstantRankRange)) {
      throw new TableException(
          String.format("Gluten rank only supports a constant rank range, got %s", rankRange));
    }
    final ConstantRankRange constantRange = (ConstantRankRange) rankRange;
    // Offset rank ranges (rankStart > 1) pass through with their true
    // values: the native operator gates every rank-bearing emission through
    // isInRankRange and forces the with-row-number algorithm (hasOffset).
    // The FirstN fast path detection mirrors the native routing
    // (StreamExecRank.java in flink-table-planner): a single proctime
    // ascending sort key routes to the per-key counter branch; every other
    // shape falls to the generic TopN/Top1 branches. The (1,1) window
    // overlapping this shape still takes FirstN - the native dispatch
    // order checks firstN before isTop1.
    final int[] sortFields = sortSpec.getFieldIndices();
    final boolean firstN =
        sortFields.length == 1
            && TypeCheckUtils.isProcTime(sortSpec.getFieldTypes(inputRowType)[0])
            && sortSpec.getFieldSpec(0).getIsAscendingOrder();
    // Top1 (ConstantRankRange(1,1)) is derived inside the native operator from
    // the range (FastTop1Function vs AppendOnlyTopNFunction semantics).

    Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);

    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());

    // The native sort-key comparator (RankOperator.cpp compareScalarValues)
    // covers these velox type kinds (design limit L11). DECIMAL arrives as
    // velox4j DecimalType and compares through its BIGINT/HUGEINT kind
    // (p<=18 short, p<=38 long); DATE arrives as DateType and compares
    // through the INTEGER kind; TIMESTAMP_LTZ is normalized to TIMESTAMP by
    // the converter. TIME still fails loud: the converter has no TimeType
    // entry (L14) and rejects the column earlier - the guard below keeps
    // the check explicit for any type that slips past the converter.
    for (int i = 0; i < sortFields.length; i++) {
      checkSortKeyTypeSupported(inputType.getChildren().get(sortFields[i]), sortFields[i]);
    }

    // The keyed-state spec of the plan (pure plan data): the partition key
    // channels, or - for the global shape without a PARTITION BY - the
    // sentinel channel one past the input schema, whose BIGINT constant
    // column the native operator injects before the probe (design D3).
    final List<Integer> keyChannels = new ArrayList<>();
    final List<Type> keyTypes = new ArrayList<>();
    if (partitionSpec.getFieldIndices().length > 0) {
      for (int field : partitionSpec.getFieldIndices()) {
        checkPartitionKeyTypeSupported(inputRowType.getTypeAt(field), field);
        keyChannels.add(field);
        keyTypes.add(LogicalTypeConverter.toVLType(inputRowType.getTypeAt(field)));
      }
    } else {
      keyChannels.add(inputType.size());
      keyTypes.add(new BigIntType());
    }
    final KeyedStateSpec keyedStateSpec = new KeyedStateSpec(keyChannels, keyTypes);

    final List<Integer> sortKeyChannels =
        Arrays.stream(sortFields).boxed().collect(Collectors.toList());
    final List<Boolean> sortAscending = new ArrayList<>();
    final List<Boolean> sortNullsFirst = new ArrayList<>();
    for (int i = 0; i < sortFields.length; i++) {
      sortAscending.add(sortSpec.getFieldSpec(i).getIsAscendingOrder());
      sortNullsFirst.add(!sortSpec.getFieldSpec(i).getNullIsLast());
    }
    // The TopN cache bound of the native StreamExecRank translation
    // (StreamExecRank.java:231 there): consumed from the same
    // table.exec.rank.topn-cache-size option the annotation already
    // declares (design alignment #67).
    final long cacheSize = config.get(TABLE_EXEC_RANK_TOPN_CACHE_SIZE);

    final PlanNode rankNode =
        new RankNode(
            PlanNodeIdGenerator.newId(),
            constantRange.getRankStart(),
            constantRange.getRankEnd(),
            sortKeyChannels,
            sortAscending,
            sortNullsFirst,
            keyedStateSpec,
            outputRankNumber,
            generateUpdateBefore,
            firstN,
            cacheSize,
            inputType,
            outputType);
    final OneInputStreamOperator operator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(rankNode.getId(), rankNode),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(rankNode.getId(), outputType),
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

    // The transformation stays NON-keyed (implementation contract): the key
    // belongs to the velox keyed state backend via the plan's
    // KeyedStateSpec, and declaring a Flink key selector would conflict
    // with the native key-group routing at parallelism > 1.
    return transform;
  }

  // The native keyed state backend supports fixed-width scalar and
  // varchar/varbinary key columns only (KeyTable::checkStateKeyTypes);
  // unsupported partition key types fail loud at translation time instead
  // of dying inside the first probe.
  private static void checkPartitionKeyTypeSupported(LogicalType type, int fieldIndex) {
    final boolean supported =
        (TypeCheckUtils.isNumeric(type) && !TypeCheckUtils.isDecimal(type))
            || TypeCheckUtils.isTimestamp(type)
            || TypeCheckUtils.isTimestampWithLocalZone(type)
            || TypeCheckUtils.isBoolean(type)
            || TypeCheckUtils.isCharacterString(type)
            || TypeCheckUtils.isBinaryString(type);
    if (!supported) {
      throw new TableException(
          String.format(
              "Gluten rank does not support partition key type %s at field %d", type, fieldIndex));
    }
  }

  // The native sort-key comparator (RankOperator.cpp compareScalarValues)
  // covers exactly these velox type kinds (design limit L11): the 11 base
  // kinds plus DECIMAL (BIGINT/HUGEINT kind by precision) and DATE (INTEGER
  // kind); a sort key outside the set fails loud at translation time
  // instead of on the first row comparison.
  private static void checkSortKeyTypeSupported(Type type, int fieldIndex) {
    final boolean supported =
        type instanceof BooleanType
            || type instanceof TinyIntType
            || type instanceof SmallIntType
            || type instanceof IntegerType
            || type instanceof BigIntType
            || type instanceof HugeIntType
            || type instanceof RealType
            || type instanceof DoubleType
            || type instanceof VarCharType
            || type instanceof VarbinaryType
            || type instanceof TimestampType
            || type instanceof DecimalType
            || type instanceof DateType;
    if (!supported) {
      throw new TableException(
          String.format(
              "Gluten rank does not support sort key type %s at field %d", type, fieldIndex));
    }
  }
}

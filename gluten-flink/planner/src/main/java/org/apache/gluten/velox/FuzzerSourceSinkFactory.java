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
package org.apache.gluten.velox;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.connector.CommitStrategy;
import io.github.zhztheplayer.velox4j.connector.DiscardDataTableHandle;
import io.github.zhztheplayer.velox4j.connector.FuzzerConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.FuzzerTableHandle;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.plan.TableWriteNode;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.List;
import java.util.Map;

public class FuzzerSourceSinkFactory implements VeloxSourceSinkFactory {

  @SuppressWarnings({"unchecked"})
  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (transformation instanceof SinkTransformation) {
      SinkTransformation<RowData, RowData> sinkTransformation =
          (SinkTransformation<RowData, RowData>) transformation;
      Transformation<?> inputTransformation = sinkTransformation.getInputs().get(0);
      if (sinkTransformation.getSink() instanceof DiscardingSink
          && !inputTransformation.getName().equals("PartitionCommitter")) {
        return true;
      }
    } else if (transformation instanceof LegacySourceTransformation) {
      Function userFunction =
          ((LegacySourceTransformation<RowData>) transformation).getOperator().getUserFunction();
      return userFunction instanceof DataGeneratorSource;
    }
    return false;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    LegacySourceTransformation<RowData> sourceTransformation =
        (LegacySourceTransformation<RowData>) transformation;
    RowType outputType =
        (RowType)
            LogicalTypeConverter.toVLType(
                ((InternalTypeInfo) sourceTransformation.getOutputType()).toLogicalType());
    String id = PlanNodeIdGenerator.newId();
    /// Create fuzzer table handle by 2 parameters: (1) a string as connector id; (2) a int as seed
    FuzzerTableHandle tableHandle = new FuzzerTableHandle("connector-fuzzer", 12367);
    PlanNode tableScan = new TableScanNode(id, outputType, tableHandle, List.of());
    GlutenStreamSource sourceOp =
        new GlutenStreamSource(
            new GlutenSourceFunction(
                new StatefulPlanNode(id, tableScan),
                Map.of(id, outputType),
                id,
                new FuzzerConnectorSplit("connector-fuzzer", 1000),
                RowData.class));
    return new LegacySourceTransformation<RowData>(
        sourceTransformation.getName(),
        sourceOp,
        sourceTransformation.getOutputType(),
        sourceTransformation.getParallelism(),
        sourceTransformation.getBoundedness(),
        false);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Transformation<RowData> buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    SinkTransformation<RowData, RowData> sinkTransformation =
        (SinkTransformation<RowData, RowData>) transformation;
    RowType outputType =
        (RowType)
            LogicalTypeConverter.toVLType(
                ((InternalTypeInfo) transformation.getOutputType()).toLogicalType());
    // TODO: this is a constrain of velox.
    // The result type should be ignored, as the data is written by velox,
    // and no result need to return.
    RowType ignore = new RowType(List.of("num"), List.of(new BigIntType()));
    PlanNode plan =
        new TableWriteNode(
            PlanNodeIdGenerator.newId(),
            outputType,
            outputType.getNames(),
            null,
            "connector-fuzzer",
            new DiscardDataTableHandle(),
            false,
            ignore,
            CommitStrategy.NO_COMMIT,
            List.of(new EmptyNode(outputType)));
    GlutenOneInputOperatorFactory operatorFactory =
        new GlutenOneInputOperatorFactory(
            new GlutenOneInputOperator(
                new StatefulPlanNode(plan.getId(), plan),
                PlanNodeIdGenerator.newId(),
                outputType,
                Map.of(plan.getId(), ignore),
                RowData.class,
                RowData.class,
                "FuzzerSink"));
    DataStream<RowData> newInputStream =
        sinkTransformation
            .getInputStream()
            .transform("Writer", CommittableMessageTypeInfo.noOutput(), operatorFactory);
    return new SinkTransformation<RowData, RowData>(
        newInputStream,
        sinkTransformation.getSink(),
        sinkTransformation.getOutputType(),
        sinkTransformation.getName(),
        sinkTransformation.getParallelism(),
        sinkTransformation.isParallelismConfigured(),
        sinkTransformation.getSinkOperatorsUidHashes());
  }
}

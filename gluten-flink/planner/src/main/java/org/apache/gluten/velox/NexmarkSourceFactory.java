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

import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.connector.NexmarkConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.NexmarkTableHandle;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class NexmarkSourceFactory implements VeloxSourceSinkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(NexmarkSourceFactory.class);

  @SuppressWarnings("rawtypes")
  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (transformation instanceof SourceTransformation) {
      Class<?> sourceClazz = ((SourceTransformation) transformation).getSource().getClass();
      return sourceClazz.getSimpleName().equals("NexmarkSource");
    }
    return false;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    RowType outputType =
        (RowType)
            LogicalTypeConverter.toVLType(
                ((InternalTypeInfo) transformation.getOutputType()).toLogicalType());
    Object nexmarkSource = ((SourceTransformation) transformation).getSource();
    String id = PlanNodeIdGenerator.newId();
    List<?> nexmarkSourceSplits =
        (List<?>)
            ReflectUtils.invokeObjectMethod(
                nexmarkSource.getClass(),
                nexmarkSource,
                "getSplits",
                new Class<?>[] {int.class},
                new Object[] {transformation.getParallelism()});
    Object nexmarkSourceSplit = nexmarkSourceSplits.get(0);
    Object generatorConfig =
        ReflectUtils.getObjectField(
            nexmarkSourceSplit.getClass(), nexmarkSourceSplit, "generatorConfig");
    Long maxEvents =
        (Long)
            ReflectUtils.getObjectField(generatorConfig.getClass(), generatorConfig, "maxEvents");
    PlanNode tableScan =
        new TableScanNode(id, outputType, new NexmarkTableHandle("connector-nexmark"), List.of());
    GlutenStreamSource sourceOp =
        new GlutenStreamSource(
            new GlutenSourceFunction(
                new StatefulPlanNode(tableScan.getId(), tableScan),
                Map.of(id, outputType),
                id,
                new NexmarkConnectorSplit(
                    "connector-nexmark",
                    maxEvents > Integer.MAX_VALUE ? Integer.MAX_VALUE : maxEvents.intValue()),
                RowData.class));

    return new LegacySourceTransformation<RowData>(
        transformation.getName(),
        sourceOp,
        transformation.getOutputType(),
        transformation.getParallelism(),
        ((SourceTransformation) transformation).getBoundedness(),
        false);
  }

  @Override
  public Transformation<RowData> buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    throw new UnsupportedOperationException("Unimplemented method 'buildSink'");
  }
}

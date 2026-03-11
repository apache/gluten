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

import io.github.zhztheplayer.velox4j.connector.FromElementsConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.FromElementsTableHandle;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class FromElementsSourceFactory implements VeloxSourceSinkFactory {

  @SuppressWarnings("rawtypes")
  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (transformation instanceof LegacySourceTransformation) {
      StreamSource source = ((LegacySourceTransformation) transformation).getOperator();
      return source.getUserFunction() instanceof FromElementsFunction;
    }
    return false;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    LegacySourceTransformation<RowData> sourceTransformation =
        (LegacySourceTransformation<RowData>) transformation;
    try {
      ScanTableSource tableSource =
          (ScanTableSource) parameters.get(ScanTableSource.class.getName());
      Class<?> tableSourceClazz =
          Class.forName(
              "org.apache.flink.table.planner.factories.TestValuesTableFactory$TestValuesScanTableSourceWithoutProjectionPushDown");
      Map<Map<String, String>, Collection<Row>> data =
          (Map) ReflectUtils.getObjectField(tableSourceClazz, tableSource, "data");
      InternalTypeInfo<RowData> typeInfo =
          (InternalTypeInfo<RowData>) sourceTransformation.getOutputType();
      io.github.zhztheplayer.velox4j.type.RowType rowType =
          (io.github.zhztheplayer.velox4j.type.RowType)
              LogicalTypeConverter.toVLType(typeInfo.toLogicalType());
      List<String> values = new ArrayList<>();
      for (Collection<Row> rows : data.values()) {
        for (Row row : rows) {
          Row projectedRow =
              (Row)
                  ReflectUtils.invokeObjectMethod(
                      tableSourceClazz,
                      tableSource,
                      "projectRow",
                      new Class<?>[] {Row.class},
                      new Object[] {row});
          values.add(projectedRow.toString());
        }
      }
      FromElementsTableHandle tableHandle =
          new FromElementsTableHandle(
              "connector-from-elements", "from-elements-table", rowType, values);
      TableScanNode scanNode =
          new TableScanNode(PlanNodeIdGenerator.newId(), rowType, tableHandle, List.of());
      GlutenStreamSource op =
          new GlutenStreamSource(
              new GlutenSourceFunction(
                  new StatefulPlanNode(scanNode.getId(), scanNode),
                  Map.of(scanNode.getId(), rowType),
                  scanNode.getId(),
                  new FromElementsConnectorSplit("connector-from-elements", 0, false),
                  RowData.class),
              "FromElementsSource");
      return new LegacySourceTransformation<RowData>(
          sourceTransformation.getName(),
          op,
          typeInfo,
          sourceTransformation.getParallelism(),
          sourceTransformation.getBoundedness(),
          false);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }

  @Override
  public Transformation<RowData> buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    throw new FlinkRuntimeException("Unimplemented method 'buildSink'");
  }
}

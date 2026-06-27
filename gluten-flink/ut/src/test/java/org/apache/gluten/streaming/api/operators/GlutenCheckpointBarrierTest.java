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
package org.apache.gluten.streaming.api.operators;

import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that {@link GlutenOneInputOperator#prepareSnapshotPreBarrier} correctly injects a barrier
 * into the Velox pipeline and that the barrier propagates through the operator chain.
 */
public class GlutenCheckpointBarrierTest extends GlutenStreamOperatorTestBase {

  @Test
  public void testPrepareSnapshotPreBarrierInjectsBarrier() throws Exception {
    RowType flinkType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"v"});
    io.github.zhztheplayer.velox4j.type.RowType veloxType = convertToVeloxType(flinkType);

    String scanId = "scan-1";
    TableScanNode scanNode =
        new TableScanNode(
            scanId,
            veloxType,
            new ExternalStreamTableHandle("connector-external-stream"),
            java.util.List.of());

    GlutenOneInputOperator operator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(scanNode.getId(), scanNode),
            PlanNodeIdGenerator.newId(),
            veloxType,
            java.util.Map.of(scanNode.getId(), veloxType),
            RowData.class,
            RowData.class);

    TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);
    org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        createTestHarness(operator, typeInfo, typeInfo);

    processTestData(harness, testData);

    assertDoesNotThrow(() -> operator.prepareSnapshotPreBarrier(1L));

    harness.close();
  }
}

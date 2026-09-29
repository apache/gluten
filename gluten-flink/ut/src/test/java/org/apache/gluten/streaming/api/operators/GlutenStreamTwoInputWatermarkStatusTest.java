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

import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;

import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GlutenStreamTwoInputWatermarkStatusTest extends GlutenStreamJoinOperatorTestBase {

  @Test
  public void testWatermarkStatusPropagationThroughNativeJoinOperator() throws Exception {
    // Two-input operators combine per-input watermark status in native execution. The operator
    // becomes idle only when all inputs are idle, and becomes active when any input reactivates.
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.INNER);

    try (TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, StatefulRecord> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup();
      harness.open();

      // One idle input is excluded from combined watermark calculation, but the operator is not
      // idle yet because the other input is still active.
      harness.processWatermarkStatus1(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).isEmpty();

      // Once both inputs are idle, the combined status changes to IDLE and is emitted downstream.
      harness.processWatermarkStatus2(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);

      // Reactivating either input changes the combined status back to ACTIVE.
      harness.processWatermarkStatus1(WatermarkStatus.ACTIVE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE);
    }
  }

  @Test
  public void testWatermarkReactivatesIdleNativeJoinOperator() throws Exception {
    // A watermark received after all inputs became idle should implicitly reactivate that input.
    // Native execution must emit ACTIVE before the resulting combined watermark.
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.INNER);

    try (TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, StatefulRecord> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup();
      harness.open();

      harness.processWatermarkStatus1(WatermarkStatus.IDLE);
      harness.processWatermarkStatus2(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);

      // Input 1's watermark makes the combined status active again and advances the watermark.
      harness.processWatermark1(new Watermark(1L));
      assertThat(harness.getOutput())
          .containsExactly(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE, new Watermark(1L));
    }
  }

  @Test
  public void testIdleInputExcludedFromMinWatermark() throws Exception {
    // When one input is idle, its watermark is excluded from the combined min-watermark
    // calculation. The other active input's watermark can advance freely.
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.INNER);

    try (TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, StatefulRecord> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup();
      harness.open();

      harness.processWatermark1(new Watermark(100L));
      harness.processWatermark2(new Watermark(90L));
      assertThat(harness.getOutput()).containsExactly(new Watermark(90L));

      harness.processWatermarkStatus1(WatermarkStatus.IDLE);
      // Input 1 (watermark=100) is idle and excluded. Combined = input 2 (90). No change.
      assertThat(harness.getOutput()).containsExactly(new Watermark(90L));

      // Input 2 advances to 120. Since input 1 is idle and excluded, combined = 120.
      // If input 1 were still active, combined would be min(100, 120) = 100.
      harness.processWatermark2(new Watermark(120L));
      assertThat(harness.getOutput()).containsExactly(new Watermark(90L), new Watermark(120L));
    }
  }

  @Test
  public void testWatermarksUseNativeTwoInputMinimum() throws Exception {
    // While both inputs are active, native execution should combine indexed input watermarks by
    // forwarding only monotonically increasing minimum watermarks downstream.
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.INNER);

    try (TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, StatefulRecord> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup();
      harness.open();

      // The first input alone cannot advance the combined watermark because the second input has no
      // watermark yet.
      harness.processWatermark1(new Watermark(100L));
      assertThat(harness.getOutput()).isEmpty();

      // Once both inputs have watermarks, the lower input watermark is emitted.
      harness.processWatermark2(new Watermark(90L));
      assertThat(harness.getOutput()).containsExactly(new Watermark(90L));

      // Advancing the lower input exposes input 1's watermark as the new combined minimum.
      harness.processWatermark2(new Watermark(110L));
      assertThat(harness.getOutput()).containsExactly(new Watermark(90L), new Watermark(100L));
    }
  }
}

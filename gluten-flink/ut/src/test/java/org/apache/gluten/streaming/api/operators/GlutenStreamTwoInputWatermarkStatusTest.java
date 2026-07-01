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
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.INNER);

    try (TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, StatefulRecord> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup();
      harness.open();

      harness.processWatermarkStatus1(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).isEmpty();

      harness.processWatermarkStatus2(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);

      harness.processWatermarkStatus1(WatermarkStatus.ACTIVE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE);
    }
  }

  @Test
  public void testWatermarkReactivatesIdleNativeJoinOperator() throws Exception {
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.INNER);

    try (TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, StatefulRecord> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup();
      harness.open();

      harness.processWatermarkStatus1(WatermarkStatus.IDLE);
      harness.processWatermarkStatus2(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);

      harness.processWatermark1(new Watermark(1L));
      assertThat(harness.getOutput())
          .containsExactly(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE, new Watermark(1L));
    }
  }
}

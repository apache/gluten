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
package org.apache.gluten.table.runtime.metrics;

import io.github.zhztheplayer.velox4j.query.SerialTaskStats;

import org.apache.flink.metrics.SimpleCounter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SourceTaskMetricsTest {

  @Test
  void updateMetricsUsesExactTableScanPlanId() {
    SimpleCounter rows = new SimpleCounter();
    SimpleCounter bytes = new SimpleCounter();
    SourceTaskMetrics metrics = new SourceTaskMetrics(rows, bytes);

    assertThat(metrics.updateMetrics(statsWithSingleTableScan(), "scan-1")).isTrue();

    assertThat(rows.getCount()).isEqualTo(5);
    assertThat(bytes.getCount()).isEqualTo(123);
  }

  @Test
  void updateMetricsSyncsTaskIoCounters() {
    SimpleCounter sourceRows = new SimpleCounter();
    SimpleCounter sourceBytes = new SimpleCounter();
    SimpleCounter taskRowsIn = new SimpleCounter();
    SimpleCounter taskRowsOut = new SimpleCounter();
    SimpleCounter taskBytesIn = new SimpleCounter();
    SimpleCounter taskBytesOut = new SimpleCounter();
    SourceTaskMetrics metrics =
        new SourceTaskMetrics(
            sourceRows, sourceBytes, taskRowsIn, taskRowsOut, taskBytesIn, taskBytesOut);

    assertThat(metrics.updateMetrics(statsWithSingleTableScan(), "scan-1")).isTrue();

    assertThat(sourceRows.getCount()).isEqualTo(5);
    assertThat(sourceBytes.getCount()).isEqualTo(123);
    assertThat(taskRowsIn.getCount()).isEqualTo(5);
    assertThat(taskRowsOut.getCount()).isEqualTo(5);
    assertThat(taskBytesIn.getCount()).isEqualTo(123);
    assertThat(taskBytesOut.getCount()).isEqualTo(123);
  }

  @Test
  void updateMetricsFallsBackToUniqueTableScanWhenPlanIdDiffers() {
    SimpleCounter rows = new SimpleCounter();
    SimpleCounter bytes = new SimpleCounter();
    SourceTaskMetrics metrics = new SourceTaskMetrics(rows, bytes);

    assertThat(metrics.updateMetrics(statsWithSingleTableScan(), "flink-source-id")).isTrue();

    assertThat(rows.getCount()).isEqualTo(5);
    assertThat(bytes.getCount()).isEqualTo(123);
  }

  @Test
  void updateMetricsDoesNotGuessWhenMultipleTableScansExist() {
    SimpleCounter rows = new SimpleCounter();
    SimpleCounter bytes = new SimpleCounter();
    SourceTaskMetrics metrics = new SourceTaskMetrics(rows, bytes);

    assertThat(metrics.updateMetrics(statsWithMultipleTableScans(), "flink-source-id")).isTrue();

    assertThat(rows.getCount()).isZero();
    assertThat(bytes.getCount()).isZero();
  }

  private static SerialTaskStats statsWithSingleTableScan() {
    return SerialTaskStats.fromJson(
        "{"
            + "\"planStats\":["
            + "{"
            + "\"planNodeId\":\"scan-1\","
            + "\"operatorType\":\"TableScan\","
            + "\"rawInputRows\":5,"
            + "\"rawInputBytes\":123"
            + "},"
            + "{"
            + "\"planNodeId\":\"project-1\","
            + "\"operatorType\":\"FilterProject\","
            + "\"rawInputRows\":0,"
            + "\"rawInputBytes\":0"
            + "}"
            + "]"
            + "}");
  }

  private static SerialTaskStats statsWithMultipleTableScans() {
    return SerialTaskStats.fromJson(
        "{"
            + "\"planStats\":["
            + "{"
            + "\"planNodeId\":\"scan-1\","
            + "\"operatorType\":\"TableScan\","
            + "\"rawInputRows\":5,"
            + "\"rawInputBytes\":123"
            + "},"
            + "{"
            + "\"planNodeId\":\"scan-2\","
            + "\"operatorType\":\"TableScan\","
            + "\"rawInputRows\":7,"
            + "\"rawInputBytes\":456"
            + "}"
            + "]"
            + "}");
  }
}

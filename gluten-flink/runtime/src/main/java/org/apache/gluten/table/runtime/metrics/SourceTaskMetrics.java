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

import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.query.SerialTaskStats;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class SourceTaskMetrics {

  private final String keyOperatorType = "operatorType";
  private final String sourceOperatorName = "TableScan";
  private final String keyInputRows = "rawInputRows";
  private final String keyInputBytes = "rawInputBytes";
  private final long metricUpdateInterval = 2000;
  private Counter sourceNumRecordsOut;
  private Counter sourceNumBytesOut;
  private Counter taskNumRecordsIn;
  private Counter taskNumRecordsOut;
  private Counter taskNumBytesIn;
  private Counter taskNumBytesOut;
  private long lastUpdateTime = 0;

  public SourceTaskMetrics(OperatorMetricGroup metricGroup) {
    sourceNumRecordsOut = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
    sourceNumBytesOut = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
    if (metricGroup instanceof InternalOperatorMetricGroup) {
      TaskIOMetricGroup taskIOMetricGroup =
          ((InternalOperatorMetricGroup) metricGroup).getTaskIOMetricGroup();
      taskNumRecordsIn = taskIOMetricGroup.getNumRecordsInCounter();
      taskNumRecordsOut = taskIOMetricGroup.getNumRecordsOutCounter();
      taskNumBytesIn = taskIOMetricGroup.getNumBytesInCounter();
      taskNumBytesOut = taskIOMetricGroup.getNumBytesOutCounter();
    }
  }

  SourceTaskMetrics(Counter sourceNumRecordsOut, Counter sourceNumBytesOut) {
    this(sourceNumRecordsOut, sourceNumBytesOut, null, null, null, null);
  }

  SourceTaskMetrics(
      Counter sourceNumRecordsOut,
      Counter sourceNumBytesOut,
      Counter taskNumRecordsIn,
      Counter taskNumRecordsOut,
      Counter taskNumBytesIn,
      Counter taskNumBytesOut) {
    this.sourceNumRecordsOut = sourceNumRecordsOut;
    this.sourceNumBytesOut = sourceNumBytesOut;
    this.taskNumRecordsIn = taskNumRecordsIn;
    this.taskNumRecordsOut = taskNumRecordsOut;
    this.taskNumBytesIn = taskNumBytesIn;
    this.taskNumBytesOut = taskNumBytesOut;
  }

  public boolean updateMetrics(SerialTask task, String planId) {
    return updateMetrics(task.collectStats(), planId);
  }

  boolean updateMetrics(SerialTaskStats taskStats, String planId) {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastUpdateTime < metricUpdateInterval) {
      return false;
    }
    try {
      ObjectNode planStats = findSourceStats(taskStats, planId);
      if (planStats != null) {
        long inputRows = planStats.get(keyInputRows).asLong();
        long inputBytes = planStats.get(keyInputBytes).asLong();
        syncCounter(sourceNumRecordsOut, inputRows);
        syncCounter(sourceNumBytesOut, inputBytes);
        syncCounter(taskNumRecordsIn, inputRows);
        syncCounter(taskNumRecordsOut, inputRows);
        syncCounter(taskNumBytesIn, inputBytes);
        syncCounter(taskNumBytesOut, inputBytes);
      }
    } catch (Exception e) {
      return false;
    }
    lastUpdateTime = currentTime;
    return true;
  }

  private ObjectNode findSourceStats(SerialTaskStats taskStats, String planId) {
    try {
      ObjectNode planStats = taskStats.planStats(planId);
      if (isSourceStats(planStats)) {
        return planStats;
      }
    } catch (Exception ignored) {
      // Fall back to the unique TableScan below.
    }

    ObjectNode sourceStats = null;
    List<ObjectNode> allPlanStats = taskStats.planStats();
    for (ObjectNode planStats : allPlanStats) {
      if (!isSourceStats(planStats)) {
        continue;
      }
      if (sourceStats != null) {
        return null;
      }
      sourceStats = planStats;
    }
    return sourceStats;
  }

  private boolean isSourceStats(ObjectNode planStats) {
    JsonNode operatorType = planStats.get(keyOperatorType);
    return operatorType != null
        && sourceOperatorName.equals(operatorType.asText())
        && planStats.has(keyInputRows)
        && planStats.has(keyInputBytes);
  }

  private void syncCounter(Counter counter, long value) {
    if (counter == null) {
      return;
    }
    long delta = value - counter.getCount();
    if (delta > 0) {
      counter.inc(delta);
    } else if (delta < 0) {
      counter.dec(-delta);
    }
  }
}

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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SourceOperatorMetrics {

  private final String keyOperatorType = "operatorType";
  private final String sourceOperatorName = "TableScan";
  private final String keyInputRows = "rawInputRows";
  private final String keyInputBytes = "rawInputBytes";
  private final long metricUpdateInterval = 2000;
  private Counter sourceNumRecordsOut;
  private Counter sourceNumBytesOut;
  private long lastUpdateTime = System.currentTimeMillis();

  public SourceOperatorMetrics(OperatorMetricGroup metricGroup) {
    sourceNumRecordsOut = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
    sourceNumBytesOut = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
  }

  public boolean updateMetrics(SerialTask task, String planId) {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastUpdateTime < metricUpdateInterval) {
      return false;
    }
    try {
      ObjectNode planStats = task.collectStats().planStats(planId);
      JsonNode jsonNode = planStats.get(keyOperatorType);
      if (jsonNode.asText().equals(sourceOperatorName)) {
        long numRecordsOut = planStats.get(keyInputRows).asInt();
        long numBytesOut = planStats.get(keyInputBytes).asInt();
        sourceNumRecordsOut.inc(numRecordsOut - sourceNumRecordsOut.getCount());
        sourceNumBytesOut.inc(numBytesOut - sourceNumBytesOut.getCount());
      }
    } catch (Exception e) {
      return false;
    }
    lastUpdateTime = currentTime;
    return true;
  }
}

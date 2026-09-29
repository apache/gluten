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
package org.apache.gluten.metrics;

public class Metrics implements IMetrics {
  public final String metricsJson;
  public final int numMetrics;
  public final SingleMetric singleMetric = new SingleMetric();

  public String taskStats;

  /** Create an instance for native metrics. */
  public Metrics(String metricsJson, int numMetrics, long veloxToArrow, String taskStats) {
    this.metricsJson = metricsJson;
    this.numMetrics = numMetrics;
    this.singleMetric.veloxToArrow = veloxToArrow;
    this.taskStats = taskStats;
  }

  public SingleMetric getSingleMetrics() {
    return singleMetric;
  }

  public static class SingleMetric {
    public long veloxToArrow;
  }
}

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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NativeMetrics implements IMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(NativeMetrics.class);

  public List<MetricsData> metricsDataList;
  public String metricsJson;

  public NativeMetrics(String metricsJson) {
    this.metricsJson = metricsJson;
    this.metricsDataList = NativeMetrics.deserializeMetricsJson(this.metricsJson);
  }

  public void setFinalOutputMetrics(long outputRowCount, long outputVectorCount) {
    if (CollectionUtils.isNotEmpty(this.metricsDataList)) {
      int listSize = this.metricsDataList.size();
      this.metricsDataList.get(listSize - 1).outputVectors = outputVectorCount;
      this.metricsDataList.get(listSize - 1).outputRows = outputRowCount;

      // skip 'kFetch' and 'kSort'
      if (listSize > 2
          && this.metricsDataList.get(listSize - 1).name.equals("kFetch")
          && this.metricsDataList.get(listSize - 2).name.equals("kSort")) {
        this.metricsDataList.get(listSize - 3).outputVectors = outputVectorCount;
        this.metricsDataList.get(listSize - 3).outputRows = outputRowCount;
      }
    }
  }

  /** Deserialize metrics json string to MetricsData */
  public static List<MetricsData> deserializeMetricsJson(String metricsJson) {
    if (metricsJson != null && !metricsJson.isEmpty()) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        List<MetricsData> metricsDataList =
            mapper.readValue(metricsJson, new TypeReference<List<MetricsData>>() {});
        Collections.reverse(metricsDataList);
        return metricsDataList;
      } catch (Exception e) {
        LOG.error("Deserialize metrics json string error:", e);
        return new ArrayList<MetricsData>();
      }
    }
    return new ArrayList<MetricsData>();
  }
}

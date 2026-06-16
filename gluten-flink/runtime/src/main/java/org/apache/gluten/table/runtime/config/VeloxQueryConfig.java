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
package org.apache.gluten.table.runtime.config;

import io.github.zhztheplayer.velox4j.config.Config;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.config.TableConfigOptions;

import java.util.HashMap;
import java.util.Map;

public class VeloxQueryConfig {

  private static final String keyVeloxAdjustTimestampToSessionTimeZone =
      "adjust_timestamp_to_session_timezone";
  private static final String keyVeloxSessionTimezone = "session_timezone";
  private static final String kStreamingAggregationMinOutputBatchRows =
      "streaming_aggregation_min_output_batch_rows";

  public static Config getConfig(RuntimeContext context) {
    if (!(context instanceof StreamingRuntimeContext)) {
      return Config.empty();
    }
    Configuration config = ((StreamingRuntimeContext) context).getJobConfiguration();
    Map<String, String> configMap = new HashMap<>();
    configMap.put(keyVeloxAdjustTimestampToSessionTimeZone, "true");
    String localTimeZone = config.get(TableConfigOptions.LOCAL_TIME_ZONE);
    // As flink's default timezone value is `default`, it is not a valid timezone id, so we should
    // convert it to `UTC` timezone.
    if (TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(localTimeZone)) {
      configMap.put(keyVeloxSessionTimezone, "UTC");
    } else {
      configMap.put(keyVeloxSessionTimezone, localTimeZone);
    }
    configMap.put(kStreamingAggregationMinOutputBatchRows, String.valueOf(1));
    return Config.create(configMap);
  }
}

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
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class VeloxConnectorConfig {

  private static final List<String> CONNECTORS =
      List.of(
          "connector-nexmark",
          "connector-kafka",
          "connector-fuzzer",
          "connector-filesystem",
          "connector-from-elements",
          "connector-print");
  private static final String keyTaskIndex = "task_index";
  private static final String keyQueryUUId = "query_uuid";

  public static ConnectorConfig getConfig(RuntimeContext context) {
    Map<String, String> configMap = new HashMap<>();
    TaskInfo taskInfo = context.getTaskInfo();
    configMap.put(keyTaskIndex, String.valueOf(taskInfo.getIndexOfThisSubtask()));
    configMap.put(
        keyQueryUUId,
        UUID.nameUUIDFromBytes(context.getJobInfo().getJobId().toHexString().getBytes())
            .toString());
    Config commonConfig = Config.create(configMap);
    Map<String, Config> connectorConfigMap = new HashMap<>();
    for (String connectorId : CONNECTORS) {
      connectorConfigMap.put(connectorId, commonConfig);
    }
    return ConnectorConfig.create(connectorConfigMap);
  }
}

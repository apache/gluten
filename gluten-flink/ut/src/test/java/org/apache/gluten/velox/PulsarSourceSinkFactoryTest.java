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
package org.apache.gluten.velox;

import org.apache.gluten.table.runtime.config.VeloxConnectorConfig;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class PulsarSourceSinkFactoryTest {

  @Test
  void serviceLoaderDiscoversPulsarFactory() {
    List<String> factoryNames =
        StreamSupport.stream(ServiceLoader.load(VeloxSourceSinkFactory.class).spliterator(), false)
            .map(factory -> factory.getClass().getName())
            .collect(Collectors.toList());

    assertThat(factoryNames).contains("org.apache.gluten.velox.PulsarSourceSinkFactory");
  }

  @Test
  @SuppressWarnings("unchecked")
  void runtimeConnectorConfigIncludesPulsarConnector() throws Exception {
    Field connectors = VeloxConnectorConfig.class.getDeclaredField("CONNECTORS");
    connectors.setAccessible(true);

    assertThat((List<String>) connectors.get(null)).contains("connector-pulsar");
  }

  @Test
  void tableParametersMapFlinkSqlOptionsToVeloxPulsarOptions() {
    PulsarSource source = new PulsarSource();
    source.options.put("pulsar.client.serviceUrl", "pulsar://127.0.0.1:16650");
    source.options.put("admin-url", "http://127.0.0.1:18080");
    source.options.put("topics", "persistent://public/default/gluten-pulsar-smoke");
    source.options.put("pulsar.consumer.subscriptionName", "gluten-test-sub");
    source.options.put("format", "raw");
    source.options.put("value.format", "json");
    source.options.put("source.start.message-id", "earliest");
    source.subscriptionType = FakeSubscriptionType.Shared;

    Map<String, String> tableParameters =
        PulsarSourceSinkFactory.buildTableParameters(null, source);

    assertThat(tableParameters)
        .containsEntry("service.url", "pulsar://127.0.0.1:16650")
        .containsEntry("admin.url", "http://127.0.0.1:18080")
        .containsEntry("topic", "persistent://public/default/gluten-pulsar-smoke")
        .containsEntry("subscription.name", "gluten-test-sub")
        .containsEntry("subscription.type", "shared")
        .containsEntry("format", "json")
        .containsEntry("initial.position", "earliest");
  }

  @Test
  void detectsWrappedPulsarSource() {
    assertThat(PulsarSourceSinkFactory.isPulsarSource(new WrappedSource())).isTrue();
  }

  private static class PulsarSource {
    private final Map<String, String> options = new HashMap<>();
    private FakeSubscriptionType subscriptionType;
  }

  private static class WrappedSource {
    private final Object source = new PulsarSource();
  }

  private enum FakeSubscriptionType {
    Shared
  }
}

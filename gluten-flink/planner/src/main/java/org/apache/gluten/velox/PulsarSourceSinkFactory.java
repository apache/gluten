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

import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.connector.PulsarConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.PulsarTableHandle;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public class PulsarSourceSinkFactory implements VeloxSourceSinkFactory {

  private static final String CONNECTOR_ID = "connector-pulsar";

  @SuppressWarnings("rawtypes")
  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (transformation instanceof SourceTransformation) {
      Source source = ((SourceTransformation) transformation).getSource();
      return isPulsarSource(source);
    }
    return false;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    RowType outputType =
        (RowType)
            LogicalTypeConverter.toVLType(
                ((InternalTypeInfo<?>) transformation.getOutputType()).toLogicalType());
    try {
      ScanTableSource tableSource =
          (ScanTableSource) parameters.get(ScanTableSource.class.getName());
      SourceTransformation sourceTransformation = (SourceTransformation) transformation;
      Source source = sourceTransformation.getSource();
      Map<String, String> pulsarTableParameters = buildTableParameters(tableSource, source);
      String topic = required(pulsarTableParameters, "topic");
      String serviceUrl = required(pulsarTableParameters, "service.url");
      String subscriptionName = required(pulsarTableParameters, "subscription.name");
      String format = pulsarTableParameters.getOrDefault("format", "raw");

      String planId = PlanNodeIdGenerator.newId();
      PulsarTableHandle pulsarTableHandle =
          new PulsarTableHandle(CONNECTOR_ID, topic, outputType, pulsarTableParameters);
      PulsarConnectorSplit connectorSplit =
          new PulsarConnectorSplit(CONNECTOR_ID, serviceUrl, topic, subscriptionName, format);
      TableScanNode pulsarScan =
          new TableScanNode(planId, outputType, pulsarTableHandle, List.of());
      GlutenStreamSource sourceOp =
          new GlutenStreamSource(
              new GlutenSourceFunction(
                  new StatefulPlanNode(pulsarScan.getId(), pulsarScan),
                  Map.of(pulsarScan.getId(), outputType),
                  pulsarScan.getId(),
                  connectorSplit,
                  RowData.class),
              "PulsarSource");
      return new LegacySourceTransformation<RowData>(
          sourceTransformation.getName(),
          sourceOp,
          transformation.getOutputType(),
          sourceTransformation.getParallelism(),
          sourceTransformation.getBoundedness(),
          false);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }

  @Override
  public Transformation<RowData> buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    throw new FlinkRuntimeException("Unimplemented method 'buildSink'");
  }

  static Map<String, String> buildTableParameters(Object tableSource, Object source) {
    Map<String, String> options = new HashMap<>();
    putAllStringOptions(options, tableSource);
    putAllStringOptions(options, source);

    firstString(tableSource, source, "serviceUrl", "serviceURL", "pulsarServiceUrl")
        .ifPresent(value -> options.put("service.url", value));
    option(options, "service-url", "pulsar.service.url", "service.url", "pulsar.client.serviceUrl")
        .ifPresent(value -> options.put("service.url", value));

    firstString(tableSource, source, "adminUrl", "adminURL", "pulsarAdminUrl")
        .ifPresent(value -> options.put("admin.url", value));
    option(options, "admin-url", "pulsar.admin.url", "admin.url")
        .ifPresent(value -> options.put("admin.url", value));

    firstTopic(tableSource, source).ifPresent(value -> options.put("topic", value));
    option(options, "topic", "topics").ifPresent(value -> options.put("topic", value));

    firstString(tableSource, source, "subscriptionName")
        .ifPresent(value -> options.put("subscription.name", value));
    option(
            options,
            "subscription-name",
            "subscription.name",
            "pulsar.subscription.name",
            "pulsar.consumer.subscriptionName",
            "properties.subscription.name",
            "source.subscription-name")
        .ifPresent(value -> options.put("subscription.name", value));

    options.putIfAbsent("subscription.name", "gluten-pulsar-" + UUID.randomUUID().toString());
    firstString(tableSource, source, "subscriptionType")
        .or(
            () ->
                option(
                    options,
                    "source.subscription-type",
                    "subscription.type",
                    "pulsar.subscription.type"))
        .map(PulsarSourceSinkFactory::normalizeSubscriptionType)
        .ifPresent(value -> options.put("subscription.type", value));
    option(options, "value.format")
        .or(
            () -> {
              String resolvedFormat = resolveFormat(tableSource);
              if (!"raw".equals(resolvedFormat)) {
                return Optional.of(resolvedFormat);
              }
              return option(options, "format").filter(value -> !"raw".equals(value));
            })
        .ifPresent(value -> options.put("format", value));
    options.putIfAbsent("format", "raw");
    option(
            options,
            "scan.startup.mode",
            "startup.mode",
            "initial.position",
            "source.start.message-id")
        .map(PulsarSourceSinkFactory::toInitialPosition)
        .ifPresent(value -> options.put("initial.position", value));
    options.putIfAbsent("initial.position", "latest");
    return options;
  }

  static boolean isPulsarSource(Object source) {
    return isPulsarSource(source, 0);
  }

  private static boolean isPulsarSource(Object source, int depth) {
    if (source == null || depth > 3) {
      return false;
    }
    if (source.getClass().getSimpleName().equals("PulsarSource")) {
      return true;
    }
    // Check wrapped source fields (e.g. Flink wraps PulsarSource in a SourceReaderContext wrapper)
    for (java.lang.reflect.Field f : source.getClass().getDeclaredFields()) {
      Class<?> fieldType = f.getType();
      // Only recurse into non-primitive, non-collection, non-map fields that could wrap a Source
      if (fieldType.isPrimitive()
          || fieldType == String.class
          || Collection.class.isAssignableFrom(fieldType)
          || Map.class.isAssignableFrom(fieldType)
          || fieldType.isEnum()) {
        continue;
      }
      f.setAccessible(true);
      try {
        Object inner = f.get(source);
        if (inner != null && isPulsarSource(inner, depth + 1)) {
          return true;
        }
      } catch (IllegalAccessException ignored) {
      }
    }
    return false;
  }

  private static String required(Map<String, String> options, String key) {
    String value = options.get(key);
    if (value == null || value.isEmpty()) {
      throw new FlinkRuntimeException("Missing Pulsar option: " + key);
    }
    return value;
  }

  private static String resolveFormat(Object tableSource) {
    Optional<Object> decodingFormat = firstField(tableSource, "valueDecodingFormat", "format");
    if (decodingFormat.isEmpty()) {
      decodingFormat =
          firstField(tableSource, "deserializationSchemaFactory")
              .flatMap(factory -> firstField(factory, "valueDecodingFormat"));
    }
    if (decodingFormat.isPresent() && decodingFormat.get() instanceof DecodingFormat) {
      String className = decodingFormat.get().getClass().getName();
      if (className.contains("JsonFormatFactory")) {
        return "json";
      } else if (className.contains("CsvFormatFactory")) {
        return "csv";
      }
    }
    return "raw";
  }

  private static String toInitialPosition(String startupMode) {
    String normalized = startupMode.toLowerCase().replace("_", "-");
    if (normalized.contains("earliest")) {
      return "earliest";
    }
    if (normalized.contains("latest")) {
      return "latest";
    }
    return normalized;
  }

  private static String normalizeSubscriptionType(String subscriptionType) {
    return subscriptionType.toLowerCase(Locale.ROOT).replace("-", "_");
  }

  private static Optional<String> firstTopic(Object... targets) {
    for (Object target : targets) {
      Optional<Object> topics = firstField(target, "topics", "topic", "topicName");
      if (topics.isPresent()) {
        Object value = topics.get();
        if (value instanceof Collection) {
          return ((Collection<?>) value).stream().findFirst().map(String::valueOf);
        }
        if (value instanceof String[]) {
          return Arrays.stream((String[]) value).findFirst();
        }
        String text = String.valueOf(value);
        if (!text.isEmpty()) {
          return Optional.of(text);
        }
      }
    }
    return Optional.empty();
  }

  private static Optional<String> firstString(Object first, Object second, String... fieldNames) {
    return firstField(first, fieldNames)
        .or(() -> firstField(second, fieldNames))
        .map(String::valueOf)
        .filter(value -> !value.isEmpty());
  }

  private static Optional<String> option(Map<String, String> options, String... keys) {
    for (String key : keys) {
      String value = options.get(key);
      if (value != null && !value.isEmpty()) {
        return Optional.of(value);
      }
    }
    return Optional.empty();
  }

  private static void putAllStringOptions(Map<String, String> output, Object target) {
    firstField(target, "properties", "tableOptions", "options", "configuration")
        .ifPresent(value -> putAllStringOptions(output, value));
    if (target instanceof Properties) {
      Properties properties = (Properties) target;
      for (String key : properties.stringPropertyNames()) {
        output.put(key, properties.getProperty(key));
      }
    } else if (target instanceof Map) {
      ((Map<?, ?>) target)
          .forEach(
              (key, value) -> {
                if (key != null && value != null) {
                  output.put(String.valueOf(key), String.valueOf(value));
                }
              });
    }
  }

  private static Optional<Object> firstField(Object target, String... fieldNames) {
    if (target == null) {
      return Optional.empty();
    }
    for (String fieldName : fieldNames) {
      Optional<Object> value = field(target, fieldName);
      if (value.isPresent()) {
        return value;
      }
    }
    return Optional.empty();
  }

  private static Optional<Object> field(Object target, String fieldName) {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        return Optional.ofNullable(ReflectUtils.getObjectField(clazz, target, fieldName));
      } catch (FlinkRuntimeException e) {
        clazz = clazz.getSuperclass();
      }
    }
    return Optional.empty();
  }
}

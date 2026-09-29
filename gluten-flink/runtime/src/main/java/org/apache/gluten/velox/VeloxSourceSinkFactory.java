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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public interface VeloxSourceSinkFactory {

  static final Logger LOG = LoggerFactory.getLogger(VeloxSourceSinkFactory.class);

  String FACTORY_CLASS_LOADER_KEY = "velox.source-sink.factory.classloader";

  List<String> FACTORY_CLASS_NAMES =
      List.of(
          "org.apache.gluten.velox.FromElementsSourceFactory",
          "org.apache.gluten.velox.KafkaSourceSinkFactory",
          "org.apache.gluten.velox.PulsarSourceSinkFactory",
          "org.apache.gluten.velox.PrintSinkFactory",
          "org.apache.gluten.velox.NexmarkSourceFactory",
          "org.apache.gluten.velox.FileSystemSinkFactory",
          "org.apache.gluten.velox.FuzzerSourceSinkFactory");

  /** Match the conditions to determine if the operator can be offloaded to velox. */
  boolean match(Transformation<RowData> transformation);

  /** Build source transformation that offload the operator to velox. */
  Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters);

  /** Build sink transformation that offload the operator to velox. */
  Transformation<RowData> buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters);

  /** Choose the matched source/sink factory by given transformation. */
  private static Optional<VeloxSourceSinkFactory> getFactory(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    ServiceLoader<VeloxSourceSinkFactory> factories =
        ServiceLoader.load(VeloxSourceSinkFactory.class);
    try {
      for (VeloxSourceSinkFactory factory : factories) {
        if (factory.match(transformation)) {
          return Optional.of(factory);
        }
      }
    } catch (ServiceConfigurationError e) {
      LOG.warn("Failed to load Velox source/sink factory", e);
    }
    for (String factoryClassName : FACTORY_CLASS_NAMES) {
      Optional<VeloxSourceSinkFactory> factory = loadFactory(factoryClassName, parameters);
      if (factory.isPresent() && factory.get().match(transformation)) {
        return factory;
      }
    }
    return Optional.empty();
  }

  private static Optional<VeloxSourceSinkFactory> loadFactory(
      String factoryClassName, Map<String, Object> parameters) {
    Object classLoader = parameters.get(FACTORY_CLASS_LOADER_KEY);
    if (classLoader instanceof ClassLoader) {
      Optional<VeloxSourceSinkFactory> factory =
          loadFactory(factoryClassName, (ClassLoader) classLoader);
      if (factory.isPresent()) {
        return factory;
      }
    }
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    Optional<VeloxSourceSinkFactory> factory = loadFactory(factoryClassName, contextClassLoader);
    if (factory.isPresent()) {
      return factory;
    }
    return loadFactory(factoryClassName, VeloxSourceSinkFactory.class.getClassLoader());
  }

  private static Optional<VeloxSourceSinkFactory> loadFactory(
      String factoryClassName, ClassLoader classLoader) {
    try {
      return Optional.of(
          (VeloxSourceSinkFactory)
              Class.forName(factoryClassName, true, classLoader)
                  .getDeclaredConstructor()
                  .newInstance());
    } catch (ClassNotFoundException e) {
      return Optional.empty();
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new RuntimeException("Failed to instantiate Velox source/sink factory", e);
    }
  }

  /** Build Velox source, or fallback to flink orignal source . */
  static Transformation<RowData> buildSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    Optional<VeloxSourceSinkFactory> factory = getFactory(transformation, parameters);
    if (factory.isEmpty()) {
      LOG.warn(
          "Not find matched factory to build velox source transformation, and we will use flink original transformation {} instead.",
          transformation.getClass().getName());
      return transformation;
    } else {
      return factory.get().buildVeloxSource(transformation, parameters);
    }
  }

  /** Build Velox sink, or fallback to flink original sink. */
  static Transformation<RowData> buildSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    Optional<VeloxSourceSinkFactory> factory = getFactory(transformation, parameters);
    if (factory.isEmpty()) {
      LOG.warn(
          "Not find matched factory to build velox sink transformation, and we will use flink original transformation {} instead.",
          transformation.getClass().getName());
      return transformation;
    } else {
      return factory.get().buildVeloxSink(transformation, parameters);
    }
  }
}

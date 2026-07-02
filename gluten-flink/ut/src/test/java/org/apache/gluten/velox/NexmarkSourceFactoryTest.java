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

import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.NexmarkConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.NexmarkGeneratorConfig;
import io.github.zhztheplayer.velox4j.connector.NexmarkParallelSplit;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NexmarkSourceFactoryTest {

  private static final String NEXMARK_SOURCE_CN = "com.github.nexmark.flink.source.NexmarkSource";
  private static final String NEXMARK_CONFIG_CN = "com.github.nexmark.flink.NexmarkConfiguration";
  private static final String GENERATOR_CONFIG_CN =
      "com.github.nexmark.flink.generator.GeneratorConfig";

  @SuppressWarnings("rawtypes")
  @Test
  void testBuildVeloxSourceWrapsSplitsInNexmarkParallelSplit() throws Exception {
    SourceTransformation tx = newSourceTransformation(/* parallelism= */ 2);

    NexmarkSourceFactory factory = new NexmarkSourceFactory();
    Transformation<RowData> result = factory.buildVeloxSource(tx, Collections.emptyMap());

    LegacySourceTransformation<RowData> legacy =
        assertInstanceOf(LegacySourceTransformation.class, result);
    GlutenStreamSource streamSource =
        assertInstanceOf(GlutenStreamSource.class, legacy.getOperator());

    ConnectorSplit split = streamSource.getConnectorSplit();
    NexmarkParallelSplit parallel = assertInstanceOf(NexmarkParallelSplit.class, split);

    NexmarkConnectorSplit s0 =
        assertInstanceOf(NexmarkConnectorSplit.class, parallel.getSubtaskSplit(0, 2));
    NexmarkConnectorSplit s1 =
        assertInstanceOf(NexmarkConnectorSplit.class, parallel.getSubtaskSplit(1, 2));

    NexmarkGeneratorConfig c0 = s0.getConfig();
    NexmarkGeneratorConfig c1 = s1.getConfig();
    assertEquals(0L, c0.getFirstEventId());
    assertEquals(500L, c0.getMaxEventsOrZero());
    assertEquals(500L, c1.getFirstEventId());
    assertEquals(500L, c1.getMaxEventsOrZero());
  }

  @SuppressWarnings("rawtypes")
  @Test
  void testBuildVeloxSourceAtParallelismOneStillProducesParallelSplit() throws Exception {
    SourceTransformation tx = newSourceTransformation(/* parallelism= */ 1);

    NexmarkSourceFactory factory = new NexmarkSourceFactory();
    Transformation<RowData> result = factory.buildVeloxSource(tx, Collections.emptyMap());

    LegacySourceTransformation<RowData> legacy =
        assertInstanceOf(LegacySourceTransformation.class, result);
    GlutenStreamSource streamSource =
        assertInstanceOf(GlutenStreamSource.class, legacy.getOperator());

    NexmarkParallelSplit parallel =
        assertInstanceOf(NexmarkParallelSplit.class, streamSource.getConnectorSplit());
    NexmarkConnectorSplit s0 =
        assertInstanceOf(NexmarkConnectorSplit.class, parallel.getSubtaskSplit(0, 1));

    assertEquals(0L, s0.getConfig().getFirstEventId());
    assertEquals(1000L, s0.getConfig().getMaxEventsOrZero());
  }

  @Test
  void testBuildVeloxSourceRejectsNonSourceTransformation() {
    NexmarkSourceFactory factory = new NexmarkSourceFactory();
    assertThrows(
        ClassCastException.class,
        () -> factory.buildVeloxSource(new StubTransformation(), Collections.emptyMap()));
  }

  @SuppressWarnings("rawtypes")
  private static SourceTransformation newSourceTransformation(int parallelism) throws Exception {
    Object nexmarkSource = newNexmarkSource(1000L);
    Constructor<?> ctor =
        SourceTransformation.class.getDeclaredConstructor(
            String.class,
            org.apache.flink.api.connector.source.Source.class,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.class,
            org.apache.flink.api.common.typeinfo.TypeInformation.class,
            int.class);
    return (SourceTransformation)
        ctor.newInstance(
            "nexmark-source",
            nexmarkSource,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
            InternalTypeInfo.of(RowType.of(new IntType())),
            parallelism);
  }

  private static Object newNexmarkSource(long maxEvents) throws Exception {
    Object nexmarkConfig = Class.forName(NEXMARK_CONFIG_CN).getDeclaredConstructor().newInstance();
    java.lang.reflect.Field numEvents = nexmarkConfig.getClass().getDeclaredField("numEvents");
    numEvents.setAccessible(true);
    numEvents.setLong(nexmarkConfig, maxEvents);

    Class<?> generatorConfigCls = Class.forName(GENERATOR_CONFIG_CN);
    Constructor<?> generatorConfigCtor =
        generatorConfigCls.getDeclaredConstructor(
            Class.forName(NEXMARK_CONFIG_CN),
            long.class,
            long.class,
            long.class,
            long.class,
            long.class);
    Object generatorConfig =
        generatorConfigCtor.newInstance(nexmarkConfig, 0L, 0L, maxEvents, 0L, 0L);

    Class<?> nexmarkSourceCls = Class.forName(NEXMARK_SOURCE_CN);
    Constructor<?> nexmarkSourceCtor =
        nexmarkSourceCls.getDeclaredConstructor(
            generatorConfigCls, org.apache.flink.api.common.typeinfo.TypeInformation.class);
    nexmarkSourceCtor.setAccessible(true);
    return nexmarkSourceCtor.newInstance(
        generatorConfig, InternalTypeInfo.of(RowType.of(new IntType())));
  }

  private static final class StubTransformation extends Transformation<RowData> {
    StubTransformation() {
      super("stub", InternalTypeInfo.of(RowType.of(new IntType())), 1);
    }

    @Override
    public java.util.List<Transformation<?>> getInputs() {
      return Collections.emptyList();
    }

    @Override
    protected java.util.List<Transformation<?>> getTransitivePredecessorsInternal() {
      return Collections.emptyList();
    }
  }
}

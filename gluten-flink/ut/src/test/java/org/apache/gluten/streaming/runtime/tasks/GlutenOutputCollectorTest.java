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
package org.apache.gluten.streaming.runtime.tasks;

import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.stateful.StatefulWatermark;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OutputWithChainingCheck;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GlutenOutputCollectorTest {

  @Test
  void emitWatermarkUpdatesGaugeAndBroadcastsToAllOutputs() {
    TestingOutput<StatefulElement> firstOutput = new TestingOutput<>();
    TestingOutput<StatefulElement> secondOutput = new TestingOutput<>();
    GlutenOutputCollector<StatefulElement> collector =
        new GlutenOutputCollector<>(
            Map.of("first", firstOutput, "second", secondOutput), new SimpleCounter());

    collector.emitWatermark(new Watermark(100L));

    assertThat(collector.getWatermarkGauge().getValue()).isEqualTo(100L);
    assertThat(firstOutput.watermarks).extracting(Watermark::getTimestamp).containsExactly(100L);
    assertThat(secondOutput.watermarks).extracting(Watermark::getTimestamp).containsExactly(100L);
  }

  @Test
  void collectStatefulWatermarkUpdatesGaugeAndRoutesByNodeId() {
    TestingOutput<StatefulElement> firstOutput = new TestingOutput<>();
    TestingOutput<StatefulElement> secondOutput = new TestingOutput<>();
    GlutenOutputCollector<StatefulElement> collector =
        new GlutenOutputCollector<>(
            Map.of("first", firstOutput, "second", secondOutput), new SimpleCounter());

    collector.collect(new StreamRecord<>(new StatefulWatermark("first", 200L)));

    assertThat(collector.getWatermarkGauge().getValue()).isEqualTo(200L);
    assertThat(firstOutput.watermarks).extracting(Watermark::getTimestamp).containsExactly(200L);
    assertThat(secondOutput.watermarks).isEmpty();
  }

  private static class TestingOutput<T> implements OutputWithChainingCheck<StreamRecord<T>> {
    private final List<Watermark> watermarks = new ArrayList<>();
    private final List<StreamRecord<T>> records = new ArrayList<>();
    private final WatermarkGauge watermarkGauge = new WatermarkGauge();

    @Override
    public void emitWatermark(Watermark watermark) {
      watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
      watermarks.add(watermark);
    }

    @Override
    public WatermarkGauge getWatermarkGauge() {
      return watermarkGauge;
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {}

    @Override
    public void collect(StreamRecord<T> record) {
      records.add(record);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

    @Override
    public void close() {}

    @Override
    public void emitRecordAttributes(RecordAttributes recordAttributes) {}

    @Override
    public boolean collectAndCheckIfChained(StreamRecord<T> record) {
      collect(record);
      return false;
    }

    @Override
    public <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record) {
      collect(outputTag, record);
      return false;
    }
  }
}

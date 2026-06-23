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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OutputWithChainingCheck;
import org.apache.flink.streaming.runtime.tasks.WatermarkGaugeExposingOutput;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * Collector for gluten, it may contain several outputs, dispatch the record according to node id.
 */
public class GlutenOutputCollector<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

  protected final Map<String, OutputWithChainingCheck<StreamRecord<T>>> outputs;
  private final WatermarkGauge watermarkGauge = new WatermarkGauge();
  protected final Counter numRecordsOutForTask;

  public GlutenOutputCollector(
      Map<String, OutputWithChainingCheck<StreamRecord<T>>> outputs, Counter numRecordsOutForTask) {
    this.outputs = outputs;
    this.numRecordsOutForTask = numRecordsOutForTask;
  }

  @Override
  public void emitWatermark(Watermark watermark) {
    // This is only used when task is finished, so broadcast it.
    outputs.values().forEach(output -> output.emitWatermark(watermark));
  }

  @Override
  public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void emitLatencyMarker(LatencyMarker latencyMarker) {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public Gauge<Long> getWatermarkGauge() {
    return watermarkGauge;
  }

  @Override
  public void collect(StreamRecord<T> record) {
    StatefulElement element = (StatefulElement) record.getValue();
    OutputWithChainingCheck<StreamRecord<T>> output = outputs.get(element.getNodeId());
    if (element.isWatermark()) {
      output.emitWatermark(new Watermark(element.asWatermark().getTimestamp()));
    } else {
      output.collect(record);
    }
  }

  @Override
  public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void close() {
    outputs.values().forEach(output -> output.close());
  }

  @Override
  public void emitRecordAttributes(RecordAttributes recordAttributes) {
    throw new RuntimeException("Not implemented for gluten");
  }
}

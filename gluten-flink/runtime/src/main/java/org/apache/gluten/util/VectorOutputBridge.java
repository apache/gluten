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
package org.apache.gluten.util;

import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.arrow.memory.BufferAllocator;

import java.io.Serializable;

/**
 * Interface for converting output data from StatefulRecord to Flink StreamRecord. Different
 * implementations handle different output types.
 */
public interface VectorOutputBridge<OUT> extends Serializable {

  /**
   * Converts a StatefulRecord to the appropriate output type and collects it to the output.
   *
   * @param collector the Flink output collector
   * @param record the StatefulRecord to convert and collect
   * @param allocator buffer allocator for converting RowVector
   * @param outputType the RowType schema of the output
   */
  void collect(
      Output<StreamRecord<OUT>> collector,
      StatefulRecord record,
      BufferAllocator allocator,
      RowType outputType);

  /** Factory for creating VectorOutputBridge instances based on output type. */
  class Factory {
    /**
     * Creates a VectorOutputBridge instance for the given output class.
     *
     * @param outputClass the output class type
     * @param <OUT> the output type
     * @return a VectorOutputBridge instance
     * @throws UnsupportedOperationException if output class is not supported
     */
    public static <OUT> VectorOutputBridge<OUT> create(Class<OUT> outputClass) {
      if (outputClass.isAssignableFrom(org.apache.flink.table.data.RowData.class)) {
        @SuppressWarnings("unchecked")
        VectorOutputBridge<OUT> bridge = (VectorOutputBridge<OUT>) new RowDataOutputBridge();
        return bridge;
      } else if (outputClass.isAssignableFrom(StatefulRecord.class)) {
        @SuppressWarnings("unchecked")
        VectorOutputBridge<OUT> bridge = (VectorOutputBridge<OUT>) new StatefulRecordOutputBridge();
        return bridge;
      } else {
        throw new UnsupportedOperationException(
            "Unsupported output class: " + outputClass.getName());
      }
    }
  }

  /**
   * Implementation for RowData output type. Converts RowVector from StatefulRecord to RowData and
   * collects to output.
   */
  class RowDataOutputBridge implements VectorOutputBridge<org.apache.flink.table.data.RowData> {
    private static final long serialVersionUID = 1L;
    private transient StreamRecord<org.apache.flink.table.data.RowData> outputElement;

    public RowDataOutputBridge() {
      this.outputElement = new StreamRecord<>(null);
    }

    @Override
    public void collect(
        Output<StreamRecord<org.apache.flink.table.data.RowData>> collector,
        StatefulRecord record,
        BufferAllocator allocator,
        RowType outputType) {
      java.util.List<org.apache.flink.table.data.RowData> rows =
          org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor.toRowData(
              record.getRowVector(), allocator, outputType);
      for (org.apache.flink.table.data.RowData row : rows) {
        collector.collect(getOrCreateOutputElement().replace(row));
      }
    }

    private StreamRecord<org.apache.flink.table.data.RowData> getOrCreateOutputElement() {
      if (outputElement == null) {
        outputElement = new StreamRecord<>(null);
      }
      return outputElement;
    }
  }

  /**
   * Implementation for StatefulRecord output type. Passes through the StatefulRecord directly to
   * output.
   */
  class StatefulRecordOutputBridge implements VectorOutputBridge<StatefulRecord> {
    private static final long serialVersionUID = 1L;
    private transient StreamRecord<StatefulRecord> outputElement;

    public StatefulRecordOutputBridge() {
      this.outputElement = new StreamRecord<>(null);
    }

    @Override
    public void collect(
        Output<StreamRecord<StatefulRecord>> collector,
        StatefulRecord record,
        BufferAllocator allocator,
        RowType outputType) {
      collector.collect(getOrCreateOutputElement().replace(record));
    }

    private StreamRecord<StatefulRecord> getOrCreateOutputElement() {
      if (outputElement == null) {
        outputElement = new StreamRecord<>(null);
      }
      return outputElement;
    }
  }
}

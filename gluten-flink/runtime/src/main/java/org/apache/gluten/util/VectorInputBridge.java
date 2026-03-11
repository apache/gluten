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

import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.arrow.memory.BufferAllocator;

import java.io.Serializable;

/**
 * Interface for converting input data from Flink StreamRecord to StatefulRecord. Different
 * implementations handle different input types.
 */
public interface VectorInputBridge<IN> extends Serializable {

  /**
   * Converts a StreamRecord to StatefulRecord based on the input type.
   *
   * @param inputData the input StreamRecord
   * @param allocator buffer allocator for creating RowVector
   * @param session Velox session
   * @param inputType the RowType schema of the input
   * @return StatefulRecord containing the converted or original data
   */
  StatefulRecord convertToStatefulRecord(
      StreamRecord<IN> inputData, BufferAllocator allocator, Session session, RowType inputType);

  /** Factory for creating VectorInputBridge instances based on input type. */
  class Factory {
    /**
     * Creates a VectorInputBridge instance for the given input class.
     *
     * @param inputClass the input class type
     * @param nodeId the node ID for the bridge
     * @param <IN> the input type
     * @return a VectorInputBridge instance
     * @throws UnsupportedOperationException if input class is not supported
     */
    public static <IN> VectorInputBridge<IN> create(Class<IN> inputClass, String nodeId) {
      if (inputClass.isAssignableFrom(org.apache.flink.table.data.RowData.class)) {
        @SuppressWarnings("unchecked")
        VectorInputBridge<IN> bridge = (VectorInputBridge<IN>) new RowDataInputBridge(nodeId);
        return bridge;
      } else if (inputClass.isAssignableFrom(StatefulRecord.class)) {
        @SuppressWarnings("unchecked")
        VectorInputBridge<IN> bridge = (VectorInputBridge<IN>) new StatefulRecordInputBridge();
        return bridge;
      } else {
        throw new UnsupportedOperationException("Unsupported input class: " + inputClass.getName());
      }
    }
  }

  /**
   * Implementation for RowData input type. Converts RowData to RowVector and wraps in
   * StatefulRecord.
   */
  class RowDataInputBridge implements VectorInputBridge<org.apache.flink.table.data.RowData> {
    private static final long serialVersionUID = 1L;
    private final String nodeId;

    public RowDataInputBridge(String nodeId) {
      this.nodeId = nodeId;
    }

    @Override
    public StatefulRecord convertToStatefulRecord(
        StreamRecord<org.apache.flink.table.data.RowData> inputData,
        BufferAllocator allocator,
        Session session,
        RowType inputType) {
      org.apache.flink.table.data.RowData rowData = inputData.getValue();
      var rowVector =
          org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor.fromRowData(
              rowData, allocator, session, inputType);
      StatefulRecord statefulRecord = new StatefulRecord(nodeId, rowVector.id(), 0, false, -1);
      statefulRecord.setRowVector(rowVector);
      return statefulRecord;
    }
  }

  /** Implementation for StatefulRecord input type. Passes through the StatefulRecord directly. */
  class StatefulRecordInputBridge implements VectorInputBridge<StatefulRecord> {
    private static final long serialVersionUID = 1L;

    @Override
    public StatefulRecord convertToStatefulRecord(
        StreamRecord<StatefulRecord> inputData,
        BufferAllocator allocator,
        Session session,
        RowType inputType) {
      // Pass through the StatefulRecord directly. This bridge does not take ownership of or close.
      return inputData.getValue();
    }
  }
}

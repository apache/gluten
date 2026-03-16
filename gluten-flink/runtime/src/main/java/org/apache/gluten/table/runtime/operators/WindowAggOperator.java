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
package org.apache.gluten.table.runtime.operators;

import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.RocksDBKeyedStateBackendParameters;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.tvf.state.WindowValueState;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType.RowField;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WindowAggOperator<IN, OUT, W> extends GlutenOneInputOperator<IN, OUT> {
  private final String windowStateName = "window-aggs";
  private WindowValueState<W> windowState;
  private InternalTypeInfo<RowData> keyType;
  private String[] accNames;
  private LogicalType[] accTypes;

  public WindowAggOperator(
      StatefulPlanNode plan,
      String id,
      RowType inputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass,
      String description,
      InternalTypeInfo<RowData> keyType,
      String[] accNames,
      LogicalType[] accTypes) {
    super(plan, id, inputType, outputTypes, inClass, outClass, description);
    this.keyType = keyType;
    this.accNames = accNames;
    this.accTypes = accTypes;
  }

  public InternalTypeInfo<RowData> getKeyTye() {
    return keyType;
  }

  public String[] getAggregateNames() {
    return accNames;
  }

  public LogicalType[] getAggregateTypes() {
    return accTypes;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    KeyedStateBackend<?> stateBackend = getKeyedStateBackend();
    ValueStateDescriptor<RowData> descriptor =
        new ValueStateDescriptor<>(windowStateName, new RowDataSerializer(accTypes));
    ValueState<RowData> state =
        stateBackend.getOrCreateKeyedState(LongSerializer.INSTANCE, descriptor);
    this.windowState = new WindowValueState<>((InternalValueState<RowData, W, RowData>) state);
    if (stateBackend instanceof RocksDBKeyedStateBackend) {
      RocksDBKeyedStateBackend<RowData> keyedStateBackend =
          (RocksDBKeyedStateBackend<RowData>) stateBackend;
      RocksDB dbInstance =
          (RocksDB)
              ReflectUtils.getObjectField(RocksDBKeyedStateBackend.class, keyedStateBackend, "db");
      ColumnFamilyHandle columnFamilyHandle =
          (ColumnFamilyHandle)
              ReflectUtils.invokeObjectMethod(
                  RocksDBKeyedStateBackend.class,
                  keyedStateBackend,
                  "getColumnFamilyHandle",
                  new Class<?>[] {String.class},
                  new Object[] {windowStateName});
      String jobId = getRuntimeContext().getJobInfo().getJobId().toString();
      String operartorId = getRuntimeContext().getOperatorUniqueID().toString();
      List<RowField> accFields = new ArrayList<>();
      for (int i = 0; i < accNames.length; ++i) {
        accFields.add(new RowField(accNames[i], accTypes[i]));
      }
      RocksDBKeyedStateBackendParameters parameters =
          new RocksDBKeyedStateBackendParameters(
              jobId,
              operartorId,
              1,
              dbInstance.getNativeHandle(),
              keyedStateBackend.getReadOptions().getNativeHandle(),
              keyedStateBackend.getWriteOptions().getNativeHandle(),
              List.of(windowStateName),
              Map.of(windowStateName, operartorId),
              Map.of(windowStateName, columnFamilyHandle.getNativeHandle()),
              Map.of(windowStateName, LogicalTypeConverter.toVLType(keyType.toLogicalType())),
              Map.of(
                  windowStateName,
                  LogicalTypeConverter.toVLType(
                      new org.apache.flink.table.types.logical.RowType(accFields))),
              Map.of(windowStateName, new BigIntType()));
      task.initializeState(0, parameters);
    }
  }

  @Override
  public <NIN, NOUT> WindowAggOperator<NIN, NOUT, W> cloneWithInputOutputClasses(
      Class<NIN> newInClass, Class<NOUT> newOutClass) {
    return new WindowAggOperator<>(
        getPlanNode(),
        getId(),
        getInputType(),
        getOutputTypes(),
        newInClass,
        newOutClass,
        getDescription(),
        keyType,
        accNames,
        accTypes);
  }

  @Override
  public void setCurrentKey(Object key) {}

  public void close() throws Exception {
    super.close();
    if (windowState != null) {}
  }
}

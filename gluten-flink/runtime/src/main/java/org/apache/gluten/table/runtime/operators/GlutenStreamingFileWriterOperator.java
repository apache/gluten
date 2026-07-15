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

import org.apache.gluten.util.Utils;

import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GlutenStreamingFileWriterOperator<IN>
    extends GlutenOneInputOperator<IN, PartitionCommitInfo> {

  private static final Logger LOG =
      LoggerFactory.getLogger(GlutenStreamingFileWriterOperator.class);

  private transient ListState<String> checkpointState;
  private transient String[] restoredCheckpointRecords = new String[0];

  public GlutenStreamingFileWriterOperator(
      StatefulPlanNode plan,
      String id,
      RowType inputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      String description) {
    super(plan, id, inputType, outputTypes, inClass, PartitionCommitInfo.class, description);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <NIN, NOUT> GlutenOneInputOperator<NIN, NOUT> cloneWithInputOutputClasses(
      StatefulPlanNode plan, Class<NIN> inClass, Class<NOUT> outClass) {
    return (GlutenOneInputOperator<NIN, NOUT>)
        new GlutenStreamingFileWriterOperator<>(
            plan, getId(), getInputType(), getOutputTypes(), inClass, getDescription());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    output.emitWatermark(mark);
  }

  @Override
  protected void snapshotNativeState(StateSnapshotContext context) throws Exception {
    checkpointState.clear();
    String[] checkpointRecords = task.snapshotState(context.getCheckpointId());
    for (String checkpointRecord : checkpointRecords) {
      checkpointState.add(checkpointRecord);
    }
  }

  @Override
  protected void initializeNativeState(StateInitializationContext context) throws Exception {
    checkpointState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>("gluten-native-file-writer-checkpoint", String.class));
    if (context.isRestored()) {
      List<String> records = new ArrayList<>();
      for (String checkpointRecord : checkpointState.get()) {
        records.add(checkpointRecord);
      }
      restoredCheckpointRecords = records.toArray(new String[0]);
    }
    LOG.info(
        "Restore native file writer state for operator {}, restored {}, records {}",
        getDescription(),
        context.isRestored(),
        Arrays.toString(restoredCheckpointRecords));
    if (task == null) {
      initSession();
    }
    task.initializeState(0, null, restoredCheckpointRecords);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    String[] committed = task.notifyCheckpointComplete(checkpointId);
    if (committed != null) {
      TaskInfo taskInfo = getRuntimeContext().getTaskInfo();
      output.collect(
          new StreamRecord<PartitionCommitInfo>(
              (PartitionCommitInfo)
                  Utils.constructCommitInfo(
                      checkpointId,
                      taskInfo.getIndexOfThisSubtask(),
                      taskInfo.getNumberOfParallelSubtasks(),
                      committed)));
    }
    super.notifyCheckpointComplete(checkpointId);
  }
}

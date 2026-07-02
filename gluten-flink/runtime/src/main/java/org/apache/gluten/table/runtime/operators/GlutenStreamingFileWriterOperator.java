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
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Map;

public class GlutenStreamingFileWriterOperator<IN>
    extends GlutenOneInputOperator<IN, PartitionCommitInfo> {

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

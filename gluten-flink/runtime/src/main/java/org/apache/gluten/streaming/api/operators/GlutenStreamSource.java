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
package org.apache.gluten.streaming.api.operators;

import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;
import org.apache.gluten.util.ReflectUtils;
import org.apache.gluten.util.Utils;

import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.util.Map;

/** Legacy stream source operator in gluten, which will call Velox to run. */
public class GlutenStreamSource extends StreamSource implements GlutenOperator {
  private final GlutenSourceFunction sourceFunction;
  private final String description;

  public GlutenStreamSource(GlutenSourceFunction function) {
    super(function);
    sourceFunction = function;
    this.description = "";
  }

  public GlutenStreamSource(GlutenSourceFunction function, String description) {
    super(function);
    sourceFunction = function;
    this.description = description;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public void open() throws Exception {
    super.open();
  }

  @Override
  public StatefulPlanNode getPlanNode() {
    return sourceFunction.getPlanNode();
  }

  @Override
  public RowType getInputType() {
    return null;
  }

  @Override
  public Map<String, RowType> getOutputTypes() {
    return sourceFunction.getOutputTypes();
  }

  @Override
  public String getId() {
    return sourceFunction.getId();
  }

  public ConnectorSplit getConnectorSplit() {
    return sourceFunction.getConnectorSplit();
  }

  @SuppressWarnings("rawtypes")
  private SourceFunction.SourceContext getSourceContext() {
    return (SourceFunction.SourceContext)
        ReflectUtils.getObjectField(StreamSource.class, this, "ctx");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    String[] committed = sourceFunction.notifyCheckpointComplete(checkpointId);
    SourceFunction.SourceContext sourceContext = getSourceContext();
    TaskInfo taskInfo = getRuntimeContext().getTaskInfo();
    if (sourceContext != null
        && committed != null
        && taskInfo.getTaskName().contains("StreamingFileWriter")) {
      sourceContext.collect(
          Utils.constructCommitInfo(
              checkpointId,
              taskInfo.getIndexOfThisSubtask(),
              taskInfo.getNumberOfParallelSubtasks(),
              committed));
    }
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    super.notifyCheckpointAborted(checkpointId);
    sourceFunction.notifyCheckpointAborted(checkpointId);
  }
}

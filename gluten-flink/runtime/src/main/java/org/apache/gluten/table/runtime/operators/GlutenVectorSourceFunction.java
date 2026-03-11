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

import org.apache.gluten.table.runtime.config.VeloxQueryConfig;
import org.apache.gluten.table.runtime.metrics.SourceTaskMetrics;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Gluten legacy source function, call velox plan to execute. It sends RowVector to downstream
 * instead of RowData to avoid data convert.
 */
public class GlutenVectorSourceFunction extends RichParallelSourceFunction<StatefulElement>
    implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(GlutenVectorSourceFunction.class);

  private final StatefulPlanNode planNode;
  private final Map<String, RowType> outputTypes;
  private final String id;
  private final ConnectorSplit split;
  private volatile boolean isRunning = true;

  private Session session;
  private Query query;
  private BufferAllocator allocator;
  private MemoryManager memoryManager;
  private SerialTask task;
  private SourceTaskMetrics taskMetrics;

  public GlutenVectorSourceFunction(
      StatefulPlanNode planNode,
      Map<String, RowType> outputTypes,
      String id,
      ConnectorSplit split) {
    this.planNode = planNode;
    this.outputTypes = outputTypes;
    this.id = id;
    this.split = split;
  }

  public StatefulPlanNode getPlanNode() {
    return planNode;
  }

  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  public String getId() {
    return id;
  }

  public ConnectorSplit getConnectorSplit() {
    return split;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    if (memoryManager == null) {
      memoryManager = MemoryManager.create(AllocationListener.NOOP);
      session = Velox4j.newSession(memoryManager);
      query =
          new Query(
              planNode, VeloxQueryConfig.getConfig(getRuntimeContext()), ConnectorConfig.empty());
      allocator = new RootAllocator(Long.MAX_VALUE);
      task = session.queryOps().execute(query);
      task.addSplit(id, split);
      task.noMoreSplits(id);
    }
    taskMetrics = new SourceTaskMetrics(getRuntimeContext().getMetricGroup());
  }

  @Override
  public void run(SourceContext<StatefulElement> sourceContext) throws Exception {
    while (isRunning) {
      UpIterator.State state = task.advance();
      if (state == UpIterator.State.AVAILABLE) {
        final StatefulElement element = task.statefulGet();
        if (element.isWatermark()) {
          sourceContext.emitWatermark(new Watermark(element.asWatermark().getTimestamp()));
        } else {
          sourceContext.collect(element);
        }
        element.close();
      } else if (state == UpIterator.State.BLOCKED) {
        LOG.debug("Get empty row");
      } else {
        LOG.info("Velox task finished");
        break;
      }
      taskMetrics.updateMetrics(task, id);
    }
    task.close();
    session.close();
    memoryManager.close();
    allocator.close();
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // TODO: implement it
    this.task.snapshotState(0);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    if (memoryManager == null) {
      LOG.debug("Running GlutenSourceFunction: " + Serde.toJson(planNode));
      memoryManager = MemoryManager.create(AllocationListener.NOOP);
      session = Velox4j.newSession(memoryManager);
      query =
          new Query(
              planNode, VeloxQueryConfig.getConfig(getRuntimeContext()), ConnectorConfig.empty());
      allocator = new RootAllocator(Long.MAX_VALUE);

      task = session.queryOps().execute(query);
      task.addSplit(id, split);
      task.noMoreSplits(id);
    }
  }

  public String[] notifyCheckpointComplete(long checkpointId) throws Exception {
    // TODO: notify velox
    return this.task.notifyCheckpointComplete(checkpointId);
  }

  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    this.task.notifyCheckpointAborted(checkpointId);
  }
}

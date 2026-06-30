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

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.table.runtime.config.VeloxConnectorConfig;
import org.apache.gluten.table.runtime.config.VeloxQueryConfig;
import org.apache.gluten.util.VectorInputBridge;
import org.apache.gluten.util.VectorOutputBridge;

import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.stateful.NativeCallbackTarget;
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.stateful.StatefulWatermark;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SetupableStreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Two input operator in gluten, which will call Velox to run. It receives RowVector from upstream
 * instead of flink RowData.
 *
 * <p>This class intentionally does not extend {@code AbstractStreamOperator}: in the Flink version
 * used here, {@code AbstractStreamOperator.processWatermarkStatus1/2(...)} are final and maintain
 * Flink's Java-side combined watermark status. Gluten needs to forward each input's status to
 * native, which is the source of truth for combined watermark status inside the Gluten operator
 * chain.
 */
public class GlutenTwoInputOperator<IN, OUT>
    implements TwoInputStreamOperator<IN, IN, OUT>,
        SetupableStreamOperator<OUT>,
        GlutenOperator,
        NativeCallbackTarget {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenTwoInputOperator.class);

  private final StatefulPlanNode glutenPlan;
  private final String leftId;
  private final String rightId;
  private final RowType leftInputType;
  private final RowType rightInputType;
  private final Map<String, RowType> outputTypes;
  private final RowType outputType;

  private GlutenSessionResource sessionResource;
  private Query query;
  private ExternalStreams.BlockingQueue leftInputQueue;
  private ExternalStreams.BlockingQueue rightInputQueue;
  private SerialTask task;
  private transient volatile boolean closing;
  private final Class<IN> inClass;
  private final Class<OUT> outClass;
  private VectorInputBridge<IN> inputBridge;
  private VectorOutputBridge<OUT> outputBridge;
  private String description;
  private final GlutenMailboxHolder mailboxHolder = new GlutenMailboxHolder();
  private transient StreamTask<?, ?> containingTask;
  private transient StreamConfig config;
  private transient Output<StreamRecord<OUT>> output;
  private transient StreamingRuntimeContext runtimeContext;
  private transient OperatorMetricGroup metricGroup;
  private transient ProcessingTimeService processingTimeService;
  private ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;
  private transient Object currentKey;

  public GlutenTwoInputOperator(
      StatefulPlanNode plan,
      String leftId,
      String rightId,
      RowType leftInputType,
      RowType rightInputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass,
      String description) {
    this.glutenPlan = plan;
    this.leftId = leftId;
    this.rightId = rightId;
    this.leftInputType = leftInputType;
    this.rightInputType = rightInputType;
    this.outputTypes = outputTypes;
    this.inClass = inClass;
    this.outClass = outClass;
    this.inputBridge = VectorInputBridge.Factory.create(inClass, getId());
    this.outputBridge = VectorOutputBridge.Factory.create(outClass);
    this.outputType = outputTypes.values().iterator().next();
    this.description = description;
  }

  public GlutenTwoInputOperator(
      StatefulPlanNode plan,
      String leftId,
      String rightId,
      RowType leftInputType,
      RowType rightInputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass) {
    this(plan, leftId, rightId, leftInputType, rightInputType, outputTypes, inClass, outClass, "");
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public void open() throws Exception {
    closing = false;
    if (!mailboxHolder().get().isMailboxBound()) {
      ensureMailboxInitialized(getContainingTask());
    }
    initSession();
  }

  @Override
  public void setup(
      StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
    this.containingTask = containingTask;
    this.config = config;
    this.output = output;
    this.metricGroup =
        containingTask
            .getEnvironment()
            .getMetricGroup()
            .getOrAddOperator(config.getOperatorID(), config.getOperatorName());
    this.runtimeContext =
        new StreamingRuntimeContext(
            containingTask.getEnvironment(),
            containingTask.getEnvironment().getAccumulatorRegistry().getUserMap(),
            metricGroup,
            config.getOperatorID(),
            processingTimeService,
            null,
            containingTask.getEnvironment().getExternalResourceInfoProvider());
  }

  @Override
  public ChainingStrategy getChainingStrategy() {
    return chainingStrategy;
  }

  @Override
  public void setChainingStrategy(ChainingStrategy chainingStrategy) {
    this.chainingStrategy = chainingStrategy;
  }

  public StreamTask<?, ?> getContainingTask() {
    return containingTask;
  }

  public StreamingRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public ExecutionConfig getExecutionConfig() {
    return containingTask.getEnvironment().getExecutionConfig();
  }

  @Override
  public String getId() {
    return glutenPlan.getId();
  }

  @Override
  public GlutenMailboxHolder mailboxHolder() {
    return mailboxHolder;
  }

  @Override
  public void scheduleProcessElementOnMailbox() {
    if (closing) {
      return;
    }
    scheduleDrainOnMailbox(this::drainTaskOutput);
  }

  @Override
  public void onProcessingTime(long timestamp) {
    if (closing) {
      return;
    }
    scheduleProcessElementOnMailbox();
  }

  @Override
  public void processElement1(StreamRecord<IN> element) {
    StatefulRecord statefulRecord =
        inputBridge.convertToStatefulRecord(
            element, sessionResource.getAllocator(), sessionResource.getSession(), leftInputType);
    leftInputQueue.put(statefulRecord.getRowVector());
    // Only the rowvectors generated by this operator should be closed here.
    if (getId().equals(statefulRecord.getNodeId())) {
      statefulRecord.close();
    }
    processElementInternal();
  }

  @Override
  public void processElement2(StreamRecord<IN> element) {
    StatefulRecord statefulRecord =
        inputBridge.convertToStatefulRecord(
            element, sessionResource.getAllocator(), sessionResource.getSession(), rightInputType);
    rightInputQueue.put(statefulRecord.getRowVector());
    // Only the rowvectors generated by this operator should be closed here.

    if (getId().equals(statefulRecord.getNodeId())) {
      statefulRecord.close();
    }
    processElementInternal();
  }

  @Override
  public void processElementInternal() {
    if (closing) {
      return;
    }
    drainOutput(this::drainTaskOutput);
  }

  private void drainTaskOutput() {
    if (closing) {
      return;
    }
    while (true) {
      if (closing) {
        return;
      }
      UpIterator.State state = task.advance();
      if (state == UpIterator.State.AVAILABLE) {
        final StatefulElement element = task.statefulGet();
        try {
          if (element.isWatermark()) {
            StatefulWatermark watermark = element.asWatermark();
            output.emitWatermark(new Watermark(watermark.getTimestamp()));
          } else if (element.isWatermarkStatus()) {
            output.emitWatermarkStatus(GlutenWatermarkStatuses.toFlinkWatermarkStatus(element));
          } else {
            outputBridge.collect(
                output, element.asRecord(), sessionResource.getAllocator(), outputType);
          }
        } finally {
          element.close();
        }
      } else {
        break;
      }
    }
  }

  public void processWatermark(Watermark mark) throws Exception {
    task.notifyWatermark(mark.getTimestamp());
    processElementInternal();
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {
    task.notifyWatermark(mark.getTimestamp(), 0);
    processElementInternal();
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    task.notifyWatermark(mark.getTimestamp(), 1);
    processElementInternal();
  }

  @Override
  public void processWatermarkStatus1(WatermarkStatus status) throws Exception {
    task.notifyWatermarkStatus(status.isIdle(), 0);
    processElementInternal();
  }

  @Override
  public void processWatermarkStatus2(WatermarkStatus status) throws Exception {
    task.notifyWatermarkStatus(status.isIdle(), 1);
    processElementInternal();
  }

  @Override
  public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
    output.emitLatencyMarker(latencyMarker);
  }

  @Override
  public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
    output.emitLatencyMarker(latencyMarker);
  }

  @Override
  public void close() throws Exception {
    closing = true;
    GlutenCloseables.runWithCleanup(
        () -> {
          if (leftInputQueue != null) {
            leftInputQueue.close();
          }
        },
        () -> {
          if (rightInputQueue != null) {
            rightInputQueue.close();
          }
        },
        () -> {
          if (task != null) {
            task.unbindNativeCallbackTarget();
          }
        },
        () -> {
          if (task != null) {
            task.close();
          }
        },
        () -> {
          GlutenTaskSessionContext.unregisterSessionResource(getId());
        },
        () -> {
          if (sessionResource != null) {
            sessionResource.close();
          }
        });
  }

  @Override
  public void finish() {}

  @Override
  public StatefulPlanNode getPlanNode() {
    return glutenPlan;
  }

  @Override
  public RowType getInputType() {
    throw new RuntimeException("Should not call getInputType on GlutenTwoInputOperator");
  }

  public RowType getLeftInputType() {
    return leftInputType;
  }

  public RowType getRightInputType() {
    return rightInputType;
  }

  @Override
  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  public String getLeftId() {
    return leftId;
  }

  public String getRightId() {
    return rightId;
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    // TODO: notify velox
  }

  @Override
  public OperatorSnapshotFutures snapshotState(
      long checkpointId,
      long timestamp,
      CheckpointOptions checkpointOptions,
      CheckpointStreamFactory storageLocation)
      throws Exception {
    if (task != null) {
      task.snapshotState(checkpointId);
    }
    return new OperatorSnapshotFutures();
  }

  @Override
  public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
    initSession();
    task.initializeState(0, null);
  }

  public void snapshotState(StateSnapshotContext context) throws Exception {
    // TODO: implement it
    task.snapshotState(0);
  }

  public void initializeState(StateInitializationContext context) throws Exception {
    initSession();
    // TODO: implement it
    task.initializeState(0, null);
  }

  private void initSession() {
    if (sessionResource != null) {
      return;
    }

    sessionResource = new GlutenSessionResource();
    GlutenTaskSessionContext.addSessionResource(getId(), sessionResource);
    leftInputQueue = sessionResource.getSession().externalStreamOps().newBlockingQueue();
    rightInputQueue = sessionResource.getSession().externalStreamOps().newBlockingQueue();

    query =
        new Query(
            glutenPlan,
            VeloxQueryConfig.getConfig(getRuntimeContext()),
            VeloxConnectorConfig.getConfig(getRuntimeContext()));
    task = sessionResource.getSession().queryOps().execute(query);
    task.bindNativeCallbackTarget(this);

    ExternalStreamConnectorSplit leftSplit =
        new ExternalStreamConnectorSplit("connector-external-stream", leftInputQueue.id());
    ExternalStreamConnectorSplit rightSplit =
        new ExternalStreamConnectorSplit("connector-external-stream", rightInputQueue.id());
    task.addSplit(leftId, leftSplit);
    task.noMoreSplits(leftId);
    task.addSplit(rightId, rightSplit);
    task.noMoreSplits(rightId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // TODO: notify velox
    task.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    task.notifyCheckpointAborted(checkpointId);
  }

  @Override
  public void setKeyContextElement1(StreamRecord<?> record) throws Exception {}

  @Override
  public void setKeyContextElement2(StreamRecord<?> record) throws Exception {}

  @Override
  public OperatorMetricGroup getMetricGroup() {
    return metricGroup;
  }

  @Override
  public OperatorID getOperatorID() {
    return config.getOperatorID();
  }

  @Override
  public void setCurrentKey(Object key) {
    currentKey = key;
  }

  @Override
  public Object getCurrentKey() {
    return currentKey;
  }

  public KeyedStateStore getKeyedStateStore() {
    return null;
  }
}

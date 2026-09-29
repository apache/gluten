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

import org.apache.gluten.streaming.api.operators.GlutenAbstractStreamOperator;
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

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
public class GlutenTwoInputOperator<IN, OUT> extends GlutenAbstractStreamOperator<OUT>
    implements TwoInputStreamOperator<IN, IN, OUT>, GlutenOperator, NativeCallbackTarget {

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
  private transient boolean stateInitialized;

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
    super.open();
    if (!mailboxHolder().get().isMailboxBound()) {
      ensureMailboxInitialized(getContainingTask());
    }
    initSession();
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

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (task == null) {
      throw new NullPointerException(
          "task is null in processWatermark; expected to be initialized in open() and "
              + "cleared only after close()");
    }
    task.notifyWatermark(mark.getTimestamp());
    processElementInternal();
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {
    if (task == null) {
      throw new NullPointerException(
          "task is null in processWatermark1; expected to be initialized in open() and "
              + "cleared only after close()");
    }
    task.notifyWatermark(mark.getTimestamp(), 0);
    processElementInternal();
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    if (task == null) {
      throw new NullPointerException(
          "task is null in processWatermark2; expected to be initialized in open() and "
              + "cleared only after close()");
    }
    task.notifyWatermark(mark.getTimestamp(), 1);
    processElementInternal();
  }

  @Override
  public void processWatermarkStatus1(WatermarkStatus status) throws Exception {
    if (task == null) {
      throw new NullPointerException(
          "task is null in processWatermarkStatus1; expected to be initialized in open() and "
              + "cleared only after close()");
    }
    task.notifyWatermarkStatus(status.isIdle(), 0);
    processElementInternal();
  }

  @Override
  public void processWatermarkStatus2(WatermarkStatus status) throws Exception {
    if (task == null) {
      throw new NullPointerException(
          "task is null in processWatermarkStatus2; expected to be initialized in open() and "
              + "cleared only after close()");
    }
    task.notifyWatermarkStatus(status.isIdle(), 1);
    processElementInternal();
  }

  @Override
  public void close() throws Exception {
    closing = true;
    GlutenCloseables.runWithCleanup(
        () -> {
          if (leftInputQueue != null) {
            leftInputQueue.noMoreInput();
          }
        },
        () -> {
          if (rightInputQueue != null) {
            rightInputQueue.noMoreInput();
          }
        },
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
        },
        super::close);
  }

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
    processElementInternal();
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // TODO: implement it
    snapshotNativeState(context.getCheckpointId());
    super.snapshotState(context);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    // TODO: implement it
    initializeNativeState();
    super.initializeState(context);
  }

  private void snapshotNativeState(long checkpointId) {
    if (task != null) {
      task.snapshotState(checkpointId);
    }
  }

  private void initializeNativeState() {
    if (stateInitialized) {
      return;
    }
    initSession();
    task.initializeState(0, null);
    stateInitialized = true;
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
    if (task != null) {
      task.notifyCheckpointComplete(checkpointId);
    }
    super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    if (task != null) {
      task.notifyCheckpointAborted(checkpointId);
    }
    super.notifyCheckpointAborted(checkpointId);
  }
}

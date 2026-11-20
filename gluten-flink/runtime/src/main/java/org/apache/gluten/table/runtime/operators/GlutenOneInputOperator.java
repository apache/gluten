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
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.stateful.KeyedStateBackendParameters;
import io.github.zhztheplayer.velox4j.stateful.NativeCallbackTarget;
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.stateful.StatefulWatermark;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenOneInputOperator<IN, OUT> extends TableStreamOperator<OUT>
    implements OneInputStreamOperator<IN, OUT>, GlutenOperator, NativeCallbackTarget {

  private final StatefulPlanNode glutenPlan;
  private final String id;
  private final RowType inputType;
  private final Map<String, RowType> outputTypes;
  private final RowType outputType;
  private final String description;

  private transient GlutenSessionResource sessionResource;
  private transient Query query;
  private transient ExternalStreams.BlockingQueue inputQueue;
  protected transient SerialTask task;
  // Native checkpoint records handed back by the last native snapshot; the
  // store of the restore path (design D4 of the rank task). The records are
  // raw checkpoint bytes, not text: a String channel would corrupt them
  // (JNI strings are modified UTF-8), so the store holds byte arrays.
  private transient ListState<byte[]> checkpointState;
  // Native checkpoint records of the last restore, handed to the native
  // state initialization (rank task design D4). Protected so subclasses
  // that own their native records (GlutenStreamingFileWriterOperator)
  // keep full control of their own restore path.
  protected transient byte[][] restoredNativeCheckpointRecords = new byte[0][];
  private transient volatile boolean closing;
  private final Class<IN> inClass;
  private final Class<OUT> outClass;
  private transient VectorInputBridge<IN> inputBridge;
  private transient VectorOutputBridge<OUT> outputBridge;
  private final GlutenMailboxHolder mailboxHolder = new GlutenMailboxHolder();

  public GlutenOneInputOperator(
      StatefulPlanNode plan,
      String id,
      RowType inputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass,
      String description) {
    if (plan == null) {
      throw new IllegalArgumentException(
          "StatefulPlanNode 'plan' cannot be null in GlutenOneInputOperator constructor");
    }
    this.glutenPlan = plan;
    this.id = id;
    this.inputType = inputType;
    this.outputTypes = outputTypes;
    this.inClass = inClass;
    this.outClass = outClass;
    this.inputBridge = VectorInputBridge.Factory.create(inClass, getId());
    this.outputBridge = VectorOutputBridge.Factory.create(outClass);
    this.outputType = outputTypes.values().iterator().next();
    this.description = description;
  }

  public GlutenOneInputOperator(
      StatefulPlanNode plan,
      String id,
      RowType inputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass) {
    this(plan, id, inputType, outputTypes, inClass, outClass, "");
  }

  @Override
  public String getDescription() {
    return description;
  }

  void initSession() {
    if (sessionResource != null) {
      return;
    }
    if (inputBridge == null) {
      inputBridge = VectorInputBridge.Factory.create(inClass, getId());
    }
    if (outputBridge == null) {
      outputBridge = VectorOutputBridge.Factory.create(outClass);
    }
    sessionResource = new GlutenSessionResource();
    GlutenTaskSessionContext.addSessionResource(id, sessionResource);
    inputQueue = sessionResource.getSession().externalStreamOps().newBlockingQueue();
    // add a mock input as velox not allow the source is empty.
    if (inputType == null) {
      LOG.warn("inputType is null. plan is " + Serde.toJson(glutenPlan));
      throw new IllegalArgumentException("inputType cannot be null");
    }
    StatefulPlanNode mockInput =
        new StatefulPlanNode(
            id,
            new TableScanNode(
                id,
                inputType,
                new ExternalStreamTableHandle("connector-external-stream"),
                List.of()));
    mockInput.addTarget(glutenPlan);
    LOG.debug("Gluten Plan: {}", Serde.toJson(mockInput));
    LOG.debug("OutTypes: {}", outputTypes.keySet());
    query =
        new Query(
            mockInput,
            VeloxQueryConfig.getConfig(getRuntimeContext()),
            VeloxConnectorConfig.getConfig(getRuntimeContext()));
    task = sessionResource.getSession().queryOps().execute(query);
    task.bindNativeCallbackTarget(this);
    task.addSplit(
        id, new ExternalStreamConnectorSplit("connector-external-stream", inputQueue.id()));
    task.noMoreSplits(id);
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
  public void processElement(StreamRecord<IN> element) {
    if (element.getValue() == null) {
      return;
    }
    StatefulRecord statefulRecord =
        inputBridge.convertToStatefulRecord(
            element, sessionResource.getAllocator(), sessionResource.getSession(), inputType);
    try {
      inputQueue.put(statefulRecord.getRowVector());
    } finally {
      // Only the rowvectors generated by this operator should be closed here.
      if (getId().equals(statefulRecord.getNodeId())) {
        statefulRecord.close();
      }
    }
    processElementInternal();
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
        final StatefulElement statefulElement = task.statefulGet();
        try {
          if (statefulElement.isWatermark()) {
            StatefulWatermark watermark = statefulElement.asWatermark();
            output.emitWatermark(new Watermark(watermark.getTimestamp()));
          } else if (statefulElement.isWatermarkStatus()) {
            output.emitWatermarkStatus(
                GlutenWatermarkStatuses.toFlinkWatermarkStatus(statefulElement));
          } else {
            outputBridge.collect(
                output, statefulElement.asRecord(), sessionResource.getAllocator(), outputType);
          }
        } finally {
          statefulElement.close();
        }
      } else {
        break;
      }
    }
  }

  public <NIN, NOUT> GlutenOneInputOperator<NIN, NOUT> cloneWithInputOutputClasses(
      StatefulPlanNode plan, Class<NIN> newInClass, Class<NOUT> newOutClass) {
    return new GlutenOneInputOperator<>(
        plan, this.id, this.inputType, this.outputTypes, newInClass, newOutClass, this.description);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    task.notifyWatermark(mark.getTimestamp());
    // Process any pending elements to ensure watermark-triggered operations complete.
    processElementInternal();
  }

  @Override
  public void processWatermarkStatus(WatermarkStatus status) throws Exception {
    task.notifyWatermarkStatus(status.isIdle());
    processElementInternal();
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {
    throw new UnsupportedOperationException("Not implemented for GlutenOneInputOperator");
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    throw new UnsupportedOperationException("Not implemented for GlutenOneInputOperator");
  }

  @Override
  public void close() throws Exception {
    closing = true;
    GlutenCloseables.runWithCleanup(
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
          if (inputQueue != null) {
            inputQueue.noMoreInput();
          }
        },
        () -> {
          if (inputQueue != null) {
            inputQueue.close();
          }
        },
        () -> {
          GlutenTaskSessionContext.unregisterSessionResource(id);
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
    return inputType;
  }

  @Override
  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    snapshotNativeState(context);
    super.snapshotState(context);
  }

  protected void snapshotNativeState(StateSnapshotContext context) throws Exception {
    // The native snapshot returns the checkpoint records of the stateful
    // operators (typed keyed backends); persist them in the operator state
    // so a restore can feed them back (design D4 of the rank task). Empty
    // for operators without native state, so the path is transparent to
    // every other user of this class.
    byte[][] checkpointRecords = task.snapshotState(context.getCheckpointId());
    checkpointState.clear();
    for (byte[] checkpointRecord : checkpointRecords) {
      checkpointState.add(checkpointRecord);
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    // Declare the checkpoint-record store and collect the restored records
    // before the native state initialization consumes them (the
    // GlutenSourceFunction pattern). Union mode: the native backend's
    // snapshot is key-group sharded by design, and every subtask must
    // receive every writer's shard at restore - it picks its own key
    // groups out of each shard and skips the rest, which restores correctly
    // at any rescale. This operator-state home is an interim until the
    // native backend is backed by Flink's keyed-state SPI, where Flink
    // owns the key-group sharding and rescaling itself.
    checkpointState =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>("gluten-native-operator-checkpoint", byte[].class));
    if (context.isRestored()) {
      List<byte[]> records = new ArrayList<>();
      for (byte[] checkpointRecord : checkpointState.get()) {
        records.add(checkpointRecord);
      }
      restoredNativeCheckpointRecords = records.toArray(new byte[0][]);
    } else {
      restoredNativeCheckpointRecords = new byte[0][];
    }
    initializeNativeState(context);
    super.initializeState(context);
  }

  protected void initializeNativeState(StateInitializationContext context) throws Exception {
    if (task == null) {
      initSession();
    }
    if (!(getKeyedStateBackend() instanceof RocksDBKeyedStateBackend)) {
      task.initializeState(0, nativeKeyedStateBackendParameters(), restoredNativeCheckpointRecords);
    }
  }

  /** StateBackendType::HEAP of the native StatefulTask (StateBackend.h). */
  private static final int NATIVE_STATE_BACKEND_TYPE_HEAP = 0;

  /**
   * The key-group parameters of the native keyed state backend (rank2 J3): the backend owns exactly
   * the key groups this subtask holds under the native key-group assignment, so a restore shards
   * correctly at any parallelism.
   *
   * <p>The GFV transformation stays non-keyed on the Flink side (implementation contract), so Flink
   * tracks no key-group space for it - {@code getMaxNumberOfParallelSubtasks()} reports the
   * parallelism, not a key-group size. The native key-group space resolves the same way the
   * plan-side partition spec does (StreamExecExchange): the job-wide value from the ExecutionConfig
   * if set, else the scheduler default derived from the parallelism - so routing and state
   * ownership always agree (completes the rank2 L13 TODO).
   */
  private KeyedStateBackendParameters nativeKeyedStateBackendParameters() {
    final int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    int maxParallelism = getRuntimeContext().getExecutionConfig().getMaxParallelism();
    if (maxParallelism <= 0) {
      maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
    }
    final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    final KeyGroupRange keyGroupRange =
        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism, parallelism, subtaskIndex);
    return new KeyedStateBackendParameters(
        getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks(),
        id,
        NATIVE_STATE_BACKEND_TYPE_HEAP,
        maxParallelism,
        keyGroupRange.getStartKeyGroup(),
        keyGroupRange.getNumberOfKeyGroups());
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    if (!(this instanceof GlutenStreamingFileWriterOperator)) {
      task.notifyCheckpointComplete(checkpointId);
    }
    super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    task.notifyCheckpointAborted(checkpointId);
    super.notifyCheckpointAborted(checkpointId);
  }
}

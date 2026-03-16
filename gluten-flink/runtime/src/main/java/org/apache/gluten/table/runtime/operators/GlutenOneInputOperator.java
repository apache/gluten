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
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.stateful.StatefulWatermark;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenOneInputOperator<IN, OUT> extends TableStreamOperator<OUT>
    implements OneInputStreamOperator<IN, OUT>, GlutenOperator {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenOneInputOperator.class);

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
  private final Class<IN> inClass;
  private final Class<OUT> outClass;
  private transient VectorInputBridge<IN> inputBridge;
  private transient VectorOutputBridge<OUT> outputBridge;

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
    task.addSplit(
        id, new ExternalStreamConnectorSplit("connector-external-stream", inputQueue.id()));
    task.noMoreSplits(id);
  }

  @Override
  public void open() throws Exception {
    super.open();
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
    inputQueue.put(statefulRecord.getRowVector());

    // Only the rowvectors generated by this operator should be closed here.
    if (getId().equals(statefulRecord.getNodeId())) {
      statefulRecord.close();
    }
    processElementInternal();
  }

  private void processElementInternal() {
    while (true) {
      UpIterator.State state = task.advance();
      if (state == UpIterator.State.AVAILABLE) {
        final StatefulElement statefulElement = task.statefulGet();
        if (statefulElement.isWatermark()) {
          StatefulWatermark watermark = statefulElement.asWatermark();
          output.emitWatermark(new Watermark(watermark.getTimestamp()));
        } else {
          outputBridge.collect(
              output, statefulElement.asRecord(), sessionResource.getAllocator(), outputType);
        }
        statefulElement.close();
      } else {
        break;
      }
    }
  }

  public <NIN, NOUT> GlutenOneInputOperator<NIN, NOUT> cloneWithInputOutputClasses(
      Class<NIN> newInClass, Class<NOUT> newOutClass) {
    return new GlutenOneInputOperator<>(
        this.glutenPlan,
        this.id,
        this.inputType,
        this.outputTypes,
        newInClass,
        newOutClass,
        this.description);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    task.notifyWatermark(mark.getTimestamp());
    // Process any pending elements to ensure watermark-triggered operations complete.
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
    if (inputQueue != null) {
      inputQueue.noMoreInput();
      inputQueue.close();
    }
    if (task != null) {
      task.close();
    }
    if (sessionResource != null) {
      sessionResource.close();
    }
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
    // TODO: notify velox
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // TODO: implement it
    task.snapshotState(0);
    super.snapshotState(context);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    if (task == null) {
      initSession();
    }
    task.initializeState(0, null);
    super.initializeState(context);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // TODO: notify velox
    task.notifyCheckpointComplete(checkpointId);
    super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    task.notifyCheckpointAborted(checkpointId);
    super.notifyCheckpointAborted(checkpointId);
  }
}

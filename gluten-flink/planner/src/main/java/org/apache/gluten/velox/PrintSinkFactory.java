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
package org.apache.gluten.velox;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.connector.CommitStrategy;
import io.github.zhztheplayer.velox4j.connector.PrintTableHandle;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableWriteNode;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.List;
import java.util.Map;

public class PrintSinkFactory implements VeloxSourceSinkFactory {

  @SuppressWarnings("rawtypes")
  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (transformation instanceof LegacySinkTransformation) {
      SimpleOperatorFactory operatorFactory =
          (SimpleOperatorFactory) ((LegacySinkTransformation) transformation).getOperatorFactory();
      OneInputStreamOperator sinkOp = (OneInputStreamOperator) operatorFactory.getOperator();
      if (sinkOp instanceof SinkOperator
          && ((SinkOperator) sinkOp)
              .getUserFunction()
              .getClass()
              .getSimpleName()
              .equals("RowDataPrintFunction")) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    throw new FlinkRuntimeException("Unimplemented method 'buildSource'");
  }

  // Pulls print-identifier/standard-error from RowDataPrintFunction via reflection.
  // Flink 1.19.x field names: sinkIdentifier (print-identifier), target (standard-error, true =
  // stderr).
  static PrintOptions extractPrintOptions(Transformation<RowData> transformation) {
    SimpleOperatorFactory operatorFactory =
        (SimpleOperatorFactory) ((LegacySinkTransformation) transformation).getOperatorFactory();
    SinkOperator sinkOp = (SinkOperator) operatorFactory.getOperator();
    Object rowDataPrintFn = sinkOp.getUserFunction();
    Object writer =
        ReflectUtils.getObjectField(rowDataPrintFn.getClass(), rowDataPrintFn, "writer");
    String printIdentifier =
        (String) ReflectUtils.getObjectField(writer.getClass(), writer, "sinkIdentifier");
    boolean isStdErr = (boolean) ReflectUtils.getObjectField(writer.getClass(), writer, "target");
    return new PrintOptions(printIdentifier == null ? "" : printIdentifier, isStdErr);
  }

  static final class PrintOptions {
    private final String printIdentifier;
    private final boolean stdErr;

    PrintOptions(String printIdentifier, boolean stdErr) {
      this.printIdentifier = printIdentifier;
      this.stdErr = stdErr;
    }

    public String getPrintIdentifier() {
      return printIdentifier;
    }

    public boolean isStdErr() {
      return stdErr;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Transformation buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    Transformation inputTrans = (Transformation) transformation.getInputs().get(0);
    // PrintSink reads rowtime directly from RowData via SinkOperator.timestamp() and never
    // inspects StreamRecord.timestamp, so a native StreamRecordTimestampInserter on the input
    // chain (if any) is dead weight - remove it.
    inputTrans = GlutenRowtimeInserterHelper.processTransformation(inputTrans, false);
    InternalTypeInfo inputTypeInfo = (InternalTypeInfo) inputTrans.getOutputType();

    PrintOptions printOpts = extractPrintOptions(transformation);

    RowType inputColumns = (RowType) LogicalTypeConverter.toVLType(inputTypeInfo.toLogicalType());
    RowType ignore = new RowType(List.of("num"), List.of(new BigIntType()));
    PrintTableHandle tableHandle =
        new PrintTableHandle(
            "print-table", inputColumns, printOpts.getPrintIdentifier(), printOpts.isStdErr());
    TableWriteNode tableWriteNode =
        new TableWriteNode(
            PlanNodeIdGenerator.newId(),
            inputColumns,
            inputColumns.getNames(),
            null,
            "connector-print",
            tableHandle,
            false,
            ignore,
            CommitStrategy.NO_COMMIT,
            List.of(new EmptyNode(inputColumns)));
    return new LegacySinkTransformation(
        inputTrans,
        transformation.getName(),
        new GlutenOneInputOperatorFactory(
            new GlutenOneInputOperator(
                new StatefulPlanNode(tableWriteNode.getId(), tableWriteNode),
                PlanNodeIdGenerator.newId(),
                inputColumns,
                Map.of(tableWriteNode.getId(), ignore),
                RowData.class,
                RowData.class,
                "PrintSink")),
        transformation.getParallelism());
  }
}

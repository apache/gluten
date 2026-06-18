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

import java.lang.reflect.Field;
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
  // Package-private for direct unit testing.
  static String[] extractPrintOptions(Transformation<RowData> transformation) {
    SimpleOperatorFactory operatorFactory =
        (SimpleOperatorFactory) ((LegacySinkTransformation) transformation).getOperatorFactory();
    SinkOperator sinkOp = (SinkOperator) operatorFactory.getOperator();
    Object rowDataPrintFn = sinkOp.getUserFunction();
    try {
      Field writerField = rowDataPrintFn.getClass().getDeclaredField("writer");
      writerField.setAccessible(true);
      Object writer = writerField.get(rowDataPrintFn);
      Field idField = writer.getClass().getDeclaredField("sinkIdentifier");
      idField.setAccessible(true);
      Field stdErrField = writer.getClass().getDeclaredField("target");
      stdErrField.setAccessible(true);
      String printIdentifier = (String) idField.get(writer);
      boolean isStdErr = stdErrField.getBoolean(writer);
      return new String[] {
        printIdentifier == null ? "" : printIdentifier, Boolean.toString(isStdErr)
      };
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new FlinkRuntimeException("Failed to extract print sink options", e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Transformation buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    Transformation inputTrans = (Transformation) transformation.getInputs().get(0);
    InternalTypeInfo inputTypeInfo = (InternalTypeInfo) inputTrans.getOutputType();

    String[] printOpts = extractPrintOptions(transformation);
    String printIdentifier = printOpts[0];
    boolean isStdErr = Boolean.parseBoolean(printOpts[1]);

    RowType inputColumns = (RowType) LogicalTypeConverter.toVLType(inputTypeInfo.toLogicalType());
    RowType ignore = new RowType(List.of("num"), List.of(new BigIntType()));
    PrintTableHandle tableHandle =
        new PrintTableHandle("print-table", inputColumns, printIdentifier, isStdErr);
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

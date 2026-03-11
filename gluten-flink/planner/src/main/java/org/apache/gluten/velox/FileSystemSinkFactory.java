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
import io.github.zhztheplayer.velox4j.connector.FileSystemInsertTableHandle;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableWriteNode;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileSystemSinkFactory implements VeloxSourceSinkFactory {

  @SuppressWarnings("unchecked")
  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (transformation instanceof SinkTransformation) {
      SinkTransformation<RowData, RowData> sinkTransformation =
          (SinkTransformation<RowData, RowData>) transformation;
      Transformation<RowData> inputTransformation =
          (Transformation<RowData>) sinkTransformation.getInputs().get(0);
      if (inputTransformation instanceof OneInputTransformation
          && inputTransformation.getName().equals("PartitionCommitter")) {
        OneInputTransformation<RowData, RowData> oneInputTransformatin =
            (OneInputTransformation<RowData, RowData>) inputTransformation;
        Transformation<RowData> preInputTransformation =
            (Transformation<RowData>) oneInputTransformatin.getInputs().get(0);
        return preInputTransformation.getName().equals("StreamingFileWriter");
      }
    }
    return false;
  }

  @Override
  public Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    throw new UnsupportedOperationException("Unimplemented method 'buildVeloxSource'");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Transformation<RowData> buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    SinkTransformation<RowData, RowData> sinkTransformation =
        (SinkTransformation<RowData, RowData>) transformation;
    OneInputTransformation<RowData, RowData> partitionCommitTransformation =
        (OneInputTransformation<RowData, RowData>) sinkTransformation.getInputs().get(0);
    OneInputTransformation<RowData, RowData> fileWriterTransformation =
        (OneInputTransformation<RowData, RowData>) partitionCommitTransformation.getInputs().get(0);
    OneInputStreamOperator<?, ?> operator = fileWriterTransformation.getOperator();
    List<String> partitionKeys =
        (List<String>) ReflectUtils.getObjectField(operator.getClass(), operator, "partitionKeys");
    Map<String, String> tableParams = new HashMap<>();
    Configuration tableOptions =
        (Configuration)
            ReflectUtils.getObjectField(
                "org.apache.flink.connector.file.table.stream.PartitionCommitter",
                partitionCommitTransformation.getOperator(),
                "conf");
    tableParams.putAll(tableOptions.toMap());

    ResolvedSchema schema = (ResolvedSchema) parameters.get(ResolvedSchema.class.getName());
    List<String> columnList = schema.getColumnNames();
    List<Integer> partitionIndexes =
        partitionKeys.stream().mapToInt(columnList::indexOf).boxed().collect(Collectors.toList());
    org.apache.flink.table.types.logical.RowType inputType =
        (org.apache.flink.table.types.logical.RowType)
            ((InternalTypeInfo<?>) fileWriterTransformation.getInputType()).toLogicalType();
    RowType inputDataColumns = (RowType) LogicalTypeConverter.toVLType(inputType);
    FileSystemInsertTableHandle insertTableHandle =
        new FileSystemInsertTableHandle(
            fileWriterTransformation.getName(),
            inputDataColumns,
            partitionKeys,
            partitionIndexes,
            tableParams);
    RowType ignore = new RowType(List.of("num"), List.of(new BigIntType()));
    TableWriteNode fileSystemWriteNode =
        new TableWriteNode(
            PlanNodeIdGenerator.newId(),
            inputDataColumns,
            inputDataColumns.getNames(),
            null,
            "connector-filesystem",
            insertTableHandle,
            false,
            ignore,
            CommitStrategy.NO_COMMIT,
            List.of(new EmptyNode(inputDataColumns)));
    GlutenOneInputOperator onewInputOperator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(fileSystemWriteNode.getId(), fileSystemWriteNode),
            PlanNodeIdGenerator.newId(),
            inputDataColumns,
            Map.of(fileSystemWriteNode.getId(), ignore),
            RowData.class,
            RowData.class,
            "FileSystemInsertTable");
    GlutenOneInputOperatorFactory<?, ?> operatorFactory =
        new GlutenOneInputOperatorFactory(onewInputOperator);
    Transformation<RowData> veloxFileWriterTransformation =
        new OneInputTransformation(
            fileWriterTransformation.getInputs().get(0),
            fileWriterTransformation.getName(),
            operatorFactory,
            fileWriterTransformation.getOutputType(),
            fileWriterTransformation.getParallelism());
    OneInputTransformation<RowData, RowData> newPartitionCommitTransformation =
        new OneInputTransformation(
            veloxFileWriterTransformation,
            partitionCommitTransformation.getName(),
            partitionCommitTransformation.getOperatorFactory(),
            partitionCommitTransformation.getOutputType(),
            partitionCommitTransformation.getParallelism());
    DataStream<RowData> newInputStream =
        new DataStream<RowData>(
            sinkTransformation.getInputStream().getExecutionEnvironment(),
            newPartitionCommitTransformation);
    return new SinkTransformation<RowData, RowData>(
        newInputStream,
        sinkTransformation.getSink(),
        sinkTransformation.getOutputType(),
        sinkTransformation.getName(),
        sinkTransformation.getParallelism(),
        sinkTransformation.isParallelismConfigured(),
        sinkTransformation.getSinkOperatorsUidHashes());
  }
}

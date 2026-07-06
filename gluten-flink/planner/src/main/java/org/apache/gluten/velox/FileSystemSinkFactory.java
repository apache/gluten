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
import org.apache.gluten.table.runtime.operators.GlutenStreamingFileWriterOperator;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileSystemSinkFactory implements VeloxSourceSinkFactory {

  protected static final String PARTITION_COMMITTER_CLASS =
      "org.apache.flink.connector.file.table.stream.PartitionCommitter";
  protected static final String ABSTRACT_STREAMING_WRITER_CLASS =
      "org.apache.flink.connector.file.table.stream.AbstractStreamingWriter";
  private static final String HIVE_TABLE_META_STORE_FACTORY_CLASS =
      "org.apache.flink.connectors.hive.HiveTableMetaStoreFactory";
  private static final String CONNECTOR_FILESYSTEM = "connector-filesystem";

  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (!isFileSystemSinkTransformation(transformation)) {
      return false;
    }
    return !isHiveConnector(transformation);
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
        getPartitionCommitTransformation(transformation);
    OneInputTransformation<RowData, RowData> fileWriterTransformation =
        getFileWriterTransformation(transformation);
    OneInputStreamOperator<?, ?> operator = fileWriterTransformation.getOperator();
    List<String> partitionKeys =
        (List<String>) ReflectUtils.getObjectField(operator.getClass(), operator, "partitionKeys");
    Object partitionCommitter =
        partitionCommitTransformation == null ? null : partitionCommitTransformation.getOperator();
    Map<String, String> tableParams = buildTableParams(partitionCommitter, operator);
    ResolvedSchema schema = (ResolvedSchema) parameters.get(ResolvedSchema.class.getName());
    List<String> columnList = schema.getColumnNames();
    List<Integer> partitionIndexes =
        partitionKeys.stream().mapToInt(columnList::indexOf).boxed().collect(Collectors.toList());
    org.apache.flink.table.types.logical.RowType inputType =
        (org.apache.flink.table.types.logical.RowType)
            schema.toPhysicalRowDataType().getLogicalType();
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
            CONNECTOR_FILESYSTEM,
            insertTableHandle,
            false,
            ignore,
            CommitStrategy.NO_COMMIT,
            List.of(new EmptyNode(inputDataColumns)));
    GlutenOneInputOperator onewInputOperator =
        new GlutenStreamingFileWriterOperator(
            new StatefulPlanNode(fileSystemWriteNode.getId(), fileSystemWriteNode),
            PlanNodeIdGenerator.newId(),
            inputDataColumns,
            Map.of(fileSystemWriteNode.getId(), ignore),
            RowData.class,
            getSinkDescription());
    GlutenOneInputOperatorFactory<RowData, ?> operatorFactory =
        new GlutenOneInputOperatorFactory<>(onewInputOperator);
    // StreamingFileWriter is fully offloaded to the velox file writer, which never consults
    // StreamRecord.timestamp for partition / roll / commit. Remove any native
    // StreamRecordTimestampInserter from the input chain.
    Transformation<RowData> veloxFileWriterInput =
        GlutenRowtimeInserterHelper.processTransformation(
            (Transformation<RowData>) fileWriterTransformation.getInputs().get(0), false);
    OneInputTransformation<RowData, ?> veloxFileWriterTransformation =
        new OneInputTransformation(
            veloxFileWriterInput,
            fileWriterTransformation.getName(),
            operatorFactory,
            fileWriterTransformation.getOutputType(),
            fileWriterTransformation.getParallelism());
    Transformation<?> newSinkInputTransformation;
    if (partitionCommitTransformation == null) {
      newSinkInputTransformation = veloxFileWriterTransformation;
    } else {
      newSinkInputTransformation =
          new OneInputTransformation(
              veloxFileWriterTransformation,
              partitionCommitTransformation.getName(),
              partitionCommitTransformation.getOperatorFactory(),
              partitionCommitTransformation.getOutputType(),
              partitionCommitTransformation.getParallelism());
    }
    DataStream<RowData> newInputStream =
        new DataStream<RowData>(
            sinkTransformation.getInputStream().getExecutionEnvironment(),
            (Transformation<RowData>) newSinkInputTransformation);
    return new SinkTransformation<RowData, RowData>(
        newInputStream,
        sinkTransformation.getSink(),
        sinkTransformation.getOutputType(),
        sinkTransformation.getName(),
        sinkTransformation.getParallelism(),
        sinkTransformation.isParallelismConfigured(),
        sinkTransformation.getSinkOperatorsUidHashes());
  }

  @SuppressWarnings("unchecked")
  protected boolean isFileSystemSinkTransformation(Transformation<RowData> transformation) {
    if (transformation instanceof SinkTransformation) {
      SinkTransformation<RowData, RowData> sinkTransformation =
          (SinkTransformation<RowData, RowData>) transformation;
      Transformation<RowData> inputTransformation =
          (Transformation<RowData>) sinkTransformation.getInputs().get(0);
      if (inputTransformation instanceof OneInputTransformation) {
        if (inputTransformation.getName().equals("StreamingFileWriter")) {
          return true;
        }
        if (inputTransformation.getName().equals("PartitionCommitter")) {
          OneInputTransformation<RowData, RowData> oneInputTransformation =
              (OneInputTransformation<RowData, RowData>) inputTransformation;
          Transformation<RowData> preInputTransformation =
              (Transformation<RowData>) oneInputTransformation.getInputs().get(0);
          return preInputTransformation.getName().equals("StreamingFileWriter");
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  protected OneInputTransformation<RowData, RowData> getPartitionCommitTransformation(
      Transformation<RowData> transformation) {
    SinkTransformation<RowData, RowData> sinkTransformation =
        (SinkTransformation<RowData, RowData>) transformation;
    Transformation<RowData> inputTransformation =
        (Transformation<RowData>) sinkTransformation.getInputs().get(0);
    if (inputTransformation.getName().equals("PartitionCommitter")) {
      return (OneInputTransformation<RowData, RowData>) inputTransformation;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  protected OneInputTransformation<RowData, RowData> getFileWriterTransformation(
      Transformation<RowData> transformation) {
    OneInputTransformation<RowData, RowData> partitionCommitTransformation =
        getPartitionCommitTransformation(transformation);
    if (partitionCommitTransformation != null) {
      return (OneInputTransformation<RowData, RowData>)
          partitionCommitTransformation.getInputs().get(0);
    }
    SinkTransformation<RowData, RowData> sinkTransformation =
        (SinkTransformation<RowData, RowData>) transformation;
    return (OneInputTransformation<RowData, RowData>) sinkTransformation.getInputs().get(0);
  }

  protected Object getPartitionCommitter(Transformation<RowData> transformation) {
    OneInputTransformation<RowData, RowData> partitionCommitTransformation =
        getPartitionCommitTransformation(transformation);
    return partitionCommitTransformation == null
        ? null
        : partitionCommitTransformation.getOperator();
  }

  protected boolean isHiveConnector(Transformation<RowData> transformation) {
    Object partitionCommitter = getPartitionCommitter(transformation);
    if (partitionCommitter != null) {
      return isHiveConnector(partitionCommitter);
    }
    return "hive"
        .equals(
            getTableOptions(getFileWriterTransformation(transformation).getOperator())
                .toMap()
                .get("connector"));
  }

  protected boolean isHiveConnector(Object partitionCommitter) {
    Object metaStoreFactory =
        ReflectUtils.getObjectField(
            PARTITION_COMMITTER_CLASS, partitionCommitter, "metaStoreFactory");
    return HIVE_TABLE_META_STORE_FACTORY_CLASS.equals(metaStoreFactory.getClass().getName());
  }

  protected Map<String, String> buildTableParams(
      Object partitionCommitter, OneInputStreamOperator<?, ?> fileWriterOperator) {
    Configuration tableOptions = getTableOptions(partitionCommitter, fileWriterOperator);
    Map<String, String> tableParams = new HashMap<>(tableOptions.toMap());
    tableParams.putIfAbsent("path", getLocationPath(partitionCommitter, fileWriterOperator));
    tableParams.putIfAbsent("format", resolveWriteFormat(fileWriterOperator));
    tableParams.put("connector", "filesystem");
    return tableParams;
  }

  protected String getSinkDescription() {
    return "FileSystemInsertTable";
  }

  protected String getDefaultFormat() {
    return "unknown";
  }

  protected Configuration getTableOptions(OneInputStreamOperator<?, ?> fileWriterOperator) {
    return (Configuration)
        ReflectUtils.getObjectField(fileWriterOperator.getClass(), fileWriterOperator, "conf");
  }

  protected Configuration getTableOptions(
      Object partitionCommitter, OneInputStreamOperator<?, ?> fileWriterOperator) {
    if (partitionCommitter != null) {
      return (Configuration)
          ReflectUtils.getObjectField(PARTITION_COMMITTER_CLASS, partitionCommitter, "conf");
    }
    return getTableOptions(fileWriterOperator);
  }

  protected String getLocationPath(
      Object partitionCommitter, OneInputStreamOperator<?, ?> fileWriterOperator) {
    if (partitionCommitter != null) {
      Object locationPath =
          ReflectUtils.getObjectField(
              PARTITION_COMMITTER_CLASS, partitionCommitter, "locationPath");
      return locationPath.toString();
    }
    Object bucketsBuilder =
        ReflectUtils.getObjectField(
            ABSTRACT_STREAMING_WRITER_CLASS, fileWriterOperator, "bucketsBuilder");
    Object basePath = ReflectUtils.tryGetObjectField(bucketsBuilder, "basePath");
    return basePath.toString();
  }

  protected String resolveWriteFormat(OneInputStreamOperator<?, ?> fileWriterOperator) {
    Object bucketsBuilder =
        ReflectUtils.getObjectField(
            ABSTRACT_STREAMING_WRITER_CLASS, fileWriterOperator, "bucketsBuilder");
    String format = resolveFormatFromBucketsBuilder(bucketsBuilder);
    if (format != null) {
      return format;
    }
    return getDefaultFormat();
  }

  protected String resolveFormatFromBucketsBuilder(Object bucketsBuilder) {
    Class<?> builderClass = bucketsBuilder.getClass();
    String builderName = builderClass.getName();
    if (builderName.contains("HadoopPathBasedBulkFormatBuilder")) {
      Object writerFactory = ReflectUtils.tryGetObjectField(bucketsBuilder, "writerFactory");
      if (writerFactory != null) {
        return resolveFormatFromHadoopBulkWriterFactory(writerFactory);
      }
      return inferFormatFromClassName(builderName);
    }
    if (builderName.contains("BulkFormatBuilder")) {
      Object writerFactory = ReflectUtils.tryGetObjectField(bucketsBuilder, "writerFactory");
      if (writerFactory != null) {
        return resolveFormatFromBulkWriterFactory(writerFactory);
      }
      return inferFormatFromClassName(builderName);
    }
    if (builderName.contains("RowFormatBuilder")) {
      Object encoder = ReflectUtils.tryGetObjectField(bucketsBuilder, "encoder");
      if (encoder != null) {
        return inferFormatFromClassName(encoder.getClass().getName());
      }
    }
    return inferFormatFromClassName(builderName);
  }

  protected String resolveFormatFromHadoopBulkWriterFactory(Object writerFactory) {
    return inferFormatFromClassName(writerFactory.getClass().getName());
  }

  protected String resolveFormatFromBulkWriterFactory(Object writerFactory) {
    String format = inferFormatFromClassName(writerFactory.getClass().getName());
    if (format != null) {
      return format;
    }
    Object innerFactory =
        ReflectUtils.tryGetObjectField(writerFactory.getClass(), writerFactory, "factory");
    if (innerFactory != null) {
      format = inferFormatFromClassName(innerFactory.getClass().getName());
      if (format != null) {
        return format;
      }
    }
    return null;
  }

  protected String inferFormatFromClassName(String className) {
    String lower = className.toLowerCase();
    if (lower.contains("parquet")) {
      return "parquet";
    }
    if (lower.contains("orc")) {
      return "orc";
    }
    if (lower.contains("avro")) {
      return "avro";
    }
    if (lower.contains("json")) {
      return "json";
    }
    if (lower.contains("csv")) {
      return "csv";
    }
    return null;
  }
}

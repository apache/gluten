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
package org.apache.iceberg.spark.source;

import org.apache.gluten.connector.write.DataFileJson;
import org.apache.gluten.connector.write.PartitionDataJson;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * Compatibility accessors for Iceberg's package-private {@link SparkPositionDeltaWrite}.
 *
 * <p>Iceberg intentionally keeps the position-delta implementation internal. Gluten needs a small
 * package-local bridge so a native writer can preserve Iceberg's original {@code DeltaBatchWrite}
 * coordinator while producing Iceberg's expected {@code DeltaTaskCommit} messages.
 */
public final class SparkPositionDeltaWriteUtil {
  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private SparkPositionDeltaWriteUtil() {}

  /**
   * Returns {@code null} when the write has the V3/Puffin shape required by native mutation writes,
   * or a fallback reason otherwise.
   */
  public static String validate(DeltaWrite write) {
    if (!(write instanceof SparkPositionDeltaWrite)) {
      return "Not an Iceberg SparkPositionDeltaWrite";
    }

    try {
      WriteInfo info = writeInfo(write);
      if (TableUtil.formatVersion(info.table()) != 3) {
        return "Native Iceberg mutation write requires table format version 3";
      }
      if (info.dataFileFormat() != FileFormat.PARQUET) {
        return "Native Iceberg mutation write requires Parquet data files";
      }
      if (info.deleteFileFormat() != FileFormat.PUFFIN) {
        return "Native Iceberg mutation write requires Puffin delete files";
      }
      if (info.command() != RowLevelOperation.Command.DELETE
          && info.command() != RowLevelOperation.Command.UPDATE
          && info.command() != RowLevelOperation.Command.MERGE) {
        return "Unsupported Iceberg row-level command: " + info.command();
      }

      return null;
    } catch (RuntimeException e) {
      return e.getMessage();
    }
  }

  public static WriteInfo writeInfo(DeltaWrite write) {
    if (!(write instanceof SparkPositionDeltaWrite)) {
      throw new IllegalArgumentException("Not an Iceberg SparkPositionDeltaWrite");
    }

    SparkPositionDeltaWrite positionDeltaWrite = (SparkPositionDeltaWrite) write;
    Object context = readField(positionDeltaWrite, "context");

    return new WriteInfo(
        (JavaSparkContext) readField(positionDeltaWrite, "sparkContext"),
        (Table) readField(positionDeltaWrite, "table"),
        (RowLevelOperation.Command) readField(positionDeltaWrite, "command"),
        (SparkBatchQueryScan) readField(positionDeltaWrite, "scan"),
        (Map<String, String>) readField(positionDeltaWrite, "writeProperties"),
        (org.apache.iceberg.Schema) readField(context, "dataSchema"),
        (StructType) readField(context, "dataSparkType"),
        (StructType) readField(context, "deleteSparkType"),
        (FileFormat) readField(context, "dataFileFormat"),
        (FileFormat) readField(context, "deleteFileFormat"),
        (String) readField(context, "queryId"));
  }

  public static Map<String, ReferencedDataFile> referencedDataFiles(DeltaWrite write) {
    return referencedDataFiles(writeInfo(write));
  }

  /**
   * Converts Velox Iceberg commit JSON into the commit message expected by Iceberg's original
   * {@code DeltaBatchWrite}. The original coordinator remains responsible for validation, atomic
   * commit, abort cleanup, and removal of rewritten deletion vectors.
   */
  public static WriterCommitMessage toDeltaTaskCommit(
      Map<Integer, PartitionSpec> specs,
      SortOrder sortOrder,
      FileFormat dataFileFormat,
      String[] nativeCommitMessages,
      Map<String, ReferencedDataFile> referencedDataFiles) {
    WriteResult.Builder result = WriteResult.builder();

    for (String message : nativeCommitMessages) {
      DataFileJson file = parseCommitMessage(message);
      if ("DATA".equals(file.content)) {
        result.addDataFiles(toDataFile(file, specs, sortOrder, dataFileFormat));
      } else if ("POSITION_DELETES".equals(file.content)) {
        ReferencedDataFile referenced = referencedDataFiles.get(file.referencedDataFile);
        if (referenced == null) {
          throw new IllegalArgumentException(
              "Deletion vector references an unplanned data file: " + file.referencedDataFile);
        }
        result.addDeleteFiles(toDeletionVector(file, referenced));
        result.addReferencedDataFiles(file.referencedDataFile);
        if (referenced.existingDeletionVector() != null) {
          result.addRewrittenDeleteFiles(referenced.existingDeletionVector());
        }
      } else {
        throw new IllegalArgumentException(
            "Unsupported native Iceberg file content: " + file.content);
      }
    }

    return new SparkPositionDeltaWrite.DeltaTaskCommit(result.build());
  }

  /**
   * Deletes files that were closed by the native writer but could not be converted into an Iceberg
   * task commit. Such files are not visible to Iceberg's batch abort because no commit message was
   * produced.
   */
  public static void deleteNativeFiles(
      FileIO fileIO, String[] nativeCommitMessages, Throwable cause) {
    for (String message : nativeCommitMessages) {
      try {
        DataFileJson file = parseCommitMessage(message);
        fileIO.deleteFile(file.path);
      } catch (RuntimeException e) {
        cause.addSuppressed(e);
      }
    }
  }

  private static Map<String, ReferencedDataFile> referencedDataFiles(WriteInfo info) {
    Map<String, ReferencedDataFile> referenced = new LinkedHashMap<>();
    for (PartitionScanTask task : info.scan().tasks()) {
      FileScanTask fileScanTask = task.asFileScanTask();
      DataFile dataFile = fileScanTask.file();
      String dataFilePath = dataFile.location().toString();
      DeleteFile existingDeletionVector = null;

      for (DeleteFile deleteFile : fileScanTask.deletes()) {
        if (!isDeletionVector(deleteFile)) {
          throw new IllegalArgumentException(
              "Native Iceberg mutation write cannot rewrite delete file: " + deleteFile.location());
        }
        existingDeletionVector = deleteFile;
      }

      referenced.put(
          dataFilePath,
          new ReferencedDataFile(dataFile, fileScanTask.spec(), existingDeletionVector));
    }

    return referenced;
  }

  private static DataFile toDataFile(
      DataFileJson file,
      Map<Integer, PartitionSpec> specs,
      SortOrder sortOrder,
      FileFormat defaultFormat) {
    PartitionSpec spec = specs.get(file.partitionSpecJson);

    FileFormat format =
        file.fileFormat == null ? defaultFormat : FileFormat.fromString(file.fileFormat);
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath(file.path)
            .withFormat(format)
            .withFileSizeInBytes(file.fileSizeInBytes);

    if (file.partitionDataJson != null) {
      builder.withPartition(PartitionDataJson.fromJson(file.partitionDataJson, spec));
    }
    if (file.metrics != null) {
      builder.withMetrics(file.metrics.metrics());
    }
    if (file.splitOffsets != null) {
      builder.withSplitOffsets(file.splitOffsets);
    }
    if (sortOrder != null) {
      builder.withSortOrder(sortOrder);
    }
    return builder.build();
  }

  private static DeleteFile toDeletionVector(DataFileJson file, ReferencedDataFile referenced) {
    StructLike partition = referenced.dataFile().partition();
    return FileMetadata.deleteFileBuilder(referenced.spec())
        .ofPositionDeletes()
        .withPath(file.path)
        .withFormat(FileFormat.PUFFIN)
        .withPartition(partition)
        .withRecordCount(file.metrics.metrics().recordCount())
        .withFileSizeInBytes(file.fileSizeInBytes)
        .withReferencedDataFile(file.referencedDataFile)
        .withContentOffset(file.contentOffset)
        .withContentSizeInBytes(file.contentSizeInBytes)
        .build();
  }

  private static DataFileJson parseCommitMessage(String message) {
    try {
      return MAPPER.readValue(message, DataFileJson.class);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to parse native Iceberg commit message: " + message, e);
    }
  }

  private static boolean isDeletionVector(DeleteFile deleteFile) {
    return deleteFile.content() == FileContent.POSITION_DELETES
        && deleteFile.format() == FileFormat.PUFFIN;
  }

  private static Object readField(Object target, String name) {
    try {
      Field field = target.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(
          format("Cannot access %s.%s", target.getClass().getName(), name), e);
    }
  }

  public static final class WriteInfo {
    private final JavaSparkContext sparkContext;
    private final Table table;
    private final RowLevelOperation.Command command;
    private final SparkBatchQueryScan scan;
    private final Map<String, String> writeProperties;
    private final org.apache.iceberg.Schema dataSchema;
    private final StructType dataSparkType;
    private final StructType deleteSparkType;
    private final FileFormat dataFileFormat;
    private final FileFormat deleteFileFormat;
    private final String queryId;

    private WriteInfo(
        JavaSparkContext sparkContext,
        Table table,
        RowLevelOperation.Command command,
        SparkBatchQueryScan scan,
        Map<String, String> writeProperties,
        org.apache.iceberg.Schema dataSchema,
        StructType dataSparkType,
        StructType deleteSparkType,
        FileFormat dataFileFormat,
        FileFormat deleteFileFormat,
        String queryId) {
      this.sparkContext = sparkContext;
      this.table = table;
      this.command = command;
      this.scan = scan;
      this.writeProperties = writeProperties;
      this.dataSchema = dataSchema;
      this.dataSparkType = dataSparkType;
      this.deleteSparkType = deleteSparkType;
      this.dataFileFormat = dataFileFormat;
      this.deleteFileFormat = deleteFileFormat;
      this.queryId = queryId;
    }

    public JavaSparkContext sparkContext() {
      return sparkContext;
    }

    public Table table() {
      return table;
    }

    public RowLevelOperation.Command command() {
      return command;
    }

    SparkBatchQueryScan scan() {
      return scan;
    }

    public Map<String, String> writeProperties() {
      return writeProperties;
    }

    public org.apache.iceberg.Schema dataSchema() {
      return dataSchema;
    }

    public StructType dataSparkType() {
      return dataSparkType;
    }

    public StructType deleteSparkType() {
      return deleteSparkType;
    }

    public FileFormat dataFileFormat() {
      return dataFileFormat;
    }

    public FileFormat deleteFileFormat() {
      return deleteFileFormat;
    }

    public String queryId() {
      return queryId;
    }
  }

  public static final class ReferencedDataFile implements Serializable {
    private final DataFile dataFile;
    private final PartitionSpec spec;
    private final DeleteFile existingDeletionVector;

    public ReferencedDataFile(
        DataFile dataFile, PartitionSpec spec, DeleteFile existingDeletionVector) {
      this.dataFile = dataFile;
      this.spec = spec;
      this.existingDeletionVector = existingDeletionVector;
    }

    public DataFile dataFile() {
      return dataFile;
    }

    public PartitionSpec spec() {
      return spec;
    }

    public DeleteFile existingDeletionVector() {
      return existingDeletionVector;
    }
  }
}

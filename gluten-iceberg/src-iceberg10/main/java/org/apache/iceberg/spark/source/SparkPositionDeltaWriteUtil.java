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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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

  public static boolean supportsWrite(DeltaWrite write) {
    return validate(write) == null;
  }

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

      referencedDataFiles(info);
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
        (StructType) readField(context, "metadataSparkType"),
        (FileFormat) readField(context, "dataFileFormat"),
        (FileFormat) readField(context, "deleteFileFormat"),
        (String) readField(context, "queryId"));
  }

  public static Map<String, ReferencedDataFile> referencedDataFiles(DeltaWrite write) {
    return referencedDataFiles(writeInfo(write));
  }

  public static Broadcast<Map<String, ReferencedDataFile>> broadcastReferencedDataFiles(
      DeltaWrite write) {
    WriteInfo info = writeInfo(write);
    return info.sparkContext().broadcast(referencedDataFiles(info));
  }

  public static WriterCommitMessage toDeltaTaskCommit(
      DeltaWrite write, String[] nativeCommitMessages) {
    WriteInfo info = writeInfo(write);
    return toDeltaTaskCommit(
        info.table().specs(),
        info.table().sortOrder(),
        info.dataFileFormat(),
        nativeCommitMessages,
        referencedDataFiles(info));
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
    requireNonNull(specs, "specs is null");
    requireNonNull(dataFileFormat, "dataFileFormat is null");
    requireNonNull(nativeCommitMessages, "nativeCommitMessages is null");
    requireNonNull(referencedDataFiles, "referencedDataFiles is null");

    WriteResult.Builder result = WriteResult.builder();
    Set<String> emittedDeletionVectors = new LinkedHashSet<>();

    for (String message : nativeCommitMessages) {
      DataFileJson file = parseCommitMessage(message);
      String content = file.content == null ? "DATA" : file.content;
      if ("DATA".equals(content)) {
        result.addDataFiles(toDataFile(file, specs, sortOrder, dataFileFormat));
      } else if ("POSITION_DELETES".equals(content)) {
        ReferencedDataFile referenced =
            requireReferencedDataFile(file.referencedDataFile, referencedDataFiles);
        if (!emittedDeletionVectors.add(file.referencedDataFile)) {
          throw new IllegalArgumentException(
              "Native writer emitted multiple deletion vectors for data file: "
                  + file.referencedDataFile);
        }

        result.addDeleteFiles(toDeletionVector(file, referenced));
        result.addReferencedDataFiles(file.referencedDataFile);
        if (referenced.existingDeletionVector() != null) {
          result.addRewrittenDeleteFiles(referenced.existingDeletionVector());
        }
      } else {
        throw new IllegalArgumentException("Unsupported native Iceberg file content: " + content);
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
    requireNonNull(fileIO, "fileIO is null");
    requireNonNull(nativeCommitMessages, "nativeCommitMessages is null");
    requireNonNull(cause, "cause is null");

    Set<String> paths = new LinkedHashSet<>();
    for (String message : nativeCommitMessages) {
      try {
        DataFileJson file = parseCommitMessage(message);
        if (file.path != null && !file.path.isEmpty()) {
          paths.add(file.path);
        }
      } catch (RuntimeException e) {
        cause.addSuppressed(e);
      }
    }

    for (String path : paths) {
      try {
        fileIO.deleteFile(path);
      } catch (RuntimeException e) {
        cause.addSuppressed(e);
      }
    }
  }

  private static Map<String, ReferencedDataFile> referencedDataFiles(WriteInfo info) {
    if (info.scan() == null) {
      return Collections.emptyMap();
    }

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
        validateDeletionVector(deleteFile, dataFilePath);
        if (existingDeletionVector != null
            && !existingDeletionVector
                .location()
                .toString()
                .equals(deleteFile.location().toString())) {
          throw new IllegalArgumentException(
              "Multiple deletion vectors found for data file: " + dataFilePath);
        }
        existingDeletionVector = deleteFile;
      }

      ReferencedDataFile candidate =
          new ReferencedDataFile(dataFile, fileScanTask.spec(), existingDeletionVector);
      ReferencedDataFile previous = referenced.putIfAbsent(dataFilePath, candidate);
      if (previous != null && !previous.sameMetadata(candidate)) {
        throw new IllegalArgumentException(
            "Conflicting metadata found for referenced data file: " + dataFilePath);
      }
    }

    return Collections.unmodifiableMap(referenced);
  }

  private static DataFile toDataFile(
      DataFileJson file,
      Map<Integer, PartitionSpec> specs,
      SortOrder sortOrder,
      FileFormat defaultFormat) {
    validateCommonFileFields(file);
    PartitionSpec spec = requireSpec(file.partitionSpecJson, specs);
    if (spec.isPartitioned() && file.partitionDataJson == null) {
      throw new IllegalArgumentException(
          "Missing partition data for native data file: " + file.path);
    }

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
    validateCommonFileFields(file);
    if (file.fileFormat == null || FileFormat.fromString(file.fileFormat) != FileFormat.PUFFIN) {
      throw new IllegalArgumentException(
          "Native deletion vector must use Puffin format: " + file.path);
    }
    if (file.partitionSpecJson == null
        || file.partitionSpecJson.intValue() != referenced.spec().specId()) {
      throw new IllegalArgumentException(
          format(
              "Deletion vector spec ID %s does not match data file spec ID %s",
              file.partitionSpecJson, referenced.spec().specId()));
    }
    if (file.contentOffset == null || file.contentOffset < 0) {
      throw new IllegalArgumentException("Missing deletion-vector content offset: " + file.path);
    }
    if (file.contentSizeInBytes == null || file.contentSizeInBytes <= 0) {
      throw new IllegalArgumentException("Missing deletion-vector content size: " + file.path);
    }
    if (file.metrics == null || file.metrics.metrics().recordCount() == null) {
      throw new IllegalArgumentException("Missing deletion-vector cardinality: " + file.path);
    }

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
    if (message == null) {
      throw new IllegalArgumentException("Native Iceberg commit message is null");
    }
    try {
      return MAPPER.readValue(message, DataFileJson.class);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to parse native Iceberg commit message: " + message, e);
    }
  }

  private static PartitionSpec requireSpec(Integer specId, Map<Integer, PartitionSpec> specs) {
    if (specId == null) {
      throw new IllegalArgumentException("Missing partition spec ID in native commit message");
    }
    PartitionSpec spec = specs.get(specId);
    if (spec == null) {
      throw new IllegalArgumentException("Unknown partition spec ID: " + specId);
    }
    return spec;
  }

  private static ReferencedDataFile requireReferencedDataFile(
      String path, Map<String, ReferencedDataFile> referencedDataFiles) {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("Missing referenced data file in deletion-vector result");
    }
    ReferencedDataFile referenced = referencedDataFiles.get(path);
    if (referenced == null) {
      throw new IllegalArgumentException(
          "Deletion vector references an unplanned data file: " + path);
    }
    return referenced;
  }

  private static void validateCommonFileFields(DataFileJson file) {
    if (file.path == null || file.path.isEmpty()) {
      throw new IllegalArgumentException("Missing path in native Iceberg commit message");
    }
    if (file.fileSizeInBytes < 0) {
      throw new IllegalArgumentException("Missing file size for native Iceberg file: " + file.path);
    }
  }

  private static boolean isDeletionVector(DeleteFile deleteFile) {
    return deleteFile.content() == FileContent.POSITION_DELETES
        && deleteFile.format() == FileFormat.PUFFIN;
  }

  private static void validateDeletionVector(DeleteFile deleteFile, String dataFilePath) {
    if (deleteFile.referencedDataFile() == null
        || !dataFilePath.contentEquals(deleteFile.referencedDataFile())) {
      throw new IllegalArgumentException(
          "Deletion vector does not reference its scanned data file: " + deleteFile.location());
    }
    if (deleteFile.contentOffset() == null || deleteFile.contentOffset() < 0) {
      throw new IllegalArgumentException(
          "Deletion vector is missing a content offset: " + deleteFile.location());
    }
    if (deleteFile.contentSizeInBytes() == null || deleteFile.contentSizeInBytes() <= 0) {
      throw new IllegalArgumentException(
          "Deletion vector is missing a content size: " + deleteFile.location());
    }
  }

  private static Object readField(Object target, String name) {
    Class<?> current = target.getClass();
    while (current != null) {
      try {
        Field field = current.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(
            format("Cannot access %s.%s", target.getClass().getName(), name), e);
      }
    }
    throw new IllegalStateException(format("Cannot find %s.%s", target.getClass().getName(), name));
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
    private final StructType metadataSparkType;
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
        StructType metadataSparkType,
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
      this.metadataSparkType = metadataSparkType;
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

    public StructType metadataSparkType() {
      return metadataSparkType;
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
      this.dataFile = requireNonNull(dataFile, "dataFile is null");
      this.spec = requireNonNull(spec, "spec is null");
      this.existingDeletionVector = existingDeletionVector;
      if (dataFile.specId() != spec.specId()) {
        throw new IllegalArgumentException(
            format(
                "Data file spec ID %s does not match partition spec ID %s",
                dataFile.specId(), spec.specId()));
      }
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

    private boolean sameMetadata(ReferencedDataFile other) {
      String existingPath =
          existingDeletionVector == null ? null : existingDeletionVector.location().toString();
      String otherExistingPath =
          other.existingDeletionVector == null
              ? null
              : other.existingDeletionVector.location().toString();
      return spec.specId() == other.spec.specId()
          && java.util.Objects.equals(dataFile.partition(), other.dataFile.partition())
          && java.util.Objects.equals(existingPath, otherExistingPath);
    }
  }
}

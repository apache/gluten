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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.backendsapi.BackendsApiManager;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.substrait.proto.AdvancedExtension;
import io.substrait.proto.ReadRel;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.*;

public class IcebergLocalFilesNode extends LocalFilesNode {
  private final List<List<DeleteFile>> deleteFilesList;
  private final Map<String, Integer> fieldIds;
  private final Map<String, String> initialDefaults;
  private Schema icebergSchema;
  private List<Long> dataSequenceNumbers;
  private List<Map<Integer, byte[]>> identityPartitionKeys;

  public void setEqualityDeleteMetadata(
      Schema schema, List<Long> sequenceNumbers, List<Map<Integer, byte[]>> partitionKeys) {
    icebergSchema = schema;
    dataSequenceNumbers = sequenceNumbers;
    identityPartitionKeys = partitionKeys;
  }

  private static ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FieldId fieldId(
      Types.NestedField field) {
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FieldId.Builder builder =
        ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FieldId.newBuilder()
            .setId(field.fieldId());
    if (field.type().isNestedType()) {
      for (Types.NestedField child : field.type().asNestedType().fields()) {
        builder.addChildren(fieldId(child));
      }
    }
    return builder.build();
  }

  IcebergLocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Map<String, String>> partitionColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations,
      List<List<DeleteFile>> deleteFilesList,
      List<Map<String, String>> metadataColumns,
      Map<String, Integer> fieldIds,
      Map<String, String> initialDefaults) {
    super(
        index,
        paths,
        starts,
        lengths,
        new ArrayList<>(),
        new ArrayList<>(),
        partitionColumns,
        metadataColumns,
        fileFormat,
        preferredLocations,
        new HashMap<>(),
        new ArrayList<>());
    this.deleteFilesList = deleteFilesList;
    this.fieldIds = fieldIds;
    this.initialDefaults = initialDefaults;
  }

  @Override
  public ReadRel.LocalFiles toProtobuf() {
    ReadRel.LocalFiles localFiles = super.toProtobuf();
    if (initialDefaults.isEmpty()) {
      return localFiles;
    }

    Any extension =
        BackendsApiManager.getTransformerApiInstance()
            .packIcebergReadExtension(fieldIds, initialDefaults);

    ReadRel.LocalFiles.Builder builder = localFiles.toBuilder();
    builder.setAdvancedExtension(AdvancedExtension.newBuilder().setEnhancement(extension));
    return builder.build();
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    List<DeleteFile> deleteFiles = deleteFilesList.get(index);
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.Builder icebergBuilder =
        ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.newBuilder();
    if (icebergSchema != null) {
      for (Types.NestedField field : icebergSchema.columns()) {
        icebergBuilder.addSchemaFieldIds(fieldId(field));
      }
      icebergBuilder.setDataSequenceNumber(dataSequenceNumbers.get(index));
      identityPartitionKeys
          .get(index)
          .forEach(
              (id, value) -> {
                ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.IdentityPartitionKey.Builder key =
                    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.IdentityPartitionKey
                        .newBuilder()
                        .setSourceId(id)
                        .setIsNull(value == null);
                if (value != null) {
                  key.setValue(ByteString.copyFrom(value));
                }
                icebergBuilder.addIdentityPartitionKeys(key);
              });
    }
    switch (fileBuilder.getFileFormatCase()) {
      case PARQUET:
        icebergBuilder.setParquet(fileBuilder.getParquet());
        break;
      case ORC:
        icebergBuilder.setOrc(fileBuilder.getOrc());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file format "
                + fileBuilder.getFileFormatCase().name()
                + " for iceberg data file.");
    }

    for (DeleteFile delete : deleteFiles) {
      ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.Builder deleteFileBuilder =
          ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.newBuilder();
      ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FileContent fileContent;
      switch (delete.content()) {
        case EQUALITY_DELETES:
          fileContent =
              ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FileContent.EQUALITY_DELETES;
          break;
        case POSITION_DELETES:
          fileContent =
              ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FileContent.POSITION_DELETES;
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported FileCount " + delete.content().name() + " for delete file.");
      }
      deleteFileBuilder.setFileContent(fileContent);
      deleteFileBuilder.setFilePath(delete.path().toString());
      deleteFileBuilder.setFileSize(delete.fileSizeInBytes());
      deleteFileBuilder.setRecordCount(delete.recordCount());
      if (delete.dataSequenceNumber() != null) {
        deleteFileBuilder.setDataSequenceNumber(delete.dataSequenceNumber());
      }
      if (delete.content() == FileContent.POSITION_DELETES) {
        deleteFileBuilder.setLowerBounds(encodeBounds(delete.lowerBounds()));
        deleteFileBuilder.setUpperBounds(encodeBounds(delete.upperBounds()));
      }
      switch (delete.format()) {
        case PARQUET:
          ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions parquetReadOptions =
              ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.newBuilder().build();
          deleteFileBuilder.setParquet(parquetReadOptions);
          break;
        case ORC:
          ReadRel.LocalFiles.FileOrFiles.OrcReadOptions orcReadOptions =
              ReadRel.LocalFiles.FileOrFiles.OrcReadOptions.newBuilder().build();
          deleteFileBuilder.setOrc(orcReadOptions);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported format " + delete.format().name() + " for delete file.");
      }
      if (delete.equalityFieldIds() != null && !delete.equalityFieldIds().isEmpty()) {
        deleteFileBuilder.addAllEqualityFieldIds(delete.equalityFieldIds());
      }
      icebergBuilder.addDeleteFiles(deleteFileBuilder);
    }
    fileBuilder.setIceberg(icebergBuilder);
  }

  private static ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.Map encodeBounds(
      Map<Integer, ByteBuffer> bounds) {
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.Map.Builder builder =
        ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.Map.newBuilder();

    if (bounds == null || bounds.isEmpty()) {
      return builder.build();
    }

    for (Map.Entry<Integer, ByteBuffer> entry : bounds.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }

      ByteBuffer duplicate = entry.getValue().asReadOnlyBuffer();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);

      builder.addKeyValues(
          ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.Map.KeyValue.newBuilder()
              .setKey(entry.getKey())
              .setValue(Base64.getEncoder().encodeToString(bytes))
              .build());
    }

    return builder.build();
  }
}

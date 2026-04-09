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

import io.substrait.proto.ReadRel;
import org.apache.iceberg.DeleteFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergLocalFilesNode extends LocalFilesNode {
  private final List<List<DeleteFile>> deleteFilesList;

  IcebergLocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Map<String, String>> partitionColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations,
      List<List<DeleteFile>> deleteFilesList,
      List<Map<String, String>> metadataColumns) {
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
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    List<DeleteFile> deleteFiles = deleteFilesList.get(index);
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.Builder icebergBuilder =
        ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.newBuilder();

    switch (fileFormat) {
      case ParquetReadFormat:
        ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions parquetReadOptions =
            ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.newBuilder().build();
        icebergBuilder.setParquet(parquetReadOptions);
        break;
      case OrcReadFormat:
        ReadRel.LocalFiles.FileOrFiles.OrcReadOptions orcReadOptions =
            ReadRel.LocalFiles.FileOrFiles.OrcReadOptions.newBuilder().build();
        icebergBuilder.setOrc(orcReadOptions);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file format " + fileFormat.name() + " for iceberg data file.");
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
}

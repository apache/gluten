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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IcebergDeletionVectorTest {
  @Test
  public void serializesDistinctBlobsInTheSamePuffinFile() throws Exception {
    long offset = (1L << 32) + 4;
    DeleteFile first = deletionVector("/data/first.parquet", offset, 100);
    DeleteFile second = deletionVector("/data/second.parquet", offset + 100, 200);
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions actual =
        serialize(Arrays.asList(first, second));

    Assertions.assertEquals(2, actual.getDeleteFilesCount());
    for (int i = 0; i < 2; i++) {
      DeleteFile expected = Arrays.asList(first, second).get(i);
      ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile file = actual.getDeleteFiles(i);
      Assertions.assertEquals(
          ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.FileContent.POSITION_DELETES,
          file.getFileContent());
      Assertions.assertTrue(file.hasPuffin());
      Assertions.assertEquals(expected.path().toString(), file.getFilePath());
      Assertions.assertEquals(expected.recordCount(), file.getRecordCount());
      Assertions.assertEquals(expected.fileSizeInBytes(), file.getFileSize());
      Assertions.assertEquals(
          expected.referencedDataFile(), file.getPuffin().getReferencedDataFile());
      Assertions.assertEquals(
          expected.contentOffset().longValue(), file.getPuffin().getContentOffset());
      Assertions.assertEquals(
          expected.contentSizeInBytes().longValue(), file.getPuffin().getContentSizeInBytes());
    }
  }

  @Test
  public void preservesV2PositionDeletesAlongsideDeletionVectors() throws Exception {
    DeleteFile positional =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/data/delete.parquet")
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(2)
            .withFileSizeInBytes(100)
            .build();
    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions actual =
        serialize(Arrays.asList(positional, deletionVector("/data/file.parquet", 4, 100)));
    Assertions.assertTrue(actual.getDeleteFiles(0).hasParquet());
    Assertions.assertFalse(actual.getDeleteFiles(0).hasPuffin());
    Assertions.assertTrue(actual.getDeleteFiles(1).hasPuffin());
  }

  @Test
  public void rejectsBlobOutsidePuffinFile() {
    DeleteFile invalid = deletionVector("/data/file.parquet", (1L << 33) - 10, 100);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> serialize(Collections.singletonList(invalid)));
  }

  private static DeleteFile deletionVector(String dataFile, long offset, long length) {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes()
        .withPath("/data/deletes.puffin")
        .withFormat(FileFormat.PUFFIN)
        .withRecordCount(3)
        .withFileSizeInBytes(1L << 33)
        .withReferencedDataFile(dataFile)
        .withContentOffset(offset)
        .withContentSizeInBytes(length)
        .build();
  }

  private static ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions serialize(
      List<DeleteFile> deletes) throws Exception {
    IcebergLocalFilesNode node =
        new IcebergLocalFilesNode(
            0,
            Collections.singletonList("/data/file.parquet"),
            Collections.singletonList(0L),
            Collections.singletonList(100L),
            Collections.singletonList(Collections.emptyMap()),
            LocalFilesNode.ReadFileFormat.ParquetReadFormat,
            Collections.emptyList(),
            Collections.singletonList(deletes),
            Collections.singletonList(Collections.emptyMap()),
            Collections.emptyMap(),
            Collections.emptyMap());
    ReadRel.LocalFiles.FileOrFiles.Builder file = ReadRel.LocalFiles.FileOrFiles.newBuilder();
    node.processFileBuilder(file, 0);
    return ReadRel.LocalFiles.FileOrFiles.parseFrom(file.build().toByteArray()).getIceberg();
  }
}

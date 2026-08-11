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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IcebergLocalFilesNodeBoundsTest {
  private static final int ICEBERG_POS_FIELD_ID = 2147483545;

  @Test
  public void serializesDeleteFileLowerAndUpperBounds() {
    byte[] lowerBytes = new byte[] {1, 2, 3, 4};
    byte[] upperBytes = new byte[] {5, 6, 7, 8};

    FakeDeleteFile deleteFile =
        new FakeDeleteFile(
            FileContent.POSITION_DELETES,
            FileFormat.PARQUET,
            "/tmp/delete.parquet",
            2L,
            123L,
            Collections.emptyList(),
            Collections.singletonMap(ICEBERG_POS_FIELD_ID, ByteBuffer.wrap(lowerBytes)),
            Collections.singletonMap(ICEBERG_POS_FIELD_ID, ByteBuffer.wrap(upperBytes)),
            null);
    IcebergLocalFilesNode node =
        new IcebergLocalFilesNode(
            0,
            Collections.singletonList("/tmp/data.parquet"),
            Collections.singletonList(0L),
            Collections.singletonList(100L),
            Collections.singletonList(Collections.emptyMap()),
            LocalFilesNode.ReadFileFormat.ParquetReadFormat,
            Collections.emptyList(),
            Collections.singletonList(Collections.singletonList(deleteFile)),
            Collections.singletonList(Collections.emptyMap()));

    ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
        ReadRel.LocalFiles.FileOrFiles.newBuilder();

    node.processFileBuilder(fileBuilder, 0);

    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile actualDelete =
        fileBuilder.getIceberg().getDeleteFiles(0);

    Assert.assertEquals(1, actualDelete.getLowerBounds().getKeyValuesCount());
    Assert.assertEquals(
        ICEBERG_POS_FIELD_ID, actualDelete.getLowerBounds().getKeyValues(0).getKey());
    Assert.assertEquals(
        Base64.getEncoder().encodeToString(lowerBytes),
        actualDelete.getLowerBounds().getKeyValues(0).getValue());

    Assert.assertEquals(1, actualDelete.getUpperBounds().getKeyValuesCount());
    Assert.assertEquals(
        ICEBERG_POS_FIELD_ID, actualDelete.getUpperBounds().getKeyValues(0).getKey());
    Assert.assertEquals(
        Base64.getEncoder().encodeToString(upperBytes),
        actualDelete.getUpperBounds().getKeyValues(0).getValue());
  }

  private static final class FakeDeleteFile implements DeleteFile {
    private final FileContent content;
    private final FileFormat format;
    private final String path;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final List<Integer> equalityFieldIds;
    private final Map<Integer, ByteBuffer> lowerBounds;
    private final Map<Integer, ByteBuffer> upperBounds;
    private final Long dataSequenceNumber;

    private FakeDeleteFile(
        FileContent content,
        FileFormat format,
        String path,
        long recordCount,
        long fileSizeInBytes,
        List<Integer> equalityFieldIds,
        Map<Integer, ByteBuffer> lowerBounds,
        Map<Integer, ByteBuffer> upperBounds,
        Long dataSequenceNumber) {
      this.content = content;
      this.format = format;
      this.path = path;
      this.recordCount = recordCount;
      this.fileSizeInBytes = fileSizeInBytes;
      this.equalityFieldIds = equalityFieldIds;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
      this.dataSequenceNumber = dataSequenceNumber;
    }

    @Override
    public Long pos() {
      return 0L;
    }

    @Override
    public int specId() {
      return 0;
    }

    @Override
    public FileContent content() {
      return content;
    }

    @Override
    public CharSequence path() {
      return path;
    }

    @Override
    public FileFormat format() {
      return format;
    }

    @Override
    public StructLike partition() {
      return null;
    }

    @Override
    public long recordCount() {
      return recordCount;
    }

    @Override
    public long fileSizeInBytes() {
      return fileSizeInBytes;
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return null;
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return lowerBounds;
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return upperBounds;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return null;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return equalityFieldIds;
    }

    @Override
    public Long dataSequenceNumber() {
      return dataSequenceNumber;
    }

    @Override
    public DeleteFile copy() {
      return this;
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return this;
    }
  }
}

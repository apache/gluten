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

import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat;

import io.substrait.proto.ReadRel;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IcebergLocalFilesNodeTest {
  @Test
  public void serializesPuffinDeletionVectorMetadata() {
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    lowerBounds.put(1, ByteBuffer.wrap(longBytes(2)));
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    upperBounds.put(1, ByteBuffer.wrap(longBytes(9)));

    DeleteFile deletionVector =
        deleteFile(
            FileContent.POSITION_DELETES,
            FileFormat.PUFFIN,
            "file:/table/delete.puffin",
            256L,
            3L,
            lowerBounds,
            upperBounds,
            7L,
            64L,
            32L,
            "file:/table/data.parquet");

    IcebergLocalFilesNode node =
        new IcebergLocalFilesNode(
            0,
            Collections.singletonList("file:/table/data.parquet"),
            Collections.singletonList(0L),
            Collections.singletonList(1024L),
            Collections.singletonList(Collections.emptyMap()),
            ReadFileFormat.ParquetReadFormat,
            Collections.emptyList(),
            Collections.singletonList(Collections.singletonList(deletionVector)),
            Collections.singletonList(Collections.emptyMap()));

    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile actual =
        node.toProtobuf().getItems(0).getIceberg().getDeleteFiles(0);
    assertEquals(
        ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.FileFormatCase.PUFFIN,
        actual.getFileFormatCase());
    assertEquals(64L, actual.getContentOffset());
    assertEquals(32L, actual.getContentSizeInBytes());
    assertEquals("file:/table/data.parquet", actual.getReferencedDataFile());
    assertEquals(7L, actual.getDataSequenceNumber());
    assertEquals(
        Base64.getEncoder().encodeToString(longBytes(2)),
        actual.getLowerBounds().getKeyValues(0).getValue());
    assertEquals(
        Base64.getEncoder().encodeToString(longBytes(9)),
        actual.getUpperBounds().getKeyValues(0).getValue());
  }

  @Test
  public void keepsLegacyDeleteFieldsOptional() {
    DeleteFile positionDelete =
        deleteFile(
            FileContent.POSITION_DELETES,
            FileFormat.PARQUET,
            "file:/table/delete.parquet",
            128L,
            2L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            null,
            null,
            null,
            null);

    IcebergLocalFilesNode node =
        new IcebergLocalFilesNode(
            0,
            Collections.singletonList("file:/table/data.parquet"),
            Collections.singletonList(0L),
            Collections.singletonList(1024L),
            Collections.singletonList(Collections.emptyMap()),
            ReadFileFormat.ParquetReadFormat,
            Collections.emptyList(),
            Collections.singletonList(Collections.singletonList(positionDelete)),
            Collections.singletonList(Collections.emptyMap()));

    ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile actual =
        node.toProtobuf().getItems(0).getIceberg().getDeleteFiles(0);
    assertEquals(
        ReadRel.LocalFiles.FileOrFiles.IcebergReadOptions.DeleteFile.FileFormatCase.PARQUET,
        actual.getFileFormatCase());
    assertTrue(!actual.hasContentOffset());
    assertTrue(!actual.hasContentSizeInBytes());
    assertTrue(!actual.hasReferencedDataFile());
    assertTrue(!actual.hasDataSequenceNumber());
  }

  private static byte[] longBytes(long value) {
    return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
  }

  private static DeleteFile deleteFile(
      FileContent content,
      FileFormat format,
      String path,
      long fileSize,
      long recordCount,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds,
      Long dataSequenceNumber,
      Long contentOffset,
      Long contentSizeInBytes,
      String referencedDataFile) {
    return (DeleteFile)
        Proxy.newProxyInstance(
            DeleteFile.class.getClassLoader(),
            new Class<?>[] {DeleteFile.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "content":
                  return content;
                case "format":
                  return format;
                case "path":
                case "location":
                  return path;
                case "fileSizeInBytes":
                  return fileSize;
                case "recordCount":
                  return recordCount;
                case "lowerBounds":
                  return lowerBounds;
                case "upperBounds":
                  return upperBounds;
                case "equalityFieldIds":
                  return Collections.emptyList();
                case "dataSequenceNumber":
                  return dataSequenceNumber;
                case "contentOffset":
                  return contentOffset;
                case "contentSizeInBytes":
                  return contentSizeInBytes;
                case "referencedDataFile":
                  return referencedDataFile;
                case "toString":
                  return path;
                default:
                  return null;
              }
            });
  }
}

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeltaLocalFilesNode extends LocalFilesNode {
  private final List<DeltaFileReadOptions> deltaReadOptions = new ArrayList<>();

  DeltaLocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Long> fileSizes,
      List<Long> modificationTimes,
      List<Map<String, String>> partitionColumns,
      List<Map<String, String>> metadataColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations,
      Map<String, String> properties,
      List<Map<String, Object>> otherMetadataColumns,
      List<DeltaFileReadOptions> deltaReadOptions) {
    super(
        index,
        paths,
        starts,
        lengths,
        fileSizes,
        modificationTimes,
        partitionColumns,
        metadataColumns,
        fileFormat,
        preferredLocations,
        properties,
        otherMetadataColumns);
    this.deltaReadOptions.addAll(deltaReadOptions);
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    DeltaFileReadOptions options = deltaReadOptions.get(index);
    ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.Builder deltaBuilder =
        ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.newBuilder()
            .setRowIndexFilterType(toProtoRowIndexFilterType(options.rowIndexFilterType()))
            .setHasDeletionVector(options.hasDeletionVector());

    if (options.hasDeletionVector()) {
      deltaBuilder
          .setDeletionVectorCardinality(options.deletionVectorCardinality())
          .setDeletionVectorPayloadIndex(options.deletionVectorPayloadIndex());
    }

    fileBuilder.setDelta(deltaBuilder.build());
  }

  private static ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.RowIndexFilterType
      toProtoRowIndexFilterType(RowIndexFilterType rowIndexFilterType) {
    switch (rowIndexFilterType) {
      case IF_CONTAINED:
        return ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.RowIndexFilterType.IF_CONTAINED;
      case IF_NOT_CONTAINED:
        return ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.RowIndexFilterType.IF_NOT_CONTAINED;
      case KEEP_ALL:
      default:
        return ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.RowIndexFilterType.KEEP_ALL;
    }
  }

  public enum RowIndexFilterType {
    KEEP_ALL,
    IF_CONTAINED,
    IF_NOT_CONTAINED
  }

  public static class DeltaFileReadOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RowIndexFilterType rowIndexFilterType;
    private final boolean hasDeletionVector;
    private final long deletionVectorCardinality;
    private final int deletionVectorPayloadIndex;

    public DeltaFileReadOptions(
        RowIndexFilterType rowIndexFilterType,
        boolean hasDeletionVector,
        long deletionVectorCardinality,
        int deletionVectorPayloadIndex) {
      this.rowIndexFilterType = rowIndexFilterType;
      this.hasDeletionVector = hasDeletionVector;
      this.deletionVectorCardinality = deletionVectorCardinality;
      this.deletionVectorPayloadIndex = deletionVectorPayloadIndex;
    }

    public RowIndexFilterType rowIndexFilterType() {
      return rowIndexFilterType;
    }

    public boolean hasDeletionVector() {
      return hasDeletionVector;
    }

    public long deletionVectorCardinality() {
      return deletionVectorCardinality;
    }

    public int deletionVectorPayloadIndex() {
      return deletionVectorPayloadIndex;
    }
  }
}

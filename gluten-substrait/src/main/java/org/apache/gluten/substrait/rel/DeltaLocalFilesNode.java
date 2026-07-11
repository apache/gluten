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

import com.google.protobuf.UnsafeByteOperations;
import io.substrait.proto.ReadRel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeltaLocalFilesNode extends LocalFilesNode {
  private final List<DeltaFileReadOptions> deltaReadOptions = new ArrayList<>();

  DeltaLocalFilesNode(
      LocalFilesNode base,
      List<Map<String, Object>> otherMetadataColumns,
      List<DeltaFileReadOptions> deltaReadOptions) {
    super(base, otherMetadataColumns);
    if (deltaReadOptions == null || deltaReadOptions.size() != getPaths().size()) {
      throw new IllegalArgumentException(
          String.format(
              "deltaReadOptions must contain one entry per file path, expected %d but got %s",
              getPaths().size(),
              deltaReadOptions == null ? "null" : String.valueOf(deltaReadOptions.size())));
    }
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
          .setSerializedDeletionVector(
              UnsafeByteOperations.unsafeWrap(options.serializedDeletionVector()));
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
    private final byte[] serializedDeletionVector;

    public DeltaFileReadOptions(
        RowIndexFilterType rowIndexFilterType,
        boolean hasDeletionVector,
        long deletionVectorCardinality,
        byte[] serializedDeletionVector) {
      this.rowIndexFilterType = rowIndexFilterType;
      this.hasDeletionVector = hasDeletionVector;
      this.deletionVectorCardinality = deletionVectorCardinality;
      this.serializedDeletionVector =
          serializedDeletionVector == null ? new byte[0] : serializedDeletionVector;
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

    public byte[] serializedDeletionVector() {
      return serializedDeletionVector;
    }
  }
}

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

  /**
   * Serializable source for a deletion-vector payload.
   *
   * <p>The source travels inside a Spark input partition. Implementations may therefore defer
   * remote I/O until {@link #materialize()} is called while the split is converted to protobuf on
   * an executor. The returned byte array must not be modified: protobuf wraps it without copying.
   */
  public interface DeletionVectorPayload extends Serializable {
    byte[] materialize();

    /** Returns whether the payload bytes are already resident in this object. */
    boolean isMaterialized();
  }

  /** A payload source for inline DVs whose bytes are already present in Delta metadata. */
  public static final class InMemoryDeletionVectorPayload implements DeletionVectorPayload {
    private static final long serialVersionUID = 1L;

    private final byte[] payload;

    public InMemoryDeletionVectorPayload(byte[] payload) {
      this.payload = payload == null ? new byte[0] : payload.clone();
    }

    @Override
    public byte[] materialize() {
      return payload;
    }

    @Override
    public boolean isMaterialized() {
      return true;
    }
  }

  public static class DeltaFileReadOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RowIndexFilterType rowIndexFilterType;
    private final boolean hasDeletionVector;
    private final long deletionVectorCardinality;
    private final DeletionVectorPayload deletionVectorPayload;

    public DeltaFileReadOptions(
        RowIndexFilterType rowIndexFilterType,
        boolean hasDeletionVector,
        long deletionVectorCardinality,
        byte[] serializedDeletionVector) {
      this(
          rowIndexFilterType,
          hasDeletionVector,
          deletionVectorCardinality,
          new InMemoryDeletionVectorPayload(serializedDeletionVector));
    }

    public DeltaFileReadOptions(
        RowIndexFilterType rowIndexFilterType,
        boolean hasDeletionVector,
        long deletionVectorCardinality,
        DeletionVectorPayload deletionVectorPayload) {
      if (rowIndexFilterType == null) {
        throw new IllegalArgumentException("rowIndexFilterType must not be null");
      }
      if (deletionVectorPayload == null) {
        throw new IllegalArgumentException("deletionVectorPayload must not be null");
      }
      this.rowIndexFilterType = rowIndexFilterType;
      this.hasDeletionVector = hasDeletionVector;
      this.deletionVectorCardinality = deletionVectorCardinality;
      this.deletionVectorPayload = deletionVectorPayload;
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

    /**
     * Materializes and returns the serialized deletion-vector bytes.
     *
     * <p>For an on-disk deletion vector this may perform blocking filesystem I/O and is intended to
     * run during executor-side split-to-protobuf conversion. The returned array must not be
     * modified because protobuf wraps it without copying.
     */
    public byte[] serializedDeletionVector() {
      return deletionVectorPayload.materialize();
    }

    public boolean isDeletionVectorPayloadMaterialized() {
      return deletionVectorPayload.isMaterialized();
    }
  }
}

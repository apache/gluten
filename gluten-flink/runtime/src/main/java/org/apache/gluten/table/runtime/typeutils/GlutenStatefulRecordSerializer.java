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
package org.apache.gluten.table.runtime.typeutils;

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.table.runtime.operators.GlutenTaskSessionContext;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Serializer for {@link RowVector}. */
@Internal
public class GlutenStatefulRecordSerializer extends TypeSerializer<StatefulRecord> {
  private static final long serialVersionUID = 1L;
  private final RowType rowType;
  private final String operatorId;

  public GlutenStatefulRecordSerializer(RowType rowType, GlutenOperator operator) {
    this(rowType, operator.getId());
  }

  private GlutenStatefulRecordSerializer(RowType rowType, String operatorId) {
    this.rowType = rowType;
    this.operatorId = operatorId;
  }

  @Override
  public TypeSerializer<StatefulRecord> duplicate() {
    return new GlutenStatefulRecordSerializer(rowType, operatorId);
  }

  @Override
  public StatefulRecord createInstance() {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void serialize(StatefulRecord record, DataOutputView target) throws IOException {
    String vectorStr = record.getRowVector().serialize();
    target.writeInt(vectorStr.getBytes().length);
    target.write(vectorStr.getBytes());
  }

  @Override
  public StatefulRecord deserialize(DataInputView source) throws IOException {
    int len = source.readInt();
    byte[] str = new byte[len];
    source.readFully(str);
    RowVector rowVector =
        GlutenTaskSessionContext.getSession(operatorId)
            .baseVectorOps()
            .deserializeOne(new String(str))
            .asRowVector();
    StatefulRecord record = new StatefulRecord(operatorId, rowVector.id(), 0, false, -1);
    record.setRowVector(rowVector);
    return record;
  }

  @Override
  public StatefulRecord deserialize(StatefulRecord reuse, DataInputView source) throws IOException {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public StatefulRecord copy(StatefulRecord from) {
    return from;
  }

  @Override
  public StatefulRecord copy(StatefulRecord from, StatefulRecord reuse) {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GlutenStatefulRecordSerializer) {
      if (rowType != null) {
        GlutenStatefulRecordSerializer other = (GlutenStatefulRecordSerializer) obj;
        return rowType.equals(other.rowType);
      }
      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    if (rowType == null) {
      return 0;
    }
    return rowType.hashCode();
  }

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public TypeSerializerSnapshot<StatefulRecord> snapshotConfiguration() {
    return new RowVectorSerializerSnapshot(rowType, operatorId);
  }

  /** {@link TypeSerializerSnapshot} for Gluten RowVector.. */
  public static final class RowVectorSerializerSnapshot
      implements TypeSerializerSnapshot<StatefulRecord> {
    private static final int CURRENT_VERSION = 1;

    private RowType rowType;
    private String operatorId;

    @SuppressWarnings("unused")
    public RowVectorSerializerSnapshot() {
      // this constructor is used when restoring from a checkpoint/savepoint.
    }

    RowVectorSerializerSnapshot(RowType rowType, String operatorId) {
      this.rowType = rowType;
      this.operatorId = operatorId;
    }

    @Override
    public int getCurrentVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {}

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
        throws IOException {}

    @Override
    public GlutenStatefulRecordSerializer restoreSerializer() {
      return new GlutenStatefulRecordSerializer(rowType, operatorId);
    }

    @Override
    public TypeSerializerSchemaCompatibility<StatefulRecord> resolveSchemaCompatibility(
        TypeSerializerSnapshot<StatefulRecord> oldSerializerSnapshot) {
      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
  }
}

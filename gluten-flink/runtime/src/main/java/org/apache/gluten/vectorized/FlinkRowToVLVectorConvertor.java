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
package org.apache.gluten.vectorized;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.table.Table;

import java.util.ArrayList;
import java.util.List;

/** Converter between velox RowVector and Flink RowData. */
public class FlinkRowToVLVectorConvertor {

  // Matches the C++ kRowKindColumnName in velox/experimental/stateful/RowKind.h.
  // The merged RowVector carries per-row RowKind bytes as a trailing TINYINT column
  // with this name. Java appends it in fromRowData; C++ strips it via
  // StreamRecord::create and re-appends it on output via
  // StreamRecord::toMergedRowVector (only when the stream is not appendOnly).
  public static final String ROW_KIND_COLUMN_NAME = "$row_kind";

  public static RowVector fromRowData(
      RowData row, BufferAllocator allocator, Session session, RowType rowType) {
    List<Type> fieldTypes = rowType.getChildren();
    List<String> fieldNames = rowType.getNames();
    List<FieldVector> arrowVectors = new ArrayList<>(rowType.size() + 1);
    for (int i = 0; i < rowType.size(); i++) {
      ArrowVectorWriter writer =
          ArrowVectorWriter.create(fieldNames.get(i), fieldTypes.get(i), allocator);
      writer.write(i, row);
      writer.finish();
      arrowVectors.add(i, writer.getVector());
    }
    TinyIntVector rowKindVector = new TinyIntVector(ROW_KIND_COLUMN_NAME, allocator);
    rowKindVector.allocateNew(1);
    rowKindVector.setSafe(0, row.getRowKind().toByteValue());
    rowKindVector.setValueCount(1);
    arrowVectors.add(rowKindVector);

    return session.arrowOps().fromArrowTable(allocator, new Table(arrowVectors));
  }

  public static List<RowData> toRowData(
      RowVector rowVector, BufferAllocator allocator, RowType rowType) {
    // TODO: support more types
    BaseVector loadedVector = null;
    FieldVector structVector = null;

    try {
      loadedVector = rowVector.loadedVector();
      // The result is StructVector
      structVector = Arrow.toArrowVector(allocator, loadedVector);
      final List<FieldVector> fieldVectors = structVector.getChildrenFromFields();
      List<ArrowVectorAccessor> accessors =
          buildArrowVectorAccessors(fieldVectors.subList(0, rowType.size()));
      byte[] rowKinds = extractRowKindsIfPresent(fieldVectors, rowType.size(), rowVector.getSize());
      List<RowData> rowDatas = new ArrayList<>(rowVector.getSize());
      for (int j = 0; j < rowVector.getSize(); j++) {
        Object[] fieldValues = new Object[rowType.size()];
        for (int i = 0; i < rowType.size(); i++) {
          fieldValues[i] = accessors.get(i).get(j);
        }
        if (rowKinds != null) {
          rowDatas.add(GenericRowData.ofKind(RowKind.fromByteValue(rowKinds[j]), fieldValues));
        } else {
          rowDatas.add(GenericRowData.of(fieldValues));
        }
      }
      return rowDatas;
    } finally {
      /// The FieldVector/BaseVector should be closed in `finally`, to avoid it may not be closed
      // when exceptions rasied,
      /// that lead to memory leak.
      if (structVector != null) {
        structVector.close();
      }
      if (loadedVector != null) {
        loadedVector.close();
      }
    }
  }

  private static byte[] extractRowKindsIfPresent(
      List<FieldVector> fieldVectors, int schemaSize, int rowCount) {
    if (fieldVectors.size() != schemaSize + 1) {
      return null;
    }
    FieldVector lastVector = fieldVectors.get(schemaSize);
    if (!ROW_KIND_COLUMN_NAME.equals(lastVector.getField().getName())) {
      return null;
    }
    if (!(lastVector instanceof TinyIntVector)) {
      return null;
    }
    TinyIntVector tinyInt = (TinyIntVector) lastVector;
    byte[] rowKinds = new byte[rowCount];
    for (int i = 0; i < rowCount; i++) {
      rowKinds[i] = tinyInt.get(i);
    }
    return rowKinds;
  }

  private static List<ArrowVectorAccessor> buildArrowVectorAccessors(List<FieldVector> vectors) {
    List<ArrowVectorAccessor> accessors = new ArrayList<>(vectors.size());
    for (int i = 0; i < vectors.size(); ++i) {
      accessors.add(i, ArrowVectorAccessor.create(vectors.get(i)));
    }
    return accessors;
  }
}

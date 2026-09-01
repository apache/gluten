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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/*
 * This module is used to convert column vector to flink generic rows.
 * BinaryRowData is not supported here.
 */
public abstract class ArrowVectorAccessor {
  private interface AccessorBuilder {
    ArrowVectorAccessor build(FieldVector vector);
  }

  // Exact class matches
  private static final Map<Class<? extends FieldVector>, AccessorBuilder> accessorBuilders =
      Map.ofEntries(
          Map.entry(BitVector.class, vector -> new BooleanVectorAccessor(vector)),
          Map.entry(IntVector.class, vector -> new IntVectorAccessor(vector)),
          Map.entry(TinyIntVector.class, vector -> new TinyIntVectorAccessor(vector)),
          Map.entry(BigIntVector.class, vector -> new BigIntVectorAccessor(vector)),
          Map.entry(Float8Vector.class, vector -> new DoubleVectorAccessor(vector)),
          Map.entry(DecimalVector.class, vector -> new DecimalVectorAccessor(vector)),
          Map.entry(VarCharVector.class, vector -> new VarCharVectorAccessor(vector)),
          Map.entry(StructVector.class, vector -> new StructVectorAccessor(vector)),
          Map.entry(ListVector.class, vector -> new ListVectorAccessor(vector)),
          Map.entry(DateDayVector.class, vector -> new DateDayVectorAccessor(vector)),
          Map.entry(TimeStampMicroVector.class, vector -> new TimeStampMicroVectorAccessor(vector)),
          Map.entry(MapVector.class, vector -> new MapVectorAccessor(vector)));

  public static ArrowVectorAccessor create(FieldVector vector) {
    if (vector == null) {
      throw new IllegalArgumentException(
          "ArrowVectorAccessor. Cannot create accessor for null vector.");
    }
    AccessorBuilder builder = accessorBuilders.get(vector.getClass());
    if (builder == null) {
      throw new UnsupportedOperationException(
          "ArrowVectorAccessor. Unsupported vector type: " + vector.getClass().getName());
    }
    return builder.build(vector);
  }

  // A general method to extract values from the vector.
  public Object get(int rowIndex) {
    throw new UnsupportedOperationException("get not supported");
  }
}

class BaseArrowVectorAccessor<T extends FieldVector> extends ArrowVectorAccessor {
  protected final T typedVector;

  public BaseArrowVectorAccessor(FieldVector vector) {
    this.typedVector = (T) vector;
  }

  @Override
  public Object get(int rowIndex) {
    if (typedVector.isNull(rowIndex)) {
      return null; // Handle null values
    }
    return getImpl(rowIndex);
  }

  protected Object getImpl(int rowIndex) {
    throw new UnsupportedOperationException(
        "getImpl not implemented for " + typedVector.getClass().getName());
  }
}

class BooleanVectorAccessor extends BaseArrowVectorAccessor<BitVector> {
  public BooleanVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    return typedVector.get(rowIndex) != 0;
  }
}

class IntVectorAccessor extends BaseArrowVectorAccessor<IntVector> {
  public IntVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    return typedVector.get(rowIndex);
  }
}

class TinyIntVectorAccessor extends BaseArrowVectorAccessor<TinyIntVector> {
  public TinyIntVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    return typedVector.get(rowIndex);
  }
}

class BigIntVectorAccessor extends BaseArrowVectorAccessor<BigIntVector> {

  public BigIntVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    return typedVector.get(rowIndex);
  }
}

class DoubleVectorAccessor extends BaseArrowVectorAccessor<Float8Vector> {

  public DoubleVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    return typedVector.get(rowIndex);
  }
}

class DecimalVectorAccessor extends BaseArrowVectorAccessor<DecimalVector> {

  private int precision = 0;
  private int scale = 0;

  public DecimalVectorAccessor(FieldVector vector) {
    super(vector);
    this.precision = typedVector.getPrecision();
    this.scale = typedVector.getScale();
  }

  @Override
  protected Object getImpl(int rowIndex) {
    BigDecimal decimalData = (BigDecimal) typedVector.getObject(rowIndex);
    return DecimalData.fromBigDecimal(decimalData, precision, scale);
  }
}

class DateDayVectorAccessor extends BaseArrowVectorAccessor<DateDayVector> {

  public DateDayVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    return typedVector.get(rowIndex);
  }
}

class VarCharVectorAccessor extends BaseArrowVectorAccessor<VarCharVector> {
  public VarCharVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    return BinaryStringData.fromBytes(typedVector.get(rowIndex));
  }
}

class StructVectorAccessor extends BaseArrowVectorAccessor<StructVector> {
  private final List<ArrowVectorAccessor> fieldAccessors;
  private final int fieldCount;

  public StructVectorAccessor(FieldVector vector) {
    super(vector);
    this.fieldCount = this.typedVector.size();
    this.fieldAccessors = new ArrayList<>(fieldCount);
    for (int i = 0; i < this.fieldCount; i++) {
      FieldVector fieldVector = (FieldVector) this.typedVector.getChildByOrdinal(i);
      this.fieldAccessors.add(i, ArrowVectorAccessor.create(fieldVector));
    }
  }

  @Override
  protected Object getImpl(int rowIndex) {
    Object[] fieldValues = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      fieldValues[i] = fieldAccessors.get(i).get(rowIndex);
    }
    return GenericRowData.of(fieldValues);
  }
}

class ListVectorAccessor extends BaseArrowVectorAccessor<ListVector> {

  private final ArrowVectorAccessor elementAccessor;

  public ListVectorAccessor(FieldVector vector) {
    super(vector);
    FieldVector elementVector = this.typedVector.getDataVector();
    this.elementAccessor = ArrowVectorAccessor.create(elementVector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    int startIndex = typedVector.getElementStartIndex(rowIndex);
    int endIndex = typedVector.getElementEndIndex(rowIndex);
    Object[] elements = new Object[endIndex - startIndex];
    for (int i = startIndex; i < endIndex; i++) {
      elements[i - startIndex] = elementAccessor.get(i);
    }
    return new GenericArrayData(elements);
  }
}

// In Arrow, the internal implementation of a map vector is an array vector.
class MapVectorAccessor extends BaseArrowVectorAccessor<MapVector> {
  private final StructVector entriesVector;
  private final ArrowVectorAccessor keyAccessor;
  private final ArrowVectorAccessor valueAccessor;

  public MapVectorAccessor(FieldVector vector) {
    super(vector);
    this.entriesVector = (StructVector) this.typedVector.getDataVector();
    FieldVector keyVector = this.entriesVector.getChild(MapVector.KEY_NAME);
    FieldVector valueVector = this.entriesVector.getChild(MapVector.VALUE_NAME);
    this.keyAccessor = ArrowVectorAccessor.create(keyVector);
    this.valueAccessor = ArrowVectorAccessor.create(valueVector);
  }

  @Override
  protected Object getImpl(int rowIndex) {
    int startIndex = typedVector.getElementStartIndex(rowIndex);
    int endIndex = typedVector.getElementEndIndex(rowIndex);
    Map<Object, Object> mapEntries = new LinkedHashMap<>();
    for (int i = startIndex; i < endIndex; i++) {
      Object key = keyAccessor.get(i);
      Object value = valueAccessor.get(i);
      mapEntries.put(key, value);
    }
    return new GenericMapData(mapEntries);
  }
}

class TimeStampMicroVectorAccessor extends BaseArrowVectorAccessor<TimeStampMicroVector> {

  public TimeStampMicroVectorAccessor(FieldVector vector) {
    super(vector);
  }

  @Override
  public Object getImpl(int rowIndex) {
    long milliseconds = typedVector.get(rowIndex) / 1000;
    return TimestampData.fromEpochMillis(milliseconds);
  }
}

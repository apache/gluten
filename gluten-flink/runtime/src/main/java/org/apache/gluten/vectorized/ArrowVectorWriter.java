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

import io.github.zhztheplayer.velox4j.type.*;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class ArrowVectorWriter {
  private interface WriterBuilder {
    ArrowVectorWriter build(Type fieldType, BufferAllocator allocator, FieldVector vector);
  }

  // Exact class matches
  private static Map<Class<? extends Type>, WriterBuilder> writerBuilders =
      Map.ofEntries(
          Map.entry(
              IntegerType.class,
              (fieldType, allocator, vector) -> new IntVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              BooleanType.class,
              (fieldType, allocator, vector) ->
                  new BooleanVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              BigIntType.class,
              (fieldType, allocator, vector) ->
                  new BigIntVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              DoubleType.class,
              (fieldType, allocator, vector) ->
                  new Float8VectorWriter(fieldType, allocator, vector)),
          Map.entry(
              VarCharType.class,
              (fieldType, allocator, vector) ->
                  new VarCharVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              TimestampType.class,
              (fieldType, allocator, vector) ->
                  new TimestampVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              DateType.class,
              (fieldType, allocator, vector) ->
                  new DateDayVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              RowType.class,
              (fieldType, allocator, vector) ->
                  new StructVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              ArrayType.class,
              (fieldType, allocator, vector) ->
                  new ArrayVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              MapType.class,
              (fieldType, allocator, vector) -> new MapVectorWriter(fieldType, allocator, vector)),
          Map.entry(
              DecimalType.class,
              (fieldType, allocator, vector) ->
                  new DecimalVectorWriter(fieldType, allocator, vector)));

  public static ArrowVectorWriter create(
      String fieldName, Type fieldType, BufferAllocator allocator) {
    return create(fieldName, fieldType, allocator, null);
  }

  protected static ArrowVectorWriter create(
      String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
    if (vector == null) {
      // Build an empty vector
      vector = FieldVectorCreator.create(fieldName, fieldType, false, allocator, null);
    }
    WriterBuilder builder = writerBuilders.get(fieldType.getClass());
    if (builder == null) {
      throw new UnsupportedOperationException(
          "ArrowVectorWriter. Unsupported type: " + fieldType.getClass().getName());
    }
    return builder.build(fieldType, allocator, vector);
  }

  protected FieldVector vector = null;
  protected int valueCount = 0;

  ArrowVectorWriter(FieldVector vector) {
    this.vector = vector;
  }

  public void write(int fieldIndex, RowData rowData) {
    throw new UnsupportedOperationException("assign is not supported");
  }

  public void writeArray(ArrayData arrayData) {
    throw new UnsupportedOperationException("writeArray is not supported");
  }

  int getValueCount() {
    return valueCount;
  }

  FieldVector getVector() {
    return vector;
  }

  void finish() {
    vector.setValueCount(valueCount);
  }
}

// Build FieldVector from Type.
class FieldVectorCreator {
  public static FieldVector create(
      String name, Type dataType, boolean nullable, BufferAllocator allocator, String timeZoneId) {
    Field field = toArrowField(name, dataType, nullable, timeZoneId);
    return field.createVector(allocator);
  }

  private interface ArrowTypeConverter {
    ArrowType convert(Type dataType, String timeZoneId);
  }

  // Exact class matches
  private static Map<Class<? extends Type>, ArrowTypeConverter> arrowTypeConverters =
      Map.ofEntries(
          Map.entry(BooleanType.class, (dataType, timeZoneId) -> ArrowType.Bool.INSTANCE),
          Map.entry(IntegerType.class, (dataType, timeZoneId) -> new ArrowType.Int(8 * 4, true)),
          Map.entry(BigIntType.class, (dataType, timeZoneId) -> new ArrowType.Int(8 * 8, true)),
          Map.entry(
              DoubleType.class,
              (dataType, timeZoneId) -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
          Map.entry(VarCharType.class, (dataType, timeZoneId) -> ArrowType.Utf8.INSTANCE),
          Map.entry(
              TimestampType.class,
              (dataType, timeZoneId) ->
                  new ArrowType.Timestamp(
                      TimeUnit.MILLISECOND, timeZoneId == null ? "UTC" : timeZoneId)),
          Map.entry(DateType.class, (dataType, timeZoneId) -> new ArrowType.Date(DateUnit.DAY)),
          Map.entry(
              DecimalType.class,
              (dataType, timeZoneId) -> {
                DecimalType decimalType = (DecimalType) dataType;
                return new ArrowType.Decimal(
                    decimalType.getPrecision(), decimalType.getScale(), 128);
              }));

  private static ArrowType toArrowType(Type dataType, String timeZoneId) {
    ArrowTypeConverter converter = arrowTypeConverters.get(dataType.getClass());
    if (converter == null) {
      throw new UnsupportedOperationException("Unsupported type: " + dataType.getClass().getName());
    }
    return converter.convert(dataType, timeZoneId);
  }

  private static Field toArrowField(
      String name, Type dataType, boolean nullable, String timeZoneId) {
    if (dataType instanceof ArrayType) {
      List<Type> elementTypes = ((ArrayType) dataType).getChildren();
      if (elementTypes.size() != 1) {
        throw new UnsupportedOperationException("ArrayType should have exactly one element type");
      }

      FieldType fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null);
      List<Field> elementFields = new ArrayList<>();
      elementFields.add(toArrowField("element", elementTypes.get(0), nullable, timeZoneId));

      return new Field(name, fieldType, elementFields);

    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      FieldType mapFieldType = new FieldType(nullable, new ArrowType.Map(false), null);

      List<String> fieldNames = Arrays.asList(MapVector.KEY_NAME, MapVector.VALUE_NAME);
      List<Type> fieldTypes = mapType.getChildren();
      RowType structType = new RowType(fieldNames, fieldTypes);
      Field structField =
          toArrowField(MapVector.DATA_VECTOR_NAME, structType, nullable, timeZoneId);

      return new Field(name, mapFieldType, Arrays.asList(structField));

    } else if (dataType instanceof RowType) {
      RowType structType = (RowType) dataType;
      List<String> fieldNames = structType.getNames();
      List<Type> fieldTypes = structType.getChildren();
      List<Field> subFields = new ArrayList<>();
      for (int i = 0; i < structType.getChildren().size(); ++i) {
        subFields.add(toArrowField(fieldNames.get(i), fieldTypes.get(i), nullable, timeZoneId));
      }
      FieldType strcutType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null);
      return new Field(name, strcutType, subFields);
    } else {
      // TODO: support nullable
      ArrowType arrowType = toArrowType(dataType, timeZoneId);
      FieldType fieldType = new FieldType(nullable, arrowType, null);
      return new Field(name, fieldType, new ArrayList<>());
    }
  }
}

abstract class BaseVectorWriter<T extends FieldVector, V> extends ArrowVectorWriter {
  protected final T typedVector;

  protected BaseVectorWriter(FieldVector vector) {
    super(vector);
    this.typedVector = (T) vector;
  }

  protected abstract V getValue(RowData rowData, int fieldIndex);

  protected abstract V getValue(ArrayData arrayData, int index);

  protected abstract void setValue(int index, V value);

  @Override
  public void write(int fieldIndex, RowData rowData) {
    if (rowData.isNullAt(fieldIndex)) {
      this.typedVector.setNull(valueCount);
    } else {
      setValue(valueCount, getValue(rowData, fieldIndex));
    }
    valueCount++;
  }

  @Override
  public void writeArray(ArrayData arrayData) {
    for (int i = 0; i < arrayData.size(); i++) {
      if (arrayData.isNullAt(i)) {
        this.typedVector.setNull(valueCount);
      } else {
        setValue(valueCount, getValue(arrayData, i));
      }
      valueCount++;
    }
  }
}

class IntVectorWriter extends BaseVectorWriter<IntVector, Integer> {
  public IntVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
  }

  @Override
  protected Integer getValue(RowData rowData, int fieldIndex) {
    return rowData.getInt(fieldIndex);
  }

  @Override
  protected Integer getValue(ArrayData arrayData, int index) {
    return arrayData.getInt(index);
  }

  @Override
  protected void setValue(int index, Integer value) {
    this.typedVector.setSafe(index, value);
  }
}

class BooleanVectorWriter extends BaseVectorWriter<BitVector, Boolean> {
  public BooleanVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
  }

  @Override
  protected Boolean getValue(RowData rowData, int fieldIndex) {
    return rowData.getBoolean(fieldIndex);
  }

  @Override
  protected Boolean getValue(ArrayData arrayData, int index) {
    return arrayData.getBoolean(index);
  }

  @Override
  protected void setValue(int index, Boolean value) {
    this.typedVector.setSafe(index, value ? 1 : 0);
  }
}

class BigIntVectorWriter extends BaseVectorWriter<BigIntVector, Long> {

  public BigIntVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
  }

  @Override
  protected Long getValue(RowData rowData, int fieldIndex) {
    return rowData.getLong(fieldIndex);
  }

  @Override
  protected Long getValue(ArrayData arrayData, int index) {
    return arrayData.getLong(index);
  }

  @Override
  protected void setValue(int index, Long value) {
    this.typedVector.setSafe(index, value);
  }
}

class Float8VectorWriter extends BaseVectorWriter<Float8Vector, Double> {

  public Float8VectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
  }

  @Override
  protected Double getValue(RowData rowData, int fieldIndex) {
    return rowData.getDouble(fieldIndex);
  }

  @Override
  protected Double getValue(ArrayData arrayData, int index) {
    return arrayData.getDouble(index);
  }

  @Override
  protected void setValue(int index, Double value) {
    this.typedVector.setSafe(index, value);
  }
}

class DecimalVectorWriter extends BaseVectorWriter<DecimalVector, DecimalData> {
  private final DecimalType decimalType;

  public DecimalVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.decimalType = (DecimalType) fieldType;
  }

  @Override
  protected DecimalData getValue(RowData rowData, int fieldIndex) {
    return rowData.getDecimal(fieldIndex, decimalType.getPrecision(), decimalType.getScale());
  }

  @Override
  protected DecimalData getValue(ArrayData arrayData, int index) {
    return arrayData.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
  }

  @Override
  protected void setValue(int index, DecimalData value) {
    this.typedVector.setSafe(index, value.toBigDecimal());
  }
}

class VarCharVectorWriter extends BaseVectorWriter<VarCharVector, byte[]> {

  public VarCharVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
  }

  @Override
  protected byte[] getValue(RowData rowData, int fieldIndex) {
    return rowData.getString(fieldIndex).toBytes();
  }

  @Override
  protected byte[] getValue(ArrayData arrayData, int index) {
    return arrayData.getString(index).toBytes();
  }

  @Override
  protected void setValue(int index, byte[] value) {
    this.typedVector.setSafe(index, value);
  }
}

class TimestampVectorWriter extends BaseVectorWriter<FieldVector, Long> {
  private final int precision = 3; // Millisecond precision

  public TimestampVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    // Verify that the vector is a timestamp vector (either TimeStampMilliVector or
    // TimeStampMilliTZVector)
    if (!(vector instanceof TimeStampMilliVector) && !(vector instanceof TimeStampMilliTZVector)) {
      throw new IllegalArgumentException(
          "Expected TimeStampMilliVector or TimeStampMilliTZVector, but got: "
              + vector.getClass().getName());
    }
  }

  @Override
  protected Long getValue(RowData rowData, int fieldIndex) {
    return rowData.getTimestamp(fieldIndex, precision).getMillisecond();
  }

  @Override
  protected Long getValue(ArrayData arrayData, int index) {
    return arrayData.getTimestamp(index, precision).getMillisecond();
  }

  @Override
  protected void setValue(int index, Long value) {

    // Both TimeStampMilliVector and TimeStampMilliTZVector support setSafe with long value
    if (this.typedVector instanceof TimeStampMilliVector) {
      ((TimeStampMilliVector) this.typedVector).setSafe(index, value);
    } else if (this.typedVector instanceof TimeStampMilliTZVector) {
      ((TimeStampMilliTZVector) this.typedVector).setSafe(index, value);
    } else {
      throw new IllegalStateException(
          "Unexpected vector type: " + this.typedVector.getClass().getName());
    }
  }
}

class DateDayVectorWriter extends BaseVectorWriter<DateDayVector, Integer> {
  public DateDayVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
  }

  @Override
  protected Integer getValue(RowData rowData, int fieldIndex) {
    return rowData.getInt(fieldIndex);
  }

  @Override
  protected Integer getValue(ArrayData arrayData, int index) {
    return arrayData.getInt(index);
  }

  @Override
  protected void setValue(int index, Integer value) {
    this.typedVector.setSafe(index, value);
  }
}

class StructVectorWriter extends BaseVectorWriter<StructVector, RowData> {
  private final int fieldCount;
  private final List<ArrowVectorWriter> fieldWriters;

  public StructVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    RowType rowType = (RowType) fieldType;
    List<String> fieldNames = rowType.getNames();
    fieldCount = fieldNames.size();
    fieldWriters = new ArrayList<>();
    for (int i = 0; i < fieldCount; ++i) {
      fieldWriters.add(
          ArrowVectorWriter.create(
              fieldNames.get(i),
              rowType.getChildren().get(i),
              allocator,
              (FieldVector) (this.typedVector.getChildByOrdinal(i))));
    }
  }

  @Override
  protected RowData getValue(RowData rowData, int fieldIndex) {
    return rowData.getRow(fieldIndex, fieldCount);
  }

  @Override
  protected RowData getValue(ArrayData arrayData, int index) {
    return arrayData.getRow(index, fieldCount);
  }

  @Override
  protected void setValue(int index, RowData value) {
    this.typedVector.setIndexDefined(index);
    for (int i = 0; i < fieldCount; ++i) {
      fieldWriters.get(i).write(i, value);
    }
  }

  @Override
  public void finish() {
    this.typedVector.setValueCount(valueCount);
    for (int i = 0; i < fieldCount; ++i) {
      fieldWriters.get(i).finish();
    }
  }
}

class ArrayVectorWriter extends BaseVectorWriter<ListVector, ArrayData> {
  private final ArrowVectorWriter elementWriter;

  public ArrayVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);

    FieldVector elementVector = (FieldVector) this.typedVector.getDataVector();
    List<Type> elementTypes = ((ArrayType) fieldType).getChildren();
    if (elementTypes.size() != 1) {
      throw new UnsupportedOperationException("ArrayType should have exactly one element type");
    }
    Type elementType = elementTypes.get(0);
    this.elementWriter = ArrowVectorWriter.create("element", elementType, allocator, elementVector);
  }

  @Override
  protected ArrayData getValue(RowData rowData, int fieldIndex) {
    return rowData.getArray(fieldIndex);
  }

  @Override
  protected ArrayData getValue(ArrayData arrayData, int index) {
    return arrayData.getArray(index);
  }

  @Override
  protected void setValue(int index, ArrayData value) {
    this.typedVector.startNewValue(valueCount);
    elementWriter.writeArray(value);
    this.typedVector.endValue(valueCount, value.size());
  }

  @Override
  public void finish() {
    this.typedVector.setValueCount(valueCount);
    elementWriter.finish();
  }
}

class MapVectorWriter extends BaseVectorWriter<MapVector, MapData> {
  private final ArrowVectorWriter keyWriter;
  private final ArrowVectorWriter valueWriter;
  private final StructVector entriesVector;

  public MapVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);

    entriesVector = (StructVector) this.typedVector.getDataVector();

    FieldVector keyVector = (FieldVector) entriesVector.getChild(MapVector.KEY_NAME);
    FieldVector valueVector = (FieldVector) entriesVector.getChild(MapVector.VALUE_NAME);

    MapType mapType = (MapType) fieldType;
    this.keyWriter =
        ArrowVectorWriter.create(
            MapVector.KEY_NAME, mapType.getChildren().get(0), allocator, keyVector);
    this.valueWriter =
        ArrowVectorWriter.create(
            MapVector.VALUE_NAME, mapType.getChildren().get(1), allocator, valueVector);
  }

  @Override
  protected MapData getValue(RowData rowData, int fieldIndex) {
    return rowData.getMap(fieldIndex);
  }

  @Override
  protected MapData getValue(ArrayData arrayData, int index) {
    return arrayData.getMap(index);
  }

  @Override
  protected void setValue(int index, MapData value) {
    this.typedVector.startNewValue(valueCount);
    int arrayValueCount = keyWriter.getValueCount();
    for (int i = 0; i < value.size(); i++) {
      entriesVector.setIndexDefined(arrayValueCount + i);
    }
    keyWriter.writeArray(value.keyArray());
    valueWriter.writeArray(value.valueArray());
    this.typedVector.endValue(valueCount, value.size());
  }

  @Override
  public void finish() {
    this.typedVector.setValueCount(valueCount);
    entriesVector.setValueCount(keyWriter.getValueCount());
    keyWriter.finish();
    valueWriter.finish();
  }
}

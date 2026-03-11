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

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVectorShim;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.utils.SparkArrowUtil;
import org.apache.spark.sql.utils.SparkSchemaUtil;
import org.apache.spark.sql.vectorized.ArrowColumnarArray;
import org.apache.spark.sql.vectorized.ArrowColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value and a java
 * array for the values.
 */
public final class ArrowWritableColumnVector extends WritableColumnVectorShim {
  private static final boolean bigEndianPlatform =
      ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private static final Logger LOG = LoggerFactory.getLogger(ArrowWritableColumnVector.class);

  private ArrowVectorAccessor accessor;
  private ArrowVectorWriter writer;

  private int ordinal;
  private ValueVector vector;
  private ValueVector dictionaryVector;
  public static AtomicLong vectorCount = new AtomicLong(0);
  private AtomicLong refCnt = new AtomicLong(0);
  private boolean closed = false;

  /**
   * Allocates columns to store elements of each field of the schema on heap. Capacity is the
   * initial capacity of the vector and it will grow as necessary. Capacity is in number of
   * elements, not number of bytes.
   */
  public static ArrowWritableColumnVector[] allocateColumns(int capacity, StructType schema) {
    String timeZoneId = SparkSchemaUtil.getLocalTimezoneID();
    Schema arrowSchema = SparkArrowUtil.toArrowSchema(schema, timeZoneId);
    VectorSchemaRoot newRoot =
        VectorSchemaRoot.create(arrowSchema, ArrowBufferAllocators.contextInstance());

    List<FieldVector> fieldVectors = newRoot.getFieldVectors();
    ArrowWritableColumnVector[] vectors = new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] = new ArrowWritableColumnVector(fieldVectors.get(i), i, capacity, true);
    }
    // LOG.info("allocateColumns allocator is " + allocator);
    return vectors;
  }

  public static ArrowWritableColumnVector[] allocateColumns(
      int valueCount, long[] totalSizes, StructType schema) {
    String timeZoneId = SparkSchemaUtil.getLocalTimezoneID();
    Schema arrowSchema = SparkArrowUtil.toArrowSchema(schema, timeZoneId);
    VectorSchemaRoot newRoot =
        VectorSchemaRoot.create(arrowSchema, ArrowBufferAllocators.contextInstance());

    List<FieldVector> fieldVectors = newRoot.getFieldVectors();
    ArrowWritableColumnVector[] vectors = new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] =
          new ArrowWritableColumnVector(
              fieldVectors.get(i), null, i, valueCount, totalSizes[i], true);
    }
    return vectors;
  }

  public static ArrowWritableColumnVector[] loadColumns(
      int capacity, List<FieldVector> fieldVectors, List<FieldVector> dictionaryVectors) {
    if (fieldVectors.size() != dictionaryVectors.size()) {
      throw new IllegalArgumentException(
          "Mismatched field vectors and dictionary vectors. "
              + "Field vector count: "
              + fieldVectors.size()
              + ", "
              + "dictionary vector count: "
              + dictionaryVectors.size());
    }
    ArrowWritableColumnVector[] vectors = new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] =
          new ArrowWritableColumnVector(
              fieldVectors.get(i), dictionaryVectors.get(i), i, capacity, false);
    }
    return vectors;
  }

  public static ArrowWritableColumnVector[] loadColumns(
      int capacity, List<FieldVector> fieldVectors) {
    ArrowWritableColumnVector[] vectors = new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] = new ArrowWritableColumnVector(fieldVectors.get(i), i, capacity, false);
    }
    return vectors;
  }

  public static ArrowWritableColumnVector[] loadColumns(
      int capacity, Schema arrowSchema, ArrowRecordBatch recordBatch, BufferAllocator allocator) {
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
    VectorLoader loader = new VectorLoader(root);
    loader.load(recordBatch);
    return loadColumns(capacity, root.getFieldVectors());
  }

  @Deprecated
  public ArrowWritableColumnVector(ValueVector vector, int ordinal, int capacity, boolean init) {
    this(vector, null, ordinal, capacity, init);
  }

  public ArrowWritableColumnVector(
      ValueVector vector, ValueVector dicionaryVector, int ordinal, int capacity, boolean init) {
    super(capacity, SparkArrowUtil.fromArrowField(vector.getField()));
    vectorCount.getAndIncrement();
    refCnt.getAndIncrement();

    this.ordinal = ordinal;
    this.vector = vector;
    this.dictionaryVector = dicionaryVector;
    if (init) {
      vector.setInitialCapacity(capacity);
      vector.allocateNew();
    }
    writer = createVectorWriter(vector);
    createVectorAccessor(vector, dicionaryVector);
  }

  public ArrowWritableColumnVector(
      ValueVector vector,
      ValueVector dicionaryVector,
      int ordinal,
      int valueCount,
      long totalBytes,
      boolean init) {
    super(valueCount, SparkArrowUtil.fromArrowField(vector.getField()));
    vectorCount.getAndIncrement();
    refCnt.getAndIncrement();

    this.ordinal = ordinal;
    this.vector = vector;
    this.dictionaryVector = dicionaryVector;
    if (init) {
      if (vector instanceof VariableWidthVector) {
        ((VariableWidthVector) vector).allocateNew(totalBytes, capacity);
      } else if (vector instanceof DensityAwareVector) {
        double density;
        if (vector instanceof ListVector) {
          density = Math.sqrt(1.0 * totalBytes / valueCount);
        } else {
          density = 1.0 * totalBytes / valueCount;
        }
        ((DensityAwareVector) vector).setInitialCapacity(capacity, density);
        vector.allocateNew();
      } else {
        vector.setInitialCapacity(capacity);
        vector.allocateNew();
      }
    }
    writer = createVectorWriter(vector);
    createVectorAccessor(vector, dicionaryVector);
  }

  public ArrowWritableColumnVector(int capacity, DataType dataType) {
    super(capacity, dataType);
    vectorCount.getAndIncrement();
    refCnt.getAndIncrement();
    String timeZoneId = SparkSchemaUtil.getLocalTimezoneID();
    List<Field> fields =
        Arrays.asList(SparkArrowUtil.toArrowField("col", dataType, true, timeZoneId));
    Schema arrowSchema = new Schema(fields);
    VectorSchemaRoot root =
        VectorSchemaRoot.create(arrowSchema, ArrowBufferAllocators.contextInstance());

    List<FieldVector> fieldVectors = root.getFieldVectors();
    vector = fieldVectors.get(0);
    vector.setInitialCapacity(capacity);
    vector.allocateNew();
    this.writer = createVectorWriter(vector);
    createVectorAccessor(vector, null);
  }

  public ValueVector getValueVector() {
    return vector;
  }

  public void setValueCount(int numRows) {
    vector.setValueCount(numRows);
  }

  private void createVectorAccessor(ValueVector vector, ValueVector dictionary) {
    if (dictionary != null) {
      if (!(vector instanceof IntVector)) {
        throw new IllegalArgumentException(
            "Expect int32 index vector. Found: " + vector.getMinorType());
      }
      IntVector index = (IntVector) vector;
      if (dictionary instanceof VarBinaryVector) {
        accessor = new DictionaryEncodedBinaryAccessor(index, (VarBinaryVector) dictionary);
      } else if (dictionary instanceof VarCharVector) {
        accessor = new DictionaryEncodedStringAccessor(index, (VarCharVector) dictionary);
      } else {
        throw new IllegalArgumentException(
            "Unrecognized index value type: " + dictionary.getMinorType());
      }
      return;
    }
    if (vector instanceof BitVector) {
      accessor = new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new IntAccessor((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroVector || vector instanceof TimeStampMicroTZVector) {
      accessor = new TimestampMicroAccessor((TimeStampVector) vector);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      accessor = new MapAccessor(mapVector);
      reallocateChildColumns(2);
      final StructVector structVector = (StructVector) mapVector.getDataVector();
      final FieldVector keyChild = structVector.getChild(MapVector.KEY_NAME);
      final FieldVector valueChild = structVector.getChild(MapVector.VALUE_NAME);
      childColumns[0] = new ArrowWritableColumnVector(keyChild, 0, structVector.size(), false);
      childColumns[1] = new ArrowWritableColumnVector(valueChild, 1, structVector.size(), false);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new ArrayAccessor(listVector);
      reallocateChildColumns(1);
      childColumns[0] =
          new ArrowWritableColumnVector(listVector.getDataVector(), 0, listVector.size(), false);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      accessor = new StructAccessor(structVector);

      reallocateChildColumns(structVector.size());
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] =
            new ArrowWritableColumnVector(
                structVector.getVectorById(i), i, structVector.size(), false);
      }
    } else if (vector instanceof NullVector) {
      NullVector nullVector = (NullVector) vector;
      accessor = new NullAccessor(nullVector);
    } else {
      throw new UnsupportedOperationException("Unsupported vector " + vector.getMinorType());
    }
  }

  // The child columns may already be created in super class's constructor
  //  org.apache.spark.sql.execution.vectorized
  //    .WritableColumnVector#WritableColumnVector(int, DataType).
  //  So we close them then create new ones.
  private void reallocateChildColumns(int width) {
    if (childColumns != null) {
      for (WritableColumnVector column : childColumns) {
        column.close();
      }
    }
    childColumns = new ArrowWritableColumnVector[width];
  }

  private ArrowVectorWriter createVectorWriter(ValueVector vector) {
    if (vector instanceof BitVector) {
      return new BooleanWriter((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      return new ByteWriter((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      return new ShortWriter((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      return new IntWriter((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return new LongWriter((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      return new FloatWriter((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return new DoubleWriter((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      return new DecimalWriter((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      return new StringWriter((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return new BinaryWriter((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new DateWriter((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroVector || vector instanceof TimeStampMicroTZVector) {
      return new TimestampMicroWriter((TimeStampVector) vector);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      StructVector entries = (StructVector) mapVector.getDataVector();
      ArrowVectorWriter keyWriter = createVectorWriter(entries.getChild(MapVector.KEY_NAME));
      ArrowVectorWriter valueWriter = createVectorWriter(entries.getChild(MapVector.VALUE_NAME));
      return new MapWriter(mapVector, entries, keyWriter, valueWriter);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      ArrowVectorWriter elementVector = createVectorWriter(listVector.getDataVector());
      return new ArrayWriter(listVector, elementVector);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      ArrowVectorWriter[] children = new ArrowVectorWriter[structVector.size()];
      for (int ordinal = 0; ordinal < structVector.size(); ordinal++) {
        children[ordinal] = createVectorWriter(structVector.getChildByOrdinal(ordinal));
      }
      return new StructWriter(structVector, children);
    } else if (vector instanceof NullVector) {
      return new NullWriter((NullVector) vector);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + vector.getMinorType());
    }
  }

  // Spilt this function out since it is the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    ValueVector vector = getValueVector();
    int curCapacity = vector.getValueCapacity();
    while (curCapacity < newCapacity) {
      vector.reAlloc();
      curCapacity = vector.getValueCapacity();
    }
  }

  @Override
  public ArrowWritableColumnVector reserveNewColumn(int capacity, DataType type) {
    return new ArrowWritableColumnVector(capacity, type);
  }

  public void retain() {
    refCnt.getAndIncrement();
  }

  public long refCnt() {
    return refCnt.get();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    if (refCnt.decrementAndGet() > 0) {
      return;
    }
    closed = true;
    vectorCount.getAndDecrement();
    super.close();
    vector.close();
    if (dictionaryVector != null) {
      dictionaryVector.close();
    }
  }

  public static String stat() {
    return "vectorCounter is " + vectorCount.get();
  }

  // `get` method in Spark `ColumnarArray` doesn't check null values and arrow vectors will throw
  // exception when we try to access a null value, so here we return `ArrowColumnarMap`
  // as a workaround.
  public MapData getMapInternal(int rowId) {
    return accessor.getMap(rowId);
  }

  // `get` method in Spark `ColumnarArray` doesn't check null values and arrow vectors will throw
  // exception when we try to access a null value, so here we return `ArrowColumnarMap`
  // as a workaround.
  public ArrayData getArrayInternal(int rowId) {
    return accessor.getArray(rowId);
  }

  // `get` method in Spark `ColumnarRow` doesn't check whether the data to get is a null value,
  // so we return `ArrowColumnarRow` as a workaround.
  public ArrowColumnarRow getStructInternal(int rowId) {
    if (isNullAt(rowId)) return null;
    ArrowWritableColumnVector[] writableColumns =
        new ArrowWritableColumnVector[childColumns.length];
    for (int i = 0; i < writableColumns.length; i++) {
      writableColumns[i] = (ArrowWritableColumnVector) childColumns[i];
    }
    return new ArrowColumnarRow(writableColumns, rowId);
  }

  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    writer.setNotNull(rowId);
  }

  @Override
  public void putNull(int rowId) {
    numNulls += 1;
    writer.setNull(rowId);
  }

  @Override
  public void putNulls(int rowId, int count) {
    numNulls += count;
    writer.setNulls(rowId, count);
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    writer.setNotNulls(rowId, count);
  }

  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    writer.setBoolean(rowId, value);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    writer.setBooleans(rowId, count, value);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    return accessor.getBooleans(rowId, count);
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    writer.setByte(rowId, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    writer.setBytes(rowId, count, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    writer.setBytes(rowId, count, src, srcIndex);
  }

  public void appendString(byte[] value, int srcIndex, int count) {
    writer.setBytes(elementsAppended, count, value, srcIndex);
    elementsAppended++;
  }

  public void appendDecimal(BigDecimal value) {
    writer.setBytes(elementsAppended, value);
    elementsAppended++;
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    return accessor.getBytes(rowId, count);
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    return UTF8String.fromBytes(getBytes(rowId, count));
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    writer.setShort(rowId, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    writer.setShorts(rowId, count, value);
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    writer.setShorts(rowId, count, src, srcIndex);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    writer.setShorts(rowId, count, src, srcIndex);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    return accessor.getShorts(rowId, count);
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    writer.setInt(rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    writer.setInts(rowId, count, value);
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    writer.setInts(rowId, count, src, srcIndex);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    writer.setInts(rowId, count, src, srcIndex);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    writer.setIntsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public int[] getInts(int rowId, int count) {
    return accessor.getInts(rowId, count);
  }

  /**
   * Returns the dictionary Id for rowId. This should only be called when the ColumnVector is
   * dictionaryIds. We have this separate method for dictionaryIds as per SPARK-16928.
   */
  @Override
  public int getDictId(int rowId) {
    assert (dictionary == null);
    return accessor.getInt(rowId);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    writer.setLong(rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    writer.setLongs(rowId, count, value);
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    writer.setLongs(rowId, count, src, srcIndex);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    writer.setLongs(rowId, count, src, srcIndex);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    writer.setLongsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    return accessor.getLongs(rowId, count);
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    writer.setFloat(rowId, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    writer.setFloats(rowId, count, value);
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    writer.setFloats(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    writer.setFloats(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {}

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    return accessor.getFloats(rowId, count);
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    writer.setDouble(rowId, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    writer.setDoubles(rowId, count, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    writer.setDoubles(rowId, count, src, srcIndex);
  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {}

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    writer.setDoubles(rowId, count, src, srcIndex);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    return accessor.getDoubles(rowId, count);
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    return accessor.getArrayLength(rowId);
  }

  @Override
  public int getArrayOffset(int rowId) {
    return accessor.getArrayOffset(rowId);
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    writer.setArray(rowId, offset, length);
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    writer.setBytes(rowId, length, value, offset);
    return length;
  }

  //
  // APIs copied from original ArrowWritableColumnVector
  //

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    if (dataType() instanceof ArrayType) {
      UTF8String ret_0 = accessor.getUTF8String(rowId);
      for (int i = 0; i < ((ArrayAccessor) accessor).getArrayLength(rowId); i++) {
        ret_0 = UTF8String.concat(ret_0, getArray(rowId).getUTF8String(i));
      }
      return ret_0;
    } else {
      return accessor.getUTF8String(rowId);
    }
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getBinary(rowId);
  }

  public void write(SpecializedGetters input, int ordinal) {
    writer.write(input, ordinal);
  }

  public void writeUnsafe(SpecializedGetters input, int ordinal) {
    writer.writeUnsafe(input, ordinal);
  }

  public void finishWrite() {
    writer.finish();
  }

  private abstract static class ArrowVectorAccessor {
    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    boolean[] getBooleans(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBytes(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    short[] getShorts(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    int[] getInts(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    long[] getLongs(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    float[] getFloats(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    double[] getDoubles(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    ArrayData getArray(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayLength(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayOffset(int rowId) {
      throw new UnsupportedOperationException();
    }

    MapData getMap(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowVectorAccessor {
    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Boolean.toString(getBoolean(rowId)));
    }
  }

  private static class ByteAccessor extends ArrowVectorAccessor {
    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Byte.toString(accessor.get(rowId)));
    }
  }

  private static class ShortAccessor extends ArrowVectorAccessor {
    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Short.toString(accessor.get(rowId)));
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {
    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Integer.toString(accessor.get(rowId)));
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {
    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Long.toString(accessor.get(rowId)));
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {
    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Float.toString(accessor.get(rowId)));
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {
    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Double.toString(accessor.get(rowId)));
    }
  }

  private static class DecimalAccessor extends ArrowVectorAccessor {
    private final DecimalVector accessor;
    int precision = 0;
    int scale = 0;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
      this.precision = vector.getPrecision();
      this.scale = vector.getScale();
    }

    @Override
    final Decimal getDecimal(int rowId, int _precision, int _scale) {
      if (isNullAt(rowId)) {
        return null;
      }
      return Decimal.apply(accessor.getObject(rowId), _precision, _scale);
    }

    final Decimal getDecimal(int rowId) {
      if (isNullAt(rowId)) {
        return null;
      }
      return Decimal.apply(accessor.getObject(rowId), this.precision, this.scale);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(getDecimal(rowId).toString());
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {
    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(
            null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class DictionaryEncodedStringAccessor extends ArrowVectorAccessor {
    private final IntVector index;
    private final VarCharVector dictionary;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    DictionaryEncodedStringAccessor(IntVector index, VarCharVector dictionary) {
      super(index);
      this.index = index;
      this.dictionary = dictionary;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      int idx = index.get(rowId);
      dictionary.get(idx, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(
            null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {
    private final VarBinaryVector accessor;
    private final NullableVarBinaryHolder stringResult = new NullableVarBinaryHolder();

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(
            null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class DictionaryEncodedBinaryAccessor extends ArrowVectorAccessor {
    private final IntVector index;
    private final VarBinaryVector dictionary;

    DictionaryEncodedBinaryAccessor(IntVector index, VarBinaryVector dictionary) {
      super(index);
      this.index = index;
      this.dictionary = dictionary;
    }

    @Override
    final byte[] getBinary(int rowId) {
      int idx = index.get(rowId);
      return dictionary.getObject(idx);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {
    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      Date jDate = DateTimeUtils.toJavaDate((accessor.get(rowId)));
      return UTF8String.fromString(jDate.toString());
    }
  }

  private static class TimestampMicroAccessor extends ArrowVectorAccessor {
    private final TimeStampVector accessor;

    TimestampMicroAccessor(TimeStampVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ArrayAccessor extends ArrowVectorAccessor {
    private final ListVector accessor;
    private final ArrowColumnVector elements;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
      this.elements = new ArrowColumnVector(vector.getDataVector());
    }

    @Override
    boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    public int getArrayLength(int rowId) {
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = accessor.getOffsetBuffer().getInt(index);
      int end = accessor.getOffsetBuffer().getInt(index + ListVector.OFFSET_WIDTH);
      return end - start;
    }

    @Override
    public int getArrayOffset(int rowId) {
      int index = rowId * ListVector.OFFSET_WIDTH;
      return accessor.getOffsetBuffer().getInt(index);
    }

    @Override
    final ArrayData getArray(int rowId) {
      return new ArrowColumnarArray(elements, getArrayOffset(rowId), getArrayLength(rowId));
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(
          "Array[" + getArrayOffset(rowId) + "-" + getArrayLength(rowId) + "]");
    }
  }

  private static class MapAccessor extends ArrowVectorAccessor {
    private final MapVector accessor;
    private final ArrowColumnVector keys;
    private final ArrowColumnVector values;

    MapAccessor(MapVector vector) {
      super(vector);
      this.accessor = vector;
      StructVector entries = (StructVector) vector.getDataVector();
      this.keys = new ArrowColumnVector(entries.getChild(MapVector.KEY_NAME));
      this.values = new ArrowColumnVector(entries.getChild(MapVector.VALUE_NAME));
    }

    @Override
    final MapData getMap(int rowId) {
      int index = rowId * MapVector.OFFSET_WIDTH;
      int offset = accessor.getOffsetBuffer().getInt(index);
      int length = accessor.getInnerValueCountAt(rowId);
      return new ArrowColumnarMap(keys, values, offset, length);
    }

    @Override
    int getArrayOffset(int rowId) {
      int index = rowId * MapVector.OFFSET_WIDTH;
      return accessor.getOffsetBuffer().getInt(index);
    }

    @Override
    int getArrayLength(int rowId) {
      return accessor.getInnerValueCountAt(rowId);
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * <p>Access struct values in a ArrowWritableColumnVector doesn't use this accessor. Instead, it
   * uses getStruct() method defined in the parent class. Any call to "get" method in this class is
   * a bug in the code.
   */
  private static class StructAccessor extends ArrowVectorAccessor {
    StructAccessor(StructVector vector) {
      super(vector);
    }
  }

  private static class NullAccessor extends ArrowVectorAccessor {
    NullAccessor(NullVector vector) {
      super(vector);
    }
  }

  /* Arrow Vector Writer */
  private abstract static class ArrowVectorWriter {
    private final ValueVector vector;

    ArrowVectorWriter(ValueVector vector) {
      this.vector = vector;
    }

    final void close() {
      vector.close();
    }

    void setNull(int rowId) {
      throw new UnsupportedOperationException();
    }

    void setNotNull(int rowId) {
      // Arrow Java library doesn't usually expose this API from its vectors. So we have to
      // allow no-op here than throwing exceptions which could fail caller. And basically it's
      // acceptable because finally Spark will set value after this method returned,
      // During which Arrow Java will set the validity buffer anyway. As if the call to
      // `setNotNull` is just deferred.
    }

    void setNulls(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    void setNotNulls(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    void setBoolean(int rowId, boolean value) {
      throw new UnsupportedOperationException();
    }

    void setBooleans(int rowId, int count, boolean value) {
      throw new UnsupportedOperationException();
    }

    void setByte(int rowId, byte value) {
      throw new UnsupportedOperationException();
    }

    void setBytes(int rowId, int count, byte value) {
      throw new UnsupportedOperationException();
    }

    void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setShort(int rowId, short value) {
      throw new UnsupportedOperationException();
    }

    void setShorts(int rowId, int count, short value) {
      throw new UnsupportedOperationException();
    }

    void setShorts(int rowId, int count, short[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setShorts(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setInt(int rowId, int value) {
      throw new UnsupportedOperationException();
    }

    void setInts(int rowId, int count, int value) {
      throw new UnsupportedOperationException();
    }

    void setInts(int rowId, int count, int[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setInts(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setLong(int rowId, long value) {
      throw new UnsupportedOperationException();
    }

    void setLongs(int rowId, int count, long value) {
      throw new UnsupportedOperationException();
    }

    void setLongs(int rowId, int count, long[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setLongs(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setFloat(int rowId, float value) {
      throw new UnsupportedOperationException();
    }

    void setFloats(int rowId, int count, float value) {
      throw new UnsupportedOperationException();
    }

    void setFloats(int rowId, int count, float[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setFloats(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setDouble(int rowId, double value) {
      throw new UnsupportedOperationException();
    }

    void setDoubles(int rowId, int count, double value) {
      throw new UnsupportedOperationException();
    }

    void setDoubles(int rowId, int count, double[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setDoubles(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setArray(int rowId, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    void setByteArray(int rowId, byte[] value, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    void appendBytes(byte[] value, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    void setBytes(int rowId, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    protected int count = 0;

    abstract void setValueNullSafe(SpecializedGetters input, int ordinal);

    abstract void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal);

    void write(SpecializedGetters input, int ordinal) {
      if (input.isNullAt(ordinal)) {
        setNull(count);
      } else {
        setValueNullSafe(input, ordinal);
      }
      count = count + 1;
    }

    void writeUnsafe(SpecializedGetters input, int ordinal) {
      if (input.isNullAt(ordinal)) {
        setNull(count);
      } else {
        unsafeSetValueNullSafe(input, ordinal);
      }
      count = count + 1;
    }

    void finish() {
      vector.setValueCount(count);
    }

    void reset() {
      vector.reset();
      count = 0;
    }
  }

  private static class BooleanWriter extends ArrowVectorWriter {
    private final BitVector writer;

    BooleanWriter(BitVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setBoolean(int rowId, boolean value) {
      writer.setSafe(rowId, value ? 1 : 0);
    }

    @Override
    final void setBooleans(int rowId, int count, boolean value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value ? 1 : 0);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      this.setBoolean(count, input.getBoolean(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getBoolean(ordinal) ? 1 : 0);
    }
  }

  private static class ByteWriter extends ArrowVectorWriter {
    private final TinyIntVector writer;

    ByteWriter(TinyIntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setByte(int rowId, byte value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setBytes(int rowId, int count, byte value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      this.setByte(count, input.getByte(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getByte(ordinal));
    }
  }

  private static class ShortWriter extends ArrowVectorWriter {
    private final SmallIntVector writer;

    ShortWriter(SmallIntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setShort(int rowId, short value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setShorts(int rowId, int count, short value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setShorts(int rowId, int count, short[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    final void setShorts(int rowId, int count, byte[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      this.setShort(count, input.getShort(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.setSafe(count, input.getShort(ordinal));
    }
  }

  private static class IntWriter extends ArrowVectorWriter {
    private final IntVector writer;

    IntWriter(IntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setInt(int rowId, int value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setInts(int rowId, int count, int value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setInts(int rowId, int count, int[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    final void setInts(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 4) {
        writer.setSafe(rowId + i, Platform.getInt(src, srcOffset));
      }
    }

    @Override
    void setIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 4) {
        int tmp = Platform.getInt(src, srcOffset);
        if (bigEndianPlatform) {
          tmp = java.lang.Integer.reverseBytes(tmp);
        }
        writer.setSafe(rowId + i, tmp);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      setInt(count, input.getInt(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getInt(ordinal));
    }
  }

  private static class LongWriter extends ArrowVectorWriter {
    private final BigIntVector writer;

    LongWriter(BigIntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setLong(int rowId, long value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setLongs(int rowId, int count, long value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setLongs(int rowId, int count, long[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    final void setLongs(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 8) {
        writer.setSafe(rowId + i, Platform.getLong(src, srcOffset));
      }
    }

    @Override
    final void setDouble(int rowId, double value) {
      long val = (long) value;
      writer.setSafe(rowId, val);
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      setLong(count, input.getLong(ordinal));
    }

    @Override
    void setLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 8) {
        long tmp = Platform.getLong(src, srcOffset);
        if (bigEndianPlatform) {
          tmp = java.lang.Long.reverseBytes(tmp);
        }
        writer.setSafe(rowId + i, tmp);
      }
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getLong(ordinal));
    }
  }

  private static class FloatWriter extends ArrowVectorWriter {
    private final Float4Vector writer;

    FloatWriter(Float4Vector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setFloat(int rowId, float value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setFloats(int rowId, int count, float value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setFloats(int rowId, int count, float[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      setFloat(count, input.getFloat(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getFloat(ordinal));
    }
  }

  private static class DoubleWriter extends ArrowVectorWriter {
    private final Float8Vector writer;

    DoubleWriter(Float8Vector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setDouble(int rowId, double value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setDoubles(int rowId, int count, double value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setDoubles(int rowId, int count, double[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      setDouble(count, input.getDouble(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getDouble(ordinal));
    }
  }

  private static class DecimalWriter extends ArrowVectorWriter {
    private final DecimalVector writer;
    private final int precision;
    private final int scale;

    DecimalWriter(DecimalVector vector) {
      super(vector);
      this.writer = vector;

      DataType dataType = SparkArrowUtil.fromArrowField(vector.getField());
      DecimalType decimalType = (DecimalType) dataType;
      this.precision = decimalType.precision();
      this.scale = decimalType.scale();
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setInt(int rowId, int value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setLong(int rowId, long value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setBytes(int rowId, BigDecimal value) {
      writer.setSafe(rowId, value);
    }

    final void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      if (count == src.length && srcIndex == 0) {
        writer.setBigEndianSafe(rowId, src);
        return;
      }
      throw new UnsupportedOperationException();
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      Decimal decimal = input.getDecimal(ordinal, precision, scale);
      if (decimal.changePrecision(precision, scale)) {
        setBytes(count, decimal.toJavaBigDecimal());
      } else {
        setNull(count);
      }
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      Decimal decimal = input.getDecimal(ordinal, precision, scale);
      if (decimal.changePrecision(precision, scale)) {
        writer.set(count, decimal.toJavaBigDecimal());
      } else {
        setNull(count);
      }
    }
  }

  private static class StringWriter extends ArrowVectorWriter {
    private final VarCharVector writer;
    private int rowId;

    StringWriter(VarCharVector vector) {
      super(vector);
      this.writer = vector;
      this.rowId = 0;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      writer.setSafe(rowId, src, srcIndex, count);
    }

    @Override
    final void appendBytes(byte[] value, int offset, int length) {
      writer.setSafe(rowId, value, offset, length);
      rowId++;
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      UTF8String value = input.getUTF8String(ordinal);
      setBytes(count, value.numBytes(), value.getBytes(), 0);
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      UTF8String value = input.getUTF8String(ordinal);
      writer.set(count, value.getBytes(), 0, value.numBytes());
    }
  }

  private static class BinaryWriter extends ArrowVectorWriter {
    private final VarBinaryVector writer;

    BinaryWriter(VarBinaryVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      writer.setSafe(rowId, src, srcIndex, count);
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      byte[] value = input.getBinary(ordinal);
      setBytes(count, value.length, value, 0);
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      byte[] value = input.getBinary(ordinal);
      writer.set(count, value, 0, value.length);
    }
  }

  private static class DateWriter extends ArrowVectorWriter {
    private final DateDayVector writer;

    DateWriter(DateDayVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setInt(int rowId, int value) {
      writer.setSafe(rowId, value);
    }

    @Override
    void setInts(int rowId, int count, int value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      setInt(count, input.getInt(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getInt(ordinal));
    }
  }

  private static class TimestampMicroWriter extends ArrowVectorWriter {
    private final TimeStampVector writer;

    TimestampMicroWriter(TimeStampVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    void setLongs(int rowId, int count, long value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    void setLong(int rowId, long value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      setLong(count, input.getLong(ordinal));
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.set(count, input.getLong(ordinal));
    }
  }

  private static class ArrayWriter extends ArrowVectorWriter {
    private final ListVector writer;
    private final ArrowVectorWriter elementWriter;

    ArrayWriter(ListVector vector, ArrowVectorWriter elementVector) {
      super(vector);
      this.writer = vector;
      this.elementWriter = elementVector;
    }

    @Override
    void setArray(int rowId, int offset, int length) {
      int index = rowId * ListVector.OFFSET_WIDTH;
      writer.getOffsetBuffer().setInt(index, offset);
      writer.getOffsetBuffer().setInt(index + ListVector.OFFSET_WIDTH, offset + length);
      writer.setNotNull(rowId);
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      ArrayData arrayData = input.getArray(ordinal);
      writer.startNewValue(count);
      for (int i = 0; i < arrayData.numElements(); ++i) {
        elementWriter.write(arrayData, i);
      }
      writer.endValue(count, arrayData.numElements());
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      ArrayData arrayData = input.getArray(ordinal);
      writer.startNewValue(count);
      // We just make sure the value buffer size allocated is enough, so the vector may
      // reallocate the validity buffer, so we use the safe interface here.
      for (int i = 0; i < arrayData.numElements(); ++i) {
        elementWriter.write(arrayData, i);
      }
      writer.endValue(count, arrayData.numElements());
    }

    @Override
    void finish() {
      super.finish();
      elementWriter.finish();
    }

    @Override
    void reset() {
      super.reset();
      elementWriter.reset();
    }
  }

  private static class StructWriter extends ArrowVectorWriter {
    private final StructVector writer;
    private final ArrowVectorWriter[] childrenWriter;

    StructWriter(StructVector vector, ArrowVectorWriter[] childrenWriter) {
      super(vector);
      this.writer = vector;
      this.childrenWriter = childrenWriter;
    }

    @Override
    void setNull(int rowId) {
      for (int i = 0; i < childrenWriter.length; ++i) {
        childrenWriter[i].setNull(rowId);
        childrenWriter[i].count += 1;
      }
      writer.setNull(rowId);
    }

    @Override
    void setNotNull(int rowId) {
      writer.setIndexDefined(rowId);
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      InternalRow struct = input.getStruct(ordinal, childrenWriter.length);
      writer.setIndexDefined(count);
      for (int i = 0; i < struct.numFields(); ++i) {
        childrenWriter[i].write(struct, i);
      }
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      InternalRow struct = input.getStruct(ordinal, childrenWriter.length);
      writer.setIndexDefined(count);
      // We just make sure the value buffer size allocated is enough, so the vector may
      // reallocate the validity buffer, so we use the safe interface here.
      for (int i = 0; i < struct.numFields(); ++i) {
        childrenWriter[i].write(struct, i);
      }
    }

    @Override
    void finish() {
      super.finish();
      Arrays.stream(childrenWriter).forEach(c -> c.finish());
    }

    @Override
    void reset() {
      super.reset();
      Arrays.stream(childrenWriter).forEach(c -> c.reset());
    }
  }

  private static class MapWriter extends ArrowVectorWriter {
    private final MapVector writer;
    private StructVector structVector;
    private final ArrowVectorWriter keyWriter;
    private final ArrowVectorWriter valueWriter;

    MapWriter(
        MapVector mapVector,
        StructVector structVector,
        ArrowVectorWriter mapWriter,
        ArrowVectorWriter valueWriter) {
      super(mapVector);
      this.writer = mapVector;
      this.structVector = structVector;
      this.keyWriter = mapWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    void setNull(int rowId) {}

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      MapData mapData = input.getMap(ordinal);
      writer.startNewValue(count);
      ArrayData keys = mapData.keyArray();
      ArrayData values = mapData.valueArray();
      for (int i = 0; i < mapData.numElements(); ++i) {
        structVector.setIndexDefined(i);
        keyWriter.write(keys, i);
        valueWriter.write(values, i);
      }
      writer.endValue(count, mapData.numElements());
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      MapData mapData = input.getMap(ordinal);
      writer.startNewValue(count);
      ArrayData keys = mapData.keyArray();
      ArrayData values = mapData.valueArray();
      // We just make sure the value buffer size allocated is enough, so the vector may
      // reallocate the validity buffer, so we use the safe interface here.
      for (int i = 0; i < mapData.numElements(); ++i) {
        keyWriter.write(keys, i);
      }
      for (int i = 0; i < mapData.numElements(); ++i) {
        valueWriter.write(values, i);
      }
      writer.endValue(count, mapData.numElements());
    }

    @Override
    void finish() {
      super.finish();
      keyWriter.finish();
      valueWriter.finish();
    }

    @Override
    void reset() {
      super.reset();
      keyWriter.reset();
      valueWriter.reset();
    }
  }

  private static class NullWriter extends ArrowVectorWriter {
    private final NullVector writer;

    NullWriter(NullVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    void setNull(int rowId) {
      writer.setValueCount(writer.getValueCount() + 1);
    }

    @Override
    void setValueNullSafe(SpecializedGetters input, int ordinal) {
      setNull(count);
    }

    @Override
    void unsafeSetValueNullSafe(SpecializedGetters input, int ordinal) {
      writer.setValueCount(writer.getValueCount() + 1);
    }
  }
}

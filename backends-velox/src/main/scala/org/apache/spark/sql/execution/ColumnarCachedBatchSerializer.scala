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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{RowToVeloxColumnarExec, VeloxColumnarToRowExec}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.ColumnarBatchSerializerJniWrapper

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Kryo.KRYO_SERIALIZER_MAX_BUFFER_SIZE
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, ExprId}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, PredicateHelper}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch}
import org.apache.spark.sql.columnar.SimpleMetricsCachedBatchSerializer
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import com.esotericsoftware.kryo.{Kryo, KryoException, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.arrow.c.ArrowSchema

import java.io.ByteArrayOutputStream
import java.lang.{Double => JDouble, Float => JFloat}
import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

import scala.util.control.NonFatal

/**
 * A Velox columnar cache batch carrying per-partition column statistics.
 *
 * `stats` follows the [[SimpleMetricsCachedBatch]] contract (SPARK-32274): per-column slots
 * `(lowerBound, upperBound, nullCount, count, sizeInBytes)`. `null` means stats are unavailable
 * (legacy V1 binary, or partition-stats SQLConf disabled); the serializer's `buildFilter` override
 * directs such batches through unchanged to avoid NPE in vanilla
 * `SimpleMetricsCachedBatchSerializer.buildFilter` on `partitionFilter.eval(null)`.
 *
 * `sizeInBytes` is the serialized blob length (Velox off-heap footprint), overriding the trait's
 * default per-column sum so cache-eviction accounting matches actual memory.
 *
 * Manual Kryo registration (Spark 4.1 doc TODO):
 * {{{
 *   spark.kryo.classesToRegister=org.apache.spark.sql.execution.CachedColumnarBatch
 * }}}
 */
@DefaultSerializer(classOf[CachedColumnarBatchKryoSerializer])
case class CachedColumnarBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    bytes: Array[Byte],
    override val stats: InternalRow,
    // Schema is carried per-batch so Kryo read path can dispatch (de)serializeStats by source
    // dataType. Nullable for V1 binary back-compat.
    schema: StructType = null)
  extends SimpleMetricsCachedBatch

/**
 * Kryo serializer for [[CachedColumnarBatch]].
 *
 * Wire layout:
 * {{{
 *   [numRows: Int]                  // single-arg writeInt = fixed 4-byte BE
 *   [sizeInBytes: Long]
 *   [bytes.length + 1: Int]         // +1 distinguishes Kryo.NULL
 *   [bytes]
 *   [hasStats: Boolean]  if true: [statsLen: Int] [statsBlob]
 *   [hasSchema: Boolean] if true: [schemaLen: Int] [schemaJsonBytes]
 * }}}
 */
class CachedColumnarBatchKryoSerializer extends KryoSerializer[CachedColumnarBatch] {

  override def write(kryo: Kryo, output: Output, batch: CachedColumnarBatch): Unit = {
    // Use the single-arg writeInt(int) overload: fixed 4-byte BE. The (int, boolean) overload
    // silently forwards to writeVarInt (1-5 bytes) and would corrupt the read path.
    output.writeInt(batch.numRows)
    output.writeLong(batch.sizeInBytes)
    require(
      batch.bytes != null,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"serialize using ${this.getClass.getName}")
    output.writeInt(batch.bytes.length + 1) // +1 distinguishes Kryo.NULL
    output.writeBytes(batch.bytes)
    if (batch.stats == null) {
      output.writeBoolean(false)
    } else {
      output.writeBoolean(true)
      val statsBytes = CachedColumnarBatchKryoSerializer.serializeStats(batch.stats, batch.schema)
      output.writeInt(statsBytes.length)
      output.writeBytes(statsBytes)
    }
    if (batch.schema == null) {
      output.writeBoolean(false)
    } else {
      output.writeBoolean(true)
      val schemaBytes = batch.schema.json.getBytes(UTF_8)
      output.writeInt(schemaBytes.length)
      output.writeBytes(schemaBytes)
    }
  }

  override def read(
      kryo: Kryo,
      input: Input,
      cls: Class[CachedColumnarBatch]): CachedColumnarBatch = {
    val maxLen = CachedColumnarBatchKryoSerializer.maxKryoBufferBytes
    val numRows = input.readInt()
    val sizeInBytes = input.readLong()
    val length = input.readInt()
    require(
      length != Kryo.NULL,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"deserialize using ${this.getClass.getName}")
    // length is the byte payload size + 1 (the +1 distinguishes Kryo.NULL on the write side).
    // Bound to spark.kryoserializer.buffer.max so a corrupt or malicious stream cannot trigger
    // a multi-GB allocation; Spark's own Kryo write path enforces this same ceiling on the
    // producing side, so any stream beyond it is already invalid.
    val payloadLen = length - 1
    require(
      payloadLen >= 0 && payloadLen.toLong <= maxLen,
      s"CachedColumnarBatch.bytes length ($payloadLen) out of bounds [0, $maxLen]; " +
        "stream is corrupt or exceeds spark.kryoserializer.buffer.max"
    )
    val bytes = new Array[Byte](payloadLen)
    input.readBytes(bytes)
    // Read the trailing hasStats marker. Catching a Buffer-underflow KryoException
    // here preserves backward compatibility with the V1 wire format (no trailing
    // hasStats / hasSchema booleans), which the existing
    // ColumnarCachedBatchKryoSuite#"V1 wire ..." test locks as a contract:
    // an absent trailing byte must read as null, not throw.
    //
    // Why a try/catch instead of `input.available() > 0 && readBoolean`:
    // Kryo `Input.available()` returns `(limit - position) + underlyingStream.available()`,
    // and the JDK `InputStream.available()` contract permits any implementation to
    // return 0 even when more data follows -- BufferedInputStream over shuffle-spill
    // / network chunk boundaries routinely does so. When the Kryo buffer is drained
    // AND the underlying stream reports 0 at the trailing-boolean byte position, the
    // probe falsely concludes EOF, skips hasStats, and the next readClassAndObject
    // interprets the stats payload (which contains the schema JSON) as a class name --
    // surfacing as `ClassNotFoundException: {"type":"struct",...}` with the stack
    // topped by `DefaultClassResolver.readName`. A try/catch on the real EOF surface
    // (Kryo "Buffer underflow") avoids the false-EOF probe while still tolerating
    // V1 wire.
    //
    // NB: avoid `val (a: T, b: U) = ...` -- Scala 2.13 erases Tuple2 generics and the
    // typed pattern match throws MatchError at runtime.
    val hasStats =
      try input.readBoolean()
      catch { case e: KryoException if isBufferUnderflow(e) => false }
    val statsAndSchema: (InternalRow, StructType) = if (hasStats) {
      val statsLen = input.readInt()
      require(
        statsLen >= 0 && statsLen.toLong <= maxLen,
        s"CachedColumnarBatch stats length ($statsLen) out of bounds [0, $maxLen]; " +
          "stream is corrupt or exceeds spark.kryoserializer.buffer.max"
      )
      val statsBytes = new Array[Byte](statsLen)
      input.readBytes(statsBytes)
      val sch = readOptionalSchema(input, maxLen)
      (CachedColumnarBatchKryoSerializer.deserializeStats(statsBytes, sch), sch)
    } else {
      (null, readOptionalSchema(input, maxLen))
    }
    CachedColumnarBatch(numRows, sizeInBytes, bytes, statsAndSchema._1, statsAndSchema._2)
  }

  // Kryo signals end-of-input by throwing KryoException with a message starting
  // with "Buffer underflow". There is no dedicated subclass, so a message-prefix
  // check is the narrowest filter we can apply without swallowing real corruption
  // (e.g. ClassNotFoundException wrapped during readClassAndObject).
  private def isBufferUnderflow(e: KryoException): Boolean = {
    val msg = e.getMessage
    msg != null && msg.startsWith("Buffer underflow")
  }

  private def readOptionalSchema(input: Input, maxLen: Long): StructType = {
    // Trailing schema marker. See readSchema above for the same V1-vs-chunked-fill rationale.
    val hasSchema =
      try input.readBoolean()
      catch { case e: KryoException if isBufferUnderflow(e) => false }
    if (!hasSchema) {
      null
    } else {
      val schemaLen = input.readInt()
      require(
        schemaLen >= 0 && schemaLen.toLong <= maxLen,
        s"CachedColumnarBatch schema length ($schemaLen) out of bounds [0, $maxLen]; " +
          "stream is corrupt or exceeds spark.kryoserializer.buffer.max"
      )
      val schemaBytes = new Array[Byte](schemaLen)
      input.readBytes(schemaBytes)
      DataType.fromJson(new String(schemaBytes, UTF_8)).asInstanceOf[StructType]
    }
  }
}

object CachedColumnarBatchKryoSerializer {
  // Defensive upper bound on any single length-prefixed field in the Kryo wire (payload bytes,
  // statsBlob, schema JSON). Tied to spark.kryoserializer.buffer.max because Kryo write itself
  // refuses to emit any single object larger than that ceiling, so any stream claiming a larger
  // field is necessarily corrupt or malicious. Falls back to the conf default (64 MiB) when no
  // SparkEnv is active (e.g. unit tests without a SparkContext).
  def maxKryoBufferBytes: Long = {
    val env = SparkEnv.get
    if (env == null) {
      KRYO_SERIALIZER_MAX_BUFFER_SIZE.defaultValue.get * 1024L * 1024L
    } else {
      env.conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE) * 1024L * 1024L
    }
  }

  // Sanity-check magic for the cpp/JVM ABI of the framed JNI return (serializeWithStats). Not a
  // version tag: a corrupt or truncated cpp emit fails fast here instead of feeding garbage into
  // length-prefix readers downstream.
  val STATS_FRAMED_MAGIC: Array[Byte] =
    Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x02.toByte)

  // V3 magic: same as V2 but last byte = 0x03.
  val STATS_FRAMED_MAGIC_V3: Array[Byte] =
    Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x03.toByte)

  private def magicHex(bytes: Array[Byte]): String = {
    if (bytes == null || bytes.length < 4) {
      "<short>"
    } else {
      f"0x${bytes(0) & 0xff}%02X${bytes(1) & 0xff}%02X" +
        f"${bytes(2) & 0xff}%02X${bytes(3) & 0xff}%02X"
    }
  }

  private[execution] def hasFrameMagic(bytes: Array[Byte], magic: Array[Byte]): Boolean = {
    bytes != null && bytes.length >= magic.length && {
      var i = 0
      while (i < magic.length) {
        if (bytes(i) != magic(i)) {
          return false
        }
        i += 1
      }
      true
    }
  }

  private def requireFrameMagic(bytes: Array[Byte], magic: Array[Byte], version: String): Unit = {
    require(
      hasFrameMagic(bytes, magic),
      s"$version framed bytes magic mismatch: expected ${magicHex(magic)}, got ${magicHex(bytes)}")
  }

  private def framedMagicVersion(framed: Array[Byte]): Int = {
    if (hasFrameMagic(framed, STATS_FRAMED_MAGIC)) {
      0x02
    } else if (hasFrameMagic(framed, STATS_FRAMED_MAGIC_V3)) {
      0x03
    } else {
      throw new IllegalArgumentException(
        s"framed bytes magic mismatch: expected ${magicHex(STATS_FRAMED_MAGIC)}(V2) or " +
          s"${magicHex(STATS_FRAMED_MAGIC_V3)}(V3), got ${magicHex(framed)}")
    }
  }

  // Per-column statsBlob layout (LE throughout, matches the cpp emitter in
  // VeloxColumnarBatchSerializer.cc):
  //
  //   [ numCols: u32 ]
  //   per col:
  //     [ supported: u8 ]
  //     [ nullCount: u32 ]
  //     [ count: u32 ]
  //     [ sizeInBytes: u64 ]
  //     if supported:
  //       [ lowerBoundLen: u32 ] [ lowerBound bytes ]
  //       [ upperBoundLen: u32 ] [ upperBound bytes ]
  //
  // The vanilla SimpleMetricsCachedBatch.stats InternalRow has 5 slots per source column in
  // order (lowerBound, upperBound, nullCount, count, sizeInBytes).
  //
  // Source dataTypes outside this allowlist are demoted to supported=0 in serializeStats
  // (the cpp side may still emit supported=1 for short-Decimal as Velox BIGINT; the JVM
  // gate prevents UnsupportedOperationException in dispatch).
  private[execution] def isDispatchable(dt: DataType): Boolean =
    dt match {
      case IntegerType | DateType | _: YearMonthIntervalType => true
      case ShortType => true
      case ByteType => true
      case LongType | _: DayTimeIntervalType | TimestampType | TimestampNTZType => true
      case d: DecimalType if d.precision <= 18 => true // short-decimal: Long unscaled
      case d: DecimalType if d.precision <= 38 => true // long-decimal: 16B LE int128
      case FloatType => true // 4B IEEE 754; NaN guard in cpp scanMinMax
      case DoubleType => true // 8B IEEE 754; NaN guard in cpp scanMinMax
      case BooleanType => true
      case s: StringType if SparkShimLoader.getSparkShims.isBinaryCollationString(s) => true
      // Non-binary collation: cpp scanMinMax byte-order disagrees with Spark's
      // collation-aware String ordering at runtime (PhysicalStringType.ordering
      // dispatches to CollationFactory.fetchCollation(id).comparator). Demote to
      // supported=0; the buildFilter wrapper strips any AND-conjunct that
      // references such columns to guarantee pass-through.
      case _ => false
    }

  // schema may be null for legacy callsites; in that case behave as BIGINT-only (V2 read path).
  private[execution] def serializeStats(
      stats: InternalRow,
      schema: StructType): Array[Byte] = {
    require(
      stats.numFields % 5 == 0,
      s"stats InternalRow numFields=${stats.numFields} must be a multiple of 5 " +
        s"(vanilla PartitionStatistics schema = 5 slots per column)"
    )
    val numCols = stats.numFields / 5
    val baos = new ByteArrayOutputStream()
    writeU32LE(baos, numCols)
    var col = 0
    while (col < numCols) {
      val base = col * 5
      val hasLower = !stats.isNullAt(base)
      val hasUpper = !stats.isNullAt(base + 1)
      val dispatchable = (schema == null) ||
        CachedColumnarBatchKryoSerializer.isDispatchable(schema(col).dataType)
      // For String, pre-compute the truncated payload so an all-0xFF carry overflow
      // can demote `supported` *before* the supported byte is written.
      val isStringCol = (schema != null) && hasLower && hasUpper &&
        schema(col).dataType.isInstanceOf[StringType]
      val stringPayload: Option[(Array[Byte], Array[Byte])] =
        if (isStringCol) {
          val loB = stats.getUTF8String(base).getBytes
          val hiB = stats.getUTF8String(base + 1).getBytes
          encodeStringBounds(loB, hiB)
        } else None
      val supported = hasLower && hasUpper && dispatchable &&
        (!isStringCol || stringPayload.isDefined)
      baos.write(if (supported) 1 else 0)
      writeU32LE(baos, if (stats.isNullAt(base + 2)) 0 else stats.getInt(base + 2))
      writeU32LE(baos, if (stats.isNullAt(base + 3)) 0 else stats.getInt(base + 3))
      writeU64LE(baos, if (stats.isNullAt(base + 4)) 0L else stats.getLong(base + 4))
      if (supported) {
        // schema==null => BIGINT-only legacy behavior. Otherwise dispatch by source dataType,
        // matching vanilla ColumnBuilder's union for the integer / long families.
        val dt: DataType =
          if (schema == null) LongType else schema(col).dataType
        dt match {
          case IntegerType | DateType | _: YearMonthIntervalType =>
            writeU32LE(baos, 4)
            writeU32LE(baos, stats.getInt(base))
            writeU32LE(baos, 4)
            writeU32LE(baos, stats.getInt(base + 1))
          case ShortType =>
            writeU32LE(baos, 2)
            writeU16LE(baos, stats.getShort(base) & 0xffff)
            writeU32LE(baos, 2)
            writeU16LE(baos, stats.getShort(base + 1) & 0xffff)
          case ByteType =>
            writeU32LE(baos, 1)
            baos.write(stats.getByte(base) & 0xff)
            writeU32LE(baos, 1)
            baos.write(stats.getByte(base + 1) & 0xff)
          case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getLong(base))
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getLong(base + 1))
          case d: DecimalType if d.precision <= 18 =>
            // short-Decimal: Long unscaled (matches Velox short-decimal physical = BIGINT).
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getDecimal(base, d.precision, d.scale).toUnscaledLong)
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getDecimal(base + 1, d.precision, d.scale).toUnscaledLong)
          case d: DecimalType if d.precision <= 38 =>
            // long-Decimal: 16B LE signed two's-complement (cpp HUGEINT int128 wire).
            val loDec = stats.getDecimal(base, d.precision, d.scale)
            val hiDec = stats.getDecimal(base + 1, d.precision, d.scale)
            writeU32LE(baos, 16)
            writeI128LE(baos, loDec.toJavaBigDecimal.unscaledValue)
            writeU32LE(baos, 16)
            writeI128LE(baos, hiDec.toJavaBigDecimal.unscaledValue)
          case FloatType =>
            writeU32LE(baos, 4)
            writeU32LE(baos, JFloat.floatToRawIntBits(stats.getFloat(base)))
            writeU32LE(baos, 4)
            writeU32LE(baos, JFloat.floatToRawIntBits(stats.getFloat(base + 1)))
          case DoubleType =>
            writeU32LE(baos, 8)
            writeU64LE(baos, JDouble.doubleToRawLongBits(stats.getDouble(base)))
            writeU32LE(baos, 8)
            writeU64LE(baos, JDouble.doubleToRawLongBits(stats.getDouble(base + 1)))
          case BooleanType =>
            writeU32LE(baos, 1)
            baos.write(if (stats.getBoolean(base)) 1 else 0)
            writeU32LE(baos, 1)
            baos.write(if (stats.getBoolean(base + 1)) 1 else 0)
          case _: StringType =>
            // Pre-validated: encodeStringBounds returned Some, otherwise we'd have demoted.
            val (lo, hi) = stringPayload.get
            writeU32LE(baos, lo.length)
            baos.write(lo)
            writeU32LE(baos, hi.length)
            baos.write(hi)
          case other =>
            throw new UnsupportedOperationException(
              s"serializeStats: dispatch for $other not implemented")
        }
      }
      col += 1
    }
    baos.toByteArray
  }

  // schema may be null for legacy callsites; in that case behave as BIGINT-only (V2 read path).
  private[execution] def deserializeStats(
      blob: Array[Byte],
      schema: StructType): InternalRow = {
    val buf = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN)
    val numCols = buf.getInt
    require(
      numCols >= 0 && numCols <= Int.MaxValue / 5,
      s"corrupt statsBlob: numCols=$numCols out of valid range")
    if (schema != null) {
      // Asymmetric: numCols > schema.length would IOB on schema(col) dispatch in
      // serializeStats/deserializeStats; that's a real corrupt-frame signal. The
      // numCols < schema.length direction is intentionally allowed -- the public
      // API is loose enough that callers (incl. the unit-test fixtures) pass an
      // EXPANDED 5-field-per-source-col schema where schema.length == numCols * 5.
      // Only the first numCols entries are ever consumed for type dispatch.
      require(
        numCols <= schema.length,
        s"corrupt statsBlob: numCols=$numCols > schema.length=${schema.length}; " +
          "likely cpp/JVM wire mismatch or truncated frame")
    }
    val row = new GenericInternalRow(numCols * 5)
    var col = 0
    while (col < numCols) {
      val base = col * 5
      val supported = buf.get()
      val nullCount = buf.getInt
      val count = buf.getInt
      val sizeInBytes = buf.getLong
      if (supported == 1) {
        val dt: DataType =
          if (schema == null) LongType else schema(col).dataType
        val lowerLen = buf.getInt
        require(
          lowerLen >= 0 && lowerLen <= STRING_BOUND_TRUNCATE_LEN,
          s"lowerLen=$lowerLen out of range [0, $STRING_BOUND_TRUNCATE_LEN] " +
            s"(likely cpp/JVM wire mismatch)"
        )
        dt match {
          case IntegerType | DateType | _: YearMonthIntervalType =>
            require(lowerLen == 4, s"Integer-family expects 4-byte lowerBound, got $lowerLen")
            row.update(base, buf.getInt)
            val upperLen = buf.getInt
            require(upperLen == 4, s"Integer-family expects 4-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getInt)
          case ShortType =>
            require(lowerLen == 2, s"ShortType expects 2-byte lowerBound, got $lowerLen")
            row.update(base, buf.getShort)
            val upperLen = buf.getInt
            require(upperLen == 2, s"ShortType expects 2-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getShort)
          case ByteType =>
            require(lowerLen == 1, s"ByteType expects 1-byte lowerBound, got $lowerLen")
            row.update(base, buf.get)
            val upperLen = buf.getInt
            require(upperLen == 1, s"ByteType expects 1-byte upperBound, got $upperLen")
            row.update(base + 1, buf.get)
          case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
            require(lowerLen == 8, s"Long-family expects 8-byte lowerBound, got $lowerLen")
            row.update(base, buf.getLong)
            val upperLen = buf.getInt
            require(upperLen == 8, s"Long-family expects 8-byte upperBound, got $upperLen")
            row.update(base + 1, buf.getLong)
          case d: DecimalType if d.precision <= 18 =>
            // Wrap as Decimal (NOT raw Long), else SpecificInternalRow.getDecimal CCEs at codegen.
            require(lowerLen == 8, s"short-Decimal expects 8-byte lowerBound, got $lowerLen")
            row.update(base, Decimal(buf.getLong, d.precision, d.scale))
            val upperLen = buf.getInt
            require(upperLen == 8, s"short-Decimal expects 8-byte upperBound, got $upperLen")
            row.update(base + 1, Decimal(buf.getLong, d.precision, d.scale))
          case d: DecimalType if d.precision <= 38 =>
            require(lowerLen == 16, s"long-Decimal expects 16-byte lowerBound, got $lowerLen")
            val loBytes = new Array[Byte](16)
            buf.get(loBytes)
            row.update(
              base,
              Decimal(new JBigDecimal(readI128LE(loBytes), d.scale), d.precision, d.scale))
            val upperLenL = buf.getInt
            require(upperLenL == 16, s"long-Decimal expects 16-byte upperBound, got $upperLenL")
            val hiBytes = new Array[Byte](16)
            buf.get(hiBytes)
            row.update(
              base + 1,
              Decimal(new JBigDecimal(readI128LE(hiBytes), d.scale), d.precision, d.scale))
          case FloatType =>
            require(lowerLen == 4, s"FloatType expects 4B lowerBound, got $lowerLen")
            row.update(base, JFloat.intBitsToFloat(buf.getInt))
            val upperLenF = buf.getInt
            require(upperLenF == 4, s"FloatType expects 4B upperBound, got $upperLenF")
            row.update(base + 1, JFloat.intBitsToFloat(buf.getInt))
          case DoubleType =>
            require(lowerLen == 8, s"DoubleType expects 8B lowerBound, got $lowerLen")
            row.update(base, JDouble.longBitsToDouble(buf.getLong))
            val upperLenD = buf.getInt
            require(upperLenD == 8, s"DoubleType expects 8B upperBound, got $upperLenD")
            row.update(base + 1, JDouble.longBitsToDouble(buf.getLong))
          case BooleanType =>
            require(lowerLen == 1, s"BooleanType expects 1B lowerBound, got $lowerLen")
            row.update(base, buf.get != 0)
            val upperLenB = buf.getInt
            require(upperLenB == 1, s"BooleanType expects 1B upperBound, got $upperLenB")
            row.update(base + 1, buf.get != 0)
          case _: StringType =>
            require(
              lowerLen >= 0 && lowerLen <= 256,
              s"StringType expects lowerBound in [0, 256], got $lowerLen")
            val loBytes = new Array[Byte](lowerLen)
            buf.get(loBytes)
            row.update(base, UTF8String.fromBytes(loBytes))
            val upperLenS = buf.getInt
            require(
              upperLenS >= 0 && upperLenS <= 256,
              s"StringType expects upperBound in [0, 256], got $upperLenS")
            val hiBytes = new Array[Byte](upperLenS)
            buf.get(hiBytes)
            row.update(base + 1, UTF8String.fromBytes(hiBytes))
          case _ =>
            // cpp may emit supported=1 for types not yet in JVM dispatch (e.g. short-Decimal as
            // Velox BIGINT). Skip both payloads using their wire-declared lengths instead of
            // crashing; the row keeps the slot null so the caller treats it as supported=false.
            buf.get(new Array[Byte](lowerLen))
            val upperSkipLen = buf.getInt
            require(
              upperSkipLen >= 0 && upperSkipLen <= STRING_BOUND_TRUNCATE_LEN,
              s"unknown-arm upperSkipLen=$upperSkipLen out of range " +
                s"[0, $STRING_BOUND_TRUNCATE_LEN] (likely cpp/JVM wire mismatch)"
            )
            buf.get(new Array[Byte](upperSkipLen))
        }
      }
      row.update(base + 2, nullCount)
      row.update(base + 3, count)
      row.update(base + 4, sizeInBytes)
      col += 1
    }
    row
  }

  private def writeU16LE(out: ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xff)
    out.write((v >>> 8) & 0xff)
  }

  private def writeU32LE(out: ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xff)
    out.write((v >>> 8) & 0xff)
    out.write((v >>> 16) & 0xff)
    out.write((v >>> 24) & 0xff)
  }

  private def writeU64LE(out: ByteArrayOutputStream, v: Long): Unit = {
    var i = 0
    while (i < 8) {
      out.write(((v >>> (8 * i)) & 0xffL).toInt)
      i += 1
    }
  }

  private def writeI64LE(out: ByteArrayOutputStream, v: Long): Unit =
    writeU64LE(out, v)

  // 16B LE signed two's-complement representation of a BigInteger. BigInteger.toByteArray()
  // returns big-endian signed minimal-width bytes; sign-extend to 16 then reverse to LE.
  private def writeI128LE(out: ByteArrayOutputStream, v: BigInteger): Unit = {
    val raw = v.toByteArray
    require(raw.length <= 16, s"BigInteger does not fit int128 (${raw.length} bytes)")
    val padded = new Array[Byte](16)
    val signByte: Byte = if (v.signum < 0) 0xff.toByte else 0x00.toByte
    var i = 0
    while (i < 16 - raw.length) {
      padded(i) = signByte
      i += 1
    }
    System.arraycopy(raw, 0, padded, 16 - raw.length, raw.length)
    var j = 0
    while (j < 8) {
      val t = padded(j)
      padded(j) = padded(15 - j)
      padded(15 - j) = t
      j += 1
    }
    out.write(padded)
  }

  private def readI128LE(le: Array[Byte]): BigInteger = {
    require(le.length == 16, s"readI128LE expects 16 bytes, got ${le.length}")
    val be = new Array[Byte](16)
    var i = 0
    while (i < 16) {
      be(i) = le(15 - i)
      i += 1
    }
    new BigInteger(be) // signed BE constructor
  }

  // Encode (lo, hi) string bounds for the wire by truncating each to 256 bytes.
  // - Lower: prefix is byte-wise lex <= original, monotonic.
  // - Upper: needs +1 carry on the truncated tail to ensure encoded >= original. If the carry
  //   propagates past byte 0 (all 256 prefix bytes were 0xFF), we cannot form a safe widening
  //   upper bound; return None so the caller demotes supported.
  private val STRING_BOUND_TRUNCATE_LEN = 256
  private def encodeStringBounds(
      loBytes: Array[Byte],
      hiBytes: Array[Byte]): Option[(Array[Byte], Array[Byte])] = {
    val loLen = math.min(loBytes.length, STRING_BOUND_TRUNCATE_LEN)
    val loEnc = Arrays.copyOf(loBytes, loLen)
    if (hiBytes.length <= STRING_BOUND_TRUNCATE_LEN) {
      Some((loEnc, Arrays.copyOf(hiBytes, hiBytes.length)))
    } else {
      val hiEnc = Arrays.copyOf(hiBytes, STRING_BOUND_TRUNCATE_LEN)
      var i = STRING_BOUND_TRUNCATE_LEN - 1
      while (i >= 0) {
        val b = (hiEnc(i) & 0xff) + 1
        if (b <= 0xff) {
          hiEnc(i) = b.toByte
          return Some((loEnc, hiEnc))
        }
        hiEnc(i) = 0.toByte
        i -= 1
      }
      None // carry overflowed past byte 0
    }
  }

  /**
   * Parse the JNI `serializeWithStats` framed return into (stats InternalRow, bytesBlob). Routes on
   * the full 4-byte magic: V2 -> 0xFECA5302, V3 -> 0xFECA5303.
   *
   * V2 layout: `[ magic: 4B ] [ statsLen: u32 LE ] [ statsBlob ] [ bytesLen: u32 LE ] [ bytesBlob
   * ]` V3 layout: `[ magic: 4B ] [ statsLen: u32 LE ] [ statsBlob ] [ numRows: u32 LE ] [ numCols:
   * u32 LE ] [ per-col ]`
   */
  private[execution] def parseFramedBytes(
      framed: Array[Byte],
      schema: StructType): (InternalRow, Array[Byte]) = {
    // V2 minimum = 4+4+4=12B; V3 minimum = 4+4+4+4=16B; use 12 for dispatcher guard.
    require(
      framed != null && framed.length >= 12,
      s"framed bytes too short: len=${if (framed == null) -1 else framed.length}")
    framedMagicVersion(framed) match {
      case 0x02 => parseV2Frame(framed, schema)
      case 0x03 => parseV3Frame(framed, schema)
    }
  }

  /** V2 parse: extract stats + pure Presto bytesBlob. */
  private def parseV2Frame(framed: Array[Byte], schema: StructType): (InternalRow, Array[Byte]) = {
    requireFrameMagic(framed, STATS_FRAMED_MAGIC, "V2")
    val buf = ByteBuffer.wrap(framed).order(ByteOrder.LITTLE_ENDIAN)
    buf.position(4) // skip magic
    val statsLen = buf.getInt
    require(
      statsLen >= 0 && statsLen <= buf.remaining() - 4,
      s"V2 framed bytes statsLen=$statsLen exceeds remaining buffer ${buf.remaining() - 4}")
    val statsBlob = new Array[Byte](statsLen)
    buf.get(statsBlob)
    val stats = deserializeStats(statsBlob, schema)
    val bytesLen = buf.getInt
    require(
      bytesLen >= 0 && bytesLen == buf.remaining(),
      s"V2 framed bytes bytesLen=$bytesLen != remaining ${buf.remaining()} (truncated or trailing)")
    val bytesBlob = new Array[Byte](bytesLen)
    buf.get(bytesBlob)
    (stats, bytesBlob)
  }

  /**
   * V3 parse: extract stats; bytes = the full V3 framed array (C++ deserializeV3 starts at magic).
   * Invariant: returned bytes[0..3] == V3 magic; C++ deserializeV3 re-validates the schema-level
   * contract (magic, statsLen, numRows/numCols, per-col bounds, schema numCols match), while this
   * JVM parser fails fast on top-level frame bounds and statsBlob/frame column-count agreement (the
   * only invariant C++ can't see, since C++ doesn't decode the statsBlob).
   */
  private def parseV3Frame(framed: Array[Byte], schema: StructType): (InternalRow, Array[Byte]) = {
    require(framed.length >= 16, s"V3 framed bytes too short (min 16B): len=${framed.length}")
    requireFrameMagic(framed, STATS_FRAMED_MAGIC_V3, "V3")
    val buf = ByteBuffer.wrap(framed).order(ByteOrder.LITTLE_ENDIAN)
    buf.position(4) // skip magic
    val statsLen = buf.getInt
    require(
      statsLen >= 0 && statsLen <= buf.remaining() - 8, // 8 = numRows(4)+numCols(4)
      s"V3 framed bytes statsLen=$statsLen invalid")
    val statsBlob = new Array[Byte](statsLen)
    buf.get(statsBlob)
    val stats = if (statsLen == 0) null else deserializeStats(statsBlob, schema)
    val numRows = buf.getInt
    require(numRows >= 0, s"V3 framed bytes numRows=$numRows invalid")
    val numCols = buf.getInt
    require(numCols >= 0, s"V3 framed bytes numCols=$numCols invalid")
    // The stats InternalRow must carry exactly 5 slots per frame column
    // (lowerBound, upperBound, nullCount, count, sizeInBytes). A mismatch means the embedded
    // statsBlob's column count disagrees with the frame's numCols -- a corrupt/mis-encoded frame
    // that would mis-align partition stats against the actual columns; fail fast here.
    require(
      stats == null || stats.numFields == numCols * 5,
      s"V3 framed bytes stats row numFields=${if (stats == null) -1 else stats.numFields} " +
        s"!= numCols*5=${numCols * 5} (statsBlob/frame column-count mismatch)"
    )
    var col = 0
    while (col < numCols) {
      require(
        buf.remaining() >= 4,
        s"V3 framed bytes truncated before colLen col=$col")
      val colLen = buf.getInt
      require(
        colLen >= 0,
        s"V3 framed bytes colLen=$colLen invalid at col=$col")
      require(
        colLen <= buf.remaining(),
        s"V3 framed bytes colBytes truncated at col=$col: colLen=$colLen " +
          s"remaining=${buf.remaining()}")
      buf.position(buf.position() + colLen)
      col += 1
    }
    require(
      buf.remaining() == 0,
      s"V3 framed bytes has trailing bytes after column payloads: trailing=${buf.remaining()}")
    // Return full framed bytes; C++ deserializeV3 will skip magic+stats and per-col.
    (stats, framed)
  }
}

/**
 * Velox columnar cache serializer. Supports column pruning; converts row-based input via
 * [[RowToVeloxColumnarExec]] and falls back to vanilla Spark serialization for unsupported schemas.
 */
class ColumnarCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer
  with PredicateHelper {
  private lazy val rowBasedCachedBatchSerializer = new DefaultCachedBatchSerializer

  private def glutenConf: GlutenConfig = GlutenConfig.get

  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  private def validateSchema(schema: Seq[Attribute]): Boolean = {
    val dt = toStructType(schema)
    validateSchema(dt)
  }

  private def validateSchema(schema: StructType): Boolean = {
    val reason = BackendsApiManager.getValidatorApiInstance.doSchemaValidate(schema)
    if (reason.isDefined) {
      logInfo(s"Columnar cache does not support schema $schema, due to ${reason.get}")
      false
    } else {
      true
    }
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    glutenConf.enableGluten && validateSchema(schema)
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = {
    glutenConf.enableGluten && validateSchema(schema)
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val localSchema = toStructType(schema)
    if (!validateSchema(localSchema)) {
      // we cannot use columnar cache here, as the `RowToColumnar` does not support this schema
      rowBasedCachedBatchSerializer.convertInternalRowToCachedBatch(
        input,
        schema,
        storageLevel,
        conf)
    } else {
      val numRows = conf.columnBatchSize
      val rddColumnarBatch = input.mapPartitions {
        it =>
          RowToVeloxColumnarExec.toColumnarBatchIterator(
            it,
            localSchema,
            numRows,
            VeloxConfig.get.veloxPreferredBatchBytes)
      }
      convertColumnarBatchToCachedBatch(rddColumnarBatch, schema, storageLevel, conf)
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (!validateSchema(cacheAttributes)) {
      // if we do not support this schema, that means we are using row-based serializer,
      // see `convertInternalRowToCachedBatch`, so fallback to vanilla Spark serializer
      rowBasedCachedBatchSerializer.convertCachedBatchToInternalRow(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      val rddColumnarBatch =
        convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      rddColumnarBatch.mapPartitions(it => VeloxColumnarToRowExec.toRowIterator(it))
    }
  }

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    input.mapPartitions {
      it =>
        val veloxBatches = it.map {
          /* Native code needs a Velox offloaded batch, making sure to offload
             if heavy batch is encountered */
          batch => VeloxColumnarBatches.ensureVeloxBatch(batch)
        }
        // Hoist per-partition-iterator constants out of the per-batch hot path:
        // schema, backend name, partition-stats conf, and the JNI wrapper are all
        // fixed for the lifetime of this iterator. Allocating them per CachedBatch
        // wastes GC in the many-small-batch case; GlutenConfig.get in particular
        // allocates a fresh GlutenConfig(SQLConf.get) on every call.
        val structSchema = StructType(
          schema.map(a => StructField(a.name, a.dataType, a.nullable)))
        val backendName = BackendsApiManager.getBackendName
        // Hoist partition-level configs: GlutenConfig.get allocates a fresh object on each call.
        val partitionStatsEnabled =
          GlutenConfig.get.getConf(GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED)
        val jni = ColumnarBatchSerializerJniWrapper.create(
          Runtimes.contextInstance(
            backendName,
            "ColumnarCachedBatchSerializer#serialize"))
        new Iterator[CachedBatch] {
          override def hasNext: Boolean = veloxBatches.hasNext

          override def next(): CachedBatch = {
            val batch = veloxBatches.next()
            val handle = ColumnarBatches.getNativeHandle(backendName, batch)
            def legacySerializeInline(): CachedBatch = {
              val unsafeBuffer = jni.serialize(handle)
              val bytes = unsafeBuffer.toByteArray
              CachedColumnarBatch(
                batch.numRows(),
                bytes.length,
                bytes,
                stats = null,
                schema = null)
            }
            def statsOrLegacySerializeInline(): CachedBatch = {
              if (partitionStatsEnabled && ColumnarCachedBatchSerializer.statsExtAvailable) {
                ColumnarCachedBatchSerializer.serializeOneBatchWithStats(
                  jni,
                  handle,
                  batch.numRows(),
                  structSchema,
                  () => legacySerializeInline())
              } else {
                legacySerializeInline()
              }
            }
            // V3 is the default cache format for Velox table cache: it stores each column
            // independently so reads can materialize only requested columns. Partition stats are
            // an optional V3 payload used for pruning, not a prerequisite for lazy reads.
            if (ColumnarCachedBatchSerializer.statsExtV3Available) {
              ColumnarCachedBatchSerializer.serializeOneBatchV3(
                jni,
                handle,
                batch.numRows(),
                structSchema,
                includeStats = partitionStatsEnabled,
                fallbackToV2OrLegacy = () => statsOrLegacySerializeInline())
            } else if (partitionStatsEnabled && ColumnarCachedBatchSerializer.statsExtAvailable) {
              // V2 stats path.
              ColumnarCachedBatchSerializer.serializeOneBatchWithStats(
                jni,
                handle,
                batch.numRows(),
                structSchema,
                () => legacySerializeInline())
            } else {
              legacySerializeInline()
            }
          }
        }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (!validateSchema(cacheAttributes)) {
      // if we do not support this schema, that means we are using row-based serializer,
      // see `convertInternalRowToCachedBatch`, so fallback to vanilla Spark serializer
      rowBasedCachedBatchSerializer.convertCachedBatchToColumnarBatch(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      // Find the ordinals and data types of the requested columns.
      val cacheExprIds = cacheAttributes.map(_.exprId)
      val cachedAttrNames =
        cacheAttributes.map(a => s"${a.name}#${a.exprId.id}").mkString("[", ",", "]")
      val requestedColumnIndices = selectedAttributes.map {
        a =>
          val idx = cacheExprIds.indexOf(a.exprId)
          require(
            idx >= 0,
            s"selected cache attribute ${a.name}#${a.exprId.id} is not present in cached " +
              s"attributes $cachedAttrNames")
          idx
      }
      val shouldSelectAttributes = cacheAttributes != selectedAttributes
      val localSchema = toStructType(cacheAttributes)
      val timezoneId = SQLConf.get.sessionLocalTimeZone
      input.mapPartitions {
        it =>
          val runtime = Runtimes.contextInstance(
            BackendsApiManager.getBackendName,
            "ColumnarCachedBatchSerializer#read")
          val jniWrapper = ColumnarBatchSerializerJniWrapper
            .create(runtime)
          val schema = SparkArrowUtil.toArrowSchema(localSchema, timezoneId)
          val arrowAlloc = ArrowBufferAllocators.contextInstance()
          val cSchema = ArrowSchema.allocateNew(arrowAlloc)
          ArrowAbiUtil.exportSchema(arrowAlloc, schema, cSchema)
          val deserializerHandle = jniWrapper
            .init(cSchema.memoryAddress())
          cSchema.close()

          Iterators
            .wrap(new Iterator[ColumnarBatch] {
              override def hasNext: Boolean = it.hasNext

              override def next(): ColumnarBatch = {
                val cachedBatch = it.next().asInstanceOf[CachedColumnarBatch]
                // V3 bytes are ALWAYS routed to deserializeWithProjection.
                // V3 framed bytes must NOT go to jni.deserialize() (expects Presto format).
                // Structural validation (magic, statsLen, numRows/numCols, per-col bounds,
                // schema numCols match) is handled by C++ deserializeV3; no JVM-side frame
                // walk before the JNI call.
                if (isV3Format(cachedBatch.bytes)) {
                  // C++ returns the requested M-column batch; LazyVector loads those columns
                  // on first access instead of eagerly decoding the full cached schema.
                  val reqIndices: Array[Int] =
                    if (cacheAttributes == selectedAttributes) null // all cols: C++ loadAll
                    else if (requestedColumnIndices.isEmpty) Array.empty[Int] // count(*): 0 cols
                    else requestedColumnIndices.toArray // projection: M cols
                  val batchHandle = jniWrapper.deserializeWithProjection(
                    deserializerHandle,
                    cachedBatch.bytes,
                    reqIndices)
                  ColumnarBatches.create(batchHandle)
                  // No ColumnarBatches.select(): C++ returns M-column batch.
                } else {
                  // V2 path (original logic).
                  val batchHandle = jniWrapper.deserialize(deserializerHandle, cachedBatch.bytes)
                  val batch = ColumnarBatches.create(batchHandle)
                  if (shouldSelectAttributes) {
                    try {
                      ColumnarBatches.select(
                        BackendsApiManager.getBackendName,
                        batch,
                        requestedColumnIndices.toArray)
                    } finally {
                      batch.close()
                    }
                  } else {
                    batch
                  }
                }
              }
            })
            .protectInvocationFlow()
            .recycleIterator {
              jniWrapper.close(deserializerHandle)
            }
            .recyclePayload(_.close())
            .create()
      }
    }
  }

  // Lazy-split iterator wrapper. stats=null batches are passed through unchanged; stats!=null
  // batches are routed to the inherited parent buildFilter for partition pruning. Without this
  // split, vanilla SimpleMetricsCachedBatchSerializer.buildFilter NPEs on
  // partitionFilter.eval(null) for non-trivial predicates -- the codegen and interpreted
  // paths both have no fallback for null stats.
  //
  // Strip every AND-conjunct that references a non-binary collation StringType attribute
  // (writer-side gate demoted those columns to supported=0; the cpp byte-order min/max
  // bytes do not agree with collation-aware String ordering at runtime, so feeding such
  // a conjunct to super.buildFilter would let the stats-bound check wrongly prune).
  // Or sub-trees are left intact; one disjunct losing stats already loses the Or anyway.
  private def stripUnsupportedConjuncts(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): Seq[Expression] = {
    val skipAttrIds: Set[ExprId] = cachedAttributes.collect {
      case a if (a.dataType match {
            case s: StringType => !SparkShimLoader.getSparkShims.isBinaryCollationString(s)
            case _ => false
          }) =>
        a.exprId
    }.toSet
    if (skipAttrIds.isEmpty) {
      predicates
    } else {
      predicates.flatMap {
        p =>
          val conjuncts = splitConjunctivePredicates(p)
          val kept = conjuncts.filterNot(
            c =>
              c.references.exists(r => skipAttrIds.contains(r.exprId)))
          if (kept.isEmpty) None else Some(kept.reduce(And))
      }
    }
  }

  /** True iff bytes starts with V3 magic (0xFE 0xCA 0x53 0x03). */
  private def isV3Format(bytes: Array[Byte]): Boolean =
    CachedColumnarBatchKryoSerializer.hasFrameMagic(
      bytes,
      CachedColumnarBatchKryoSerializer.STATS_FRAMED_MAGIC_V3)

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute])
      : (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    // cachedAttributes carries the cached relation's output ExprIds (the underlying scan
    // attributes), so ExprId-based matching is stable here -- no aliased ExprIds reach
    // this layer. Stripping is intentionally done before super.buildFilter sees the
    // predicate vector; empty filteredPredicates degrade gracefully because
    // super reduces partitionFilters with .reduceOption(And).getOrElse(Literal(true))
    // -- verified against spark-sql_2.13-4.0.1-sources CachedBatchSerializer.scala.
    val strippedPredicates = stripUnsupportedConjuncts(predicates, cachedAttributes)
    val parent = super.buildFilter(strippedPredicates, cachedAttributes)
    // Cached-column ordinals (each maps to a 5-slot group in the stats InternalRow) referenced by
    // the surviving predicates. A batch whose stats row carries a null min/max bound for any of
    // these columns must NOT be pruned by the vanilla parent: vanilla turns `col = l` / `col <= l`
    // into bound comparisons that evaluate to null on null bounds -> coerced false -> `!eval` ->
    // the batch is silently dropped (data loss). Such per-batch null bounds come from
    // data-dependent writer demotions that the schema-level stripUnsupportedConjuncts cannot see:
    // e.g. a binary-collation VARCHAR whose 256B upper-bound prefix is all 0xFF (carry overflow,
    // VeloxColumnarBatchSerializer.cc), or a dictionary-encoded numeric on the V2 fallback path.
    // Those batches are routed through unchanged, exactly like stats==null batches. Float/double
    // NaN columns no longer demote (the writer skips NaN), so they keep finite bounds and prune
    // normally.
    val referencedOrdinals: Set[Int] = {
      val refIds = strippedPredicates.flatMap(_.references.map(_.exprId)).toSet
      cachedAttributes.zipWithIndex.collect {
        case (a, i) if refIds.contains(a.exprId) => i
      }.toSet
    }
    (index, cachedBatchIterator) =>
      new Iterator[CachedBatch] {
        private val peekable = cachedBatchIterator.buffered
        private var staged: Iterator[CachedBatch] = Iterator.empty

        // Drain peekable until staged has an element ready, or peekable is empty. Idempotent:
        // safe to call from both hasNext and next.
        private def advance(): Unit = {
          while (!staged.hasNext && peekable.hasNext) {
            if (bypassPruning(peekable.head)) {
              // Pass through: do NOT feed to parent, which would NPE on null stats or wrongly
              // prune a batch on a null per-column bound.
              staged = Iterator.single(peekable.next())
            } else {
              // Feed parent a self-terminating sub-iterator covering the contiguous run of
              // prunable batches; loop afterwards in case parent prunes everything in the run.
              val runIt = new Iterator[CachedBatch] {
                override def hasNext: Boolean =
                  peekable.hasNext && !bypassPruning(peekable.head)
                override def next(): CachedBatch = peekable.next()
              }
              staged = parent(index, runIt)
            }
          }
        }

        private def statsOf(batch: CachedBatch): InternalRow = batch match {
          case ccb: CachedColumnarBatch => ccb.stats
          case smcb: SimpleMetricsCachedBatch => smcb.stats
          case _ => null
        }

        // A batch skips vanilla pruning when it has no stats at all, or when any
        // predicate-referenced column has a null lower/upper bound in this batch's stats row
        // (vanilla would otherwise prune it to null -> drop; see referencedOrdinals).
        private def bypassPruning(batch: CachedBatch): Boolean = {
          val stats = statsOf(batch)
          if (stats == null) {
            true
          } else {
            referencedOrdinals.exists {
              ci =>
                val base = ci * 5
                base + 1 < stats.numFields && (stats.isNullAt(base) || stats.isNullAt(base + 1))
            }
          }
        }

        override def hasNext: Boolean = {
          advance()
          staged.hasNext
        }
        override def next(): CachedBatch = {
          advance()
          staged.next()
        }
      }
  }
}

object ColumnarCachedBatchSerializer extends Logging {
  // Encapsulates the per-batch serializeWithStats fast path so the catch arms can
  // be exercised in unit tests with a stubbed jni wrapper. Two-arm catch:
  //   - UnsatisfiedLinkError trips the JVM-lifetime capability latch via
  //     markStatsExtUnavailable (one-way; native symbol gone for the whole JVM).
  //   - NonFatal absorbs per-batch corruption (corrupt magic, truncated frame,
  //     Kryo decode failure) without tripping the latch; the next batch retries
  //     the fast path. A separate counter (warnCorruptStatsFrame) caps the
  //     warning floor so high-throughput workloads don't drown the executor log.
  private[execution] def serializeOneBatchWithStats(
      jni: ColumnarBatchSerializerJniWrapper,
      handle: Long,
      numRows: Int,
      structSchema: StructType,
      fallbackToLegacy: () => CachedBatch): CachedBatch = {
    try {
      val framed = jni.serializeWithStats(handle)
      val (stats, bytesBlob) =
        CachedColumnarBatchKryoSerializer.parseFramedBytes(framed, structSchema)
      CachedColumnarBatch(numRows, bytesBlob.length, bytesBlob, stats, structSchema)
    } catch {
      case e: UnsatisfiedLinkError =>
        markStatsExtUnavailable(e)
        fallbackToLegacy()
      case NonFatal(e) =>
        warnCorruptStatsFrame(e)
        fallbackToLegacy()
    }
  }

  // Per-JVM cap on corrupt-frame warnings to avoid log flooding when a native
  // regression produces malformed frames batch after batch. Capability latch is
  // intentionally NOT tripped here: a corrupt frame is a per-batch event, not a
  // capability loss, and the next batch should still attempt the fast path.
  private val corruptFrameWarnCount = new java.util.concurrent.atomic.AtomicLong(0L)
  private val CORRUPT_FRAME_WARN_CAP = 100L

  def warnCorruptStatsFrame(cause: Throwable): Unit = {
    val n = corruptFrameWarnCount.incrementAndGet()
    if (n <= CORRUPT_FRAME_WARN_CAP) {
      logWarning(
        s"serializeWithStats produced a corrupt/undecodable frame for one cached batch; " +
          s"falling back to legacy serialize() for this batch (stats=null). Capability " +
          s"latch unchanged. [$n/$CORRUPT_FRAME_WARN_CAP]",
        cause
      )
      if (n == CORRUPT_FRAME_WARN_CAP) {
        logWarning(
          s"Further corrupt-frame warnings suppressed for the JVM lifetime " +
            s"(cap=$CORRUPT_FRAME_WARN_CAP reached). Capability latch remains active; " +
            s"investigate native serializeWithStats output.")
      }
    }
  }

  // Lazy capability flag for the serializeWithStats JNI symbol. A new Gluten jar paired with an
  // older libgluten.so will throw UnsatisfiedLinkError on the first invocation; the call site
  // catches it once via markStatsExtUnavailable() and we degrade to the legacy serialize() path
  // for the remainder of the JVM lifetime. Default true so the optimistic fast path is taken.
  @volatile private var statsExtAvailableFlag: Boolean = true

  def statsExtAvailable: Boolean = statsExtAvailableFlag

  def markStatsExtUnavailable(cause: Throwable): Unit = {
    if (statsExtAvailableFlag) {
      statsExtAvailableFlag = false
      logWarning(
        "serializeWithStats JNI symbol is not linked in libgluten.so; " +
          "falling back to serialize() and disabling per-partition stats for this JVM. " +
          "This typically indicates a Gluten jar / native library version mismatch.",
        cause
      )
    }
  }

  // Visible for testing: reset the capability flag so a unit test can re-exercise the
  // probe-once semantics.
  private[execution] def resetStatsExtAvailableForTesting(): Unit = {
    statsExtAvailableFlag = true
  }

  // V3 lazy deserialization support

  // Separate capability latch for the V3 JNI symbols
  // (framedSerializeV3 / framedSerializeWithStatsV3).
  @volatile private var statsExtV3AvailableFlag: Boolean = true

  def statsExtV3Available: Boolean = statsExtV3AvailableFlag

  // Benchmark-only hook used by ColumnarTableCacheLazyDeserBenchmark to materialize the old
  // eager/raw cache bytes as a baseline. This is intentionally package-private and not a user
  // configuration: V3 lazy deserialization remains the production default.
  private[execution] def withStatsExtV3AvailabilityForBenchmark[T](available: Boolean)(
      f: => T): T = synchronized {
    val previous = statsExtV3AvailableFlag
    statsExtV3AvailableFlag = available
    try {
      f
    } finally {
      statsExtV3AvailableFlag = previous
    }
  }

  def markStatsExtV3Unavailable(cause: Throwable): Unit = {
    if (statsExtV3AvailableFlag) {
      statsExtV3AvailableFlag = false
      logWarning(
        "V3 table cache serialization JNI path is unavailable; " +
          "disabling V3 per-column lazy deserialization for this JVM. " +
          "This typically indicates a Gluten jar / native library version mismatch.",
        cause
      )
    }
  }

  // V3 per-batch serialization: identical two-arm catch structure to serializeOneBatchWithStats.
  // null return from JNI = non-Velox backend; treated as one-shot latch, not corrupt frame.
  private[execution] def serializeOneBatchV3(
      jni: ColumnarBatchSerializerJniWrapper,
      handle: Long,
      numRows: Int,
      structSchema: StructType,
      includeStats: Boolean,
      fallbackToV2OrLegacy: () => CachedBatch): CachedBatch = {
    try {
      val framed =
        if (includeStats) jni.serializeWithStatsV3(handle)
        else jni.serializeV3(handle)
      if (framed == null) {
        // Non-Velox backend returns null; set latch and fall back.
        markStatsExtV3Unavailable(
          new RuntimeException("framedSerializeV3 returned null (backend not supported)"))
        return fallbackToV2OrLegacy()
      }
      val (stats, _) = CachedColumnarBatchKryoSerializer.parseFramedBytes(framed, structSchema)
      // bytes = full V3 frame (C++ deserializeV3 parses from byte 0 including magic).
      CachedColumnarBatch(
        numRows,
        framed.length,
        framed,
        stats,
        schema = if (stats == null) null else structSchema)
    } catch {
      case e: UnsatisfiedLinkError =>
        markStatsExtV3Unavailable(e)
        fallbackToV2OrLegacy()
      case NonFatal(e) =>
        warnCorruptStatsFrame(e) // count against shared corrupt-frame cap
        fallbackToV2OrLegacy()
    }
  }
}

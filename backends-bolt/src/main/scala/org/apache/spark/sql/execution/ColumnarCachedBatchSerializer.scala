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
import org.apache.gluten.columnarbatch.{BoltColumnarBatches, ColumnarBatches}
import org.apache.gluten.config.{BoltConfig, GlutenConfig}
import org.apache.gluten.execution.{BoltColumnarToRowExec, RowToBoltColumnarExec}
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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, ExprId, GenericInternalRow, PredicateHelper}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch}
import org.apache.spark.sql.columnar.SimpleMetricsCachedBatchSerializer
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import com.esotericsoftware.kryo.{DefaultSerializer, Kryo, KryoException, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.arrow.c.ArrowSchema

import java.io.ByteArrayOutputStream
import java.lang.{Double => JDouble, Float => JFloat}
import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

import scala.util.control.NonFatal

@DefaultSerializer(classOf[CachedColumnarBatchKryoSerializer])
case class CachedColumnarBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    bytes: Array[Byte],
    override val stats: InternalRow,
    schema: StructType = null)
  extends SimpleMetricsCachedBatch

class CachedColumnarBatchKryoSerializer extends KryoSerializer[CachedColumnarBatch] {

  override def write(kryo: Kryo, output: Output, batch: CachedColumnarBatch): Unit = {
    output.writeInt(batch.numRows)
    output.writeLong(batch.sizeInBytes)
    require(
      batch.bytes != null,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"serialize using ${this.getClass.getName}")
    output.writeInt(batch.bytes.length + 1)
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
    val payloadLen = length - 1
    require(
      payloadLen >= 0 && payloadLen.toLong <= maxLen,
      s"CachedColumnarBatch.bytes length ($payloadLen) out of bounds [0, $maxLen]; " +
        "stream is corrupt or exceeds spark.kryoserializer.buffer.max"
    )
    val bytes = new Array[Byte](payloadLen)
    input.readBytes(bytes)
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

  private def isBufferUnderflow(e: KryoException): Boolean = {
    val msg = e.getMessage
    msg != null && msg.startsWith("Buffer underflow")
  }

  private def readOptionalSchema(input: Input, maxLen: Long): StructType = {
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
  def maxKryoBufferBytes: Long = {
    val env = SparkEnv.get
    if (env == null) {
      KRYO_SERIALIZER_MAX_BUFFER_SIZE.defaultValue.get * 1024L * 1024L
    } else {
      env.conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE) * 1024L * 1024L
    }
  }

  val STATS_FRAMED_MAGIC: Array[Byte] =
    Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x02.toByte)

  private[execution] def isDispatchable(dt: DataType): Boolean =
    dt match {
      case IntegerType | DateType | _: YearMonthIntervalType => true
      case ShortType => true
      case ByteType => true
      case LongType | _: DayTimeIntervalType | TimestampType | TimestampNTZType => true
      case d: DecimalType if d.precision <= 18 => true
      case d: DecimalType if d.precision <= 38 => true
      case FloatType => true
      case DoubleType => true
      case BooleanType => true
      case s: StringType if SparkShimLoader.getSparkShims.isBinaryCollationString(s) => true
      case _ => false
    }

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
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getDecimal(base, d.precision, d.scale).toUnscaledLong)
            writeU32LE(baos, 8)
            writeI64LE(baos, stats.getDecimal(base + 1, d.precision, d.scale).toUnscaledLong)
          case d: DecimalType if d.precision <= 38 =>
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

  private[execution] def deserializeStats(
      blob: Array[Byte],
      schema: StructType): InternalRow = {
    val buf = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN)
    val numCols = buf.getInt
    require(
      numCols >= 0 && numCols <= Int.MaxValue / 5,
      s"corrupt statsBlob: numCols=$numCols out of valid range")
    if (schema != null) {
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
    new BigInteger(be)
  }

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
      None
    }
  }

  private[execution] def parseFramedBytes(
      framed: Array[Byte],
      schema: StructType): (InternalRow, Array[Byte]) = {
    require(
      framed != null && framed.length >= 12,
      s"framed bytes too short: len=${if (framed == null) -1 else framed.length}")
    require(
      framed(0) == STATS_FRAMED_MAGIC(0) && framed(1) == STATS_FRAMED_MAGIC(1) &&
        framed(2) == STATS_FRAMED_MAGIC(2) && framed(3) == STATS_FRAMED_MAGIC(3),
      "framed bytes magic mismatch"
    )
    val buf = ByteBuffer.wrap(framed).order(ByteOrder.LITTLE_ENDIAN)
    buf.position(4)
    val statsLen = buf.getInt
    require(
      statsLen >= 0 && statsLen <= buf.remaining() - 4,
      s"framed bytes statsLen=$statsLen exceeds remaining buffer ${buf.remaining() - 4}")
    val statsBlob = new Array[Byte](statsLen)
    buf.get(statsBlob)
    val stats = deserializeStats(statsBlob, schema)
    val bytesLen = buf.getInt
    require(
      bytesLen >= 0 && bytesLen == buf.remaining(),
      s"framed bytes bytesLen=$bytesLen != remaining ${buf.remaining()} (truncated or trailing)")
    val bytesBlob = new Array[Byte](bytesLen)
    buf.get(bytesBlob)
    (stats, bytesBlob)
  }
}

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
      rowBasedCachedBatchSerializer.convertInternalRowToCachedBatch(
        input,
        schema,
        storageLevel,
        conf)
    } else {
      val numRows = conf.columnBatchSize
      val rddColumnarBatch = input.mapPartitions {
        it =>
          RowToBoltColumnarExec.toColumnarBatchIterator(
            it,
            localSchema,
            numRows,
            BoltConfig.get.boltPreferredBatchBytes)
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
      rowBasedCachedBatchSerializer.convertCachedBatchToInternalRow(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      val rddColumnarBatch =
        convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      rddColumnarBatch.mapPartitions(it => BoltColumnarToRowExec.toRowIterator(it))
    }
  }

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    input.mapPartitions {
      it =>
        val boltBatches = it.map {
          batch => BoltColumnarBatches.ensureBoltBatch(batch)
        }
        val structSchema = StructType(
          schema.map(a => StructField(a.name, a.dataType, a.nullable)))
        val backendName = BackendsApiManager.getBackendName
        val partitionStatsEnabled =
          GlutenConfig.get.getConf(GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED)
        val jni = ColumnarBatchSerializerJniWrapper.create(
          Runtimes.contextInstance(
            backendName,
            "ColumnarCachedBatchSerializer#serialize"))
        new Iterator[CachedBatch] {
          override def hasNext: Boolean = boltBatches.hasNext

          override def next(): CachedBatch = {
            val batch = boltBatches.next()
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
        }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (!validateSchema(cacheAttributes)) {
      rowBasedCachedBatchSerializer.convertCachedBatchToColumnarBatch(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      val requestedColumnIndices = selectedAttributes.map {
        a => cacheAttributes.map(_.exprId).indexOf(a.exprId)
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
                val batchHandle =
                  jniWrapper
                    .deserialize(deserializerHandle, cachedBatch.bytes)
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

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute])
      : (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    val parent = super.buildFilter(
      stripUnsupportedConjuncts(predicates, cachedAttributes),
      cachedAttributes)
    (index, cachedBatchIterator) =>
      new Iterator[CachedBatch] {
        private val peekable = cachedBatchIterator.buffered
        private var staged: Iterator[CachedBatch] = Iterator.empty

        private def advance(): Unit = {
          while (!staged.hasNext && peekable.hasNext) {
            val stats = statsOf(peekable.head)
            if (stats == null) {
              staged = Iterator.single(peekable.next())
            } else {
              val runIt = new Iterator[CachedBatch] {
                override def hasNext: Boolean =
                  peekable.hasNext && statsOf(peekable.head) != null
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
}

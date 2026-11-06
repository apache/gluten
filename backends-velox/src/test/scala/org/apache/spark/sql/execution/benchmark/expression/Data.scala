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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.RowToVeloxColumnarExec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow, XXH64}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String

import org.apache.commons.lang3.StringUtils
import org.apache.datasketches.hll.HllSketch

import java.lang.{Double => JDouble, Long => JLong}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.time.LocalDate
import java.util.{Base64, Locale}
import java.util.Arrays

import scala.collection.mutable.HashSet

/**
 * Compile only the requested projections; no calibration rows or sketch fixtures are initialized.
 */
private[benchmark] object Data {
  import Catalog._

  private lazy val hllSketch: Array[Byte] = {
    val sketch = new HllSketch(12)
    Seq(1L, 2L, 3L).foreach(sketch.update)
    sketch.toCompactByteArray
  }

  final case class Context(
      rows: Long,
      keyCardinality: Option[Long] = None,
      seed: Long = 20260912L,
      inputPartitions: Int = 3) {
    require(inputPartitions > 0, "Input partitions must be positive")
    require(rows >= 0, "Row count must not be negative")
    val keys: Long = keyCardinality.getOrElse(if (rows == 0) 1L else rows)
    require(keys > 0, "Key cardinality must be positive")
  }

  final case class Plan(inputSchema: StructType, row: (Context, Long, Int, Int) => InternalRow)

  def materializeRows(plan: Plan, context: Context, batchSize: Int): Array[UnsafeRow] = {
    require(batchSize > 0, "Batch size must be positive")
    require(context.rows <= Int.MaxValue, "Input rows exceed in-memory array capacity")
    val encoder = UnsafeProjection.create(plan.inputSchema)
    encoder.initialize(0)
    Array.tabulate(context.rows.toInt) {
      rowId =>
        val local = rowId % batchSize
        val count = math.min(batchSize.toLong, context.rows - (rowId - local)).toInt
        val row = plan.row(context, rowId.toLong, local, count)
        require(row.numFields == plan.inputSchema.length, "Input row and schema disagree")
        encoder(row).copy()
    }
  }

  def materializeBatches(
      rows: Iterator[InternalRow],
      schema: StructType,
      batchSize: Int): Seq[ColumnarBatch] = {
    var source = rows
    val detached = new Iterator[InternalRow] {
      override def hasNext: Boolean = source.hasNext
      override def next(): InternalRow = source.next()
    }
    try {
      val inputBatches = if (schema.isEmpty) {
        detached.grouped(batchSize).map {
          group => new ColumnarBatch(Array.empty[ColumnVector], group.size)
        }
      } else {
        RowToVeloxColumnarExec.toColumnarBatchIterator(detached, schema, batchSize, Long.MaxValue)
      }
      inputBatches.zipWithIndex.map {
        case (batch, index) =>
          // The iterator recycles its previous payload when advanced.
          ColumnarBatches.retain(batch)
          TaskResources.addRecycler(s"ExpressionBenchmark input $index", 100)(batch.close())
          batch
      }.toVector
    } finally source = Iterator.empty
  }

  private val standardFields: Map[String, StructField] = Seq(
    StructField("long", LongType, nullable = true),
    StructField("int", IntegerType, nullable = true),
    StructField("string", StringType, nullable = true),
    StructField("double", DoubleType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("date", DateType, nullable = true),
    StructField("binary", BinaryType, nullable = false),
    StructField("intArray", ArrayType(IntegerType, containsNull = false), nullable = false),
    StructField(
      "struct",
      StructType(
        Seq(
          StructField("f1", LongType, nullable = false),
          StructField("f2", StringType, nullable = false))),
      nullable = false),
    StructField("boundedInt", IntegerType, nullable = false),
    StructField("stringArray", ArrayType(StringType, containsNull = false), nullable = false),
    StructField("timestampString", StringType, nullable = false),
    StructField("fractionalDouble", DoubleType, nullable = false),
    StructField("positiveDouble", DoubleType, nullable = false),
    StructField("text", StringType, nullable = false),
    StructField("base64String", StringType, nullable = false),
    StructField("hexString", StringType, nullable = false),
    StructField("jsonArrayString", StringType, nullable = false),
    StructField("jsonTupleString", StringType, nullable = false),
    StructField("mixedLongArray", ArrayType(LongType, containsNull = false), nullable = true),
    StructField("hllSketch", BinaryType, nullable = false),
    StructField("urlString", StringType, nullable = false),
    StructField("dateString", StringType, nullable = false),
    StructField(
      "intMap",
      MapType(StringType, IntegerType, valueContainsNull = true),
      nullable = false),
    StructField(
      "mapEntries",
      ArrayType(
        StructType(
          Seq(
            StructField("key", StringType, nullable = false),
            StructField("value", IntegerType, nullable = true))),
        containsNull = false),
      nullable = false
    )
  ).map(f => s"standard.${f.name}" -> f).toMap
  private val patterns = Set("none", "first", "cluster", "half-even", "last", "penultimate", "all")
  private val trimGenerators = Set("rtrimPattern", "ltrimPattern", "trimPattern")
  private val hashedGenerators = Set("hashedLong", "hashedString", "hashedBinary")
  private val fixtureGenerators = Set("arrayJoinInput", "arrayJoinDelimiter", "quarterInput")
  private val customGenerators =
    trimGenerators ++ hashedGenerators ++ fixtureGenerators ++
      Set(
        "rtrimBoundary",
        "ltrimBoundary",
        "rtrimCustom",
        "ltrimCustom",
        "nullableBoundedInt",
        "nanPayloadDouble"
      )
  private val strings = Set("standard.string", "standard.stringArray")

  private def argument(binding: Binding, name: String): Option[Argument] =
    binding.arguments.find(_._1 == name).map(_._2)

  private def integer(binding: Binding, name: String, default: Option[Int] = None): Int =
    argument(binding, name) match {
      case Some(LongArgument(value)) => Math.toIntExact(value)
      case None if default.isDefined => default.get
      case _ => throw new IllegalArgumentException(s"${binding.generator} requires integer $name")
    }

  private def pattern(binding: Binding): String = argument(binding, "pattern") match {
    case Some(StringArgument(value)) if patterns.contains(value) => value
    case other => throw new IllegalArgumentException(s"Invalid trim pattern: $other")
  }

  private def utf8(binding: Binding): Boolean = argument(binding, "utf8") match {
    case None => false
    case Some(BooleanArgument(value)) => value
    case _ => throw new IllegalArgumentException("utf8 must be boolean")
  }

  def validate(bindings: Seq[Binding]): Unit = {
    val names = HashSet.empty[String]
    bindings.foreach {
      binding =>
        require(
          standardFields.contains(binding.generator) || customGenerators.contains(
            binding.generator),
          s"Unknown generator: ${binding.generator}")
        require(
          binding.names.size == (if (binding.generator.endsWith("Custom")) 2 else 1),
          s"Wrong binding arity for ${binding.generator}")
        binding.names.foreach {
          name =>
            require(name.matches("[A-Za-z_][A-Za-z0-9_]*"), s"Invalid binding: $name")
            require(names.add(name.toLowerCase(Locale.ROOT)), s"Duplicate binding: $name")
        }
        val args = HashSet.empty[String]
        val allowed = if (fixtureGenerators.contains(binding.generator)) {
          Set("mode")
        } else if (strings.contains(binding.generator)) {
          Set("length")
        } else if (trimGenerators.contains(binding.generator)) {
          Set("length", "pattern", "nullPercent", "utf8")
        } else if (binding.generator == "hashedLong") {
          Set("column")
        } else if (binding.generator == "hashedString" || binding.generator == "hashedBinary") {
          Set("column", "length", "prefix", "nullPercent")
        } else if (binding.generator == "nullableBoundedInt") {
          Set("nullPercent")
        } else {
          Set.empty[String]
        }
        binding.arguments.foreach {
          case (name, _) =>
            require(args.add(name), s"Duplicate argument: $name")
            require(allowed.contains(name), s"Unknown argument: $name")
        }
        if (fixtureGenerators.contains(binding.generator)) fixtureMode(binding)
        if (args.contains("nullPercent")) {
          val nulls = integer(binding, "nullPercent")
          require(nulls >= 0 && nulls <= 100, "nullPercent must be between 0 and 100")
        }
        if (strings.contains(binding.generator)) {
          require(integer(binding, "length", Some(10)) >= 0, "length must not be negative")
        } else if (trimGenerators.contains(binding.generator)) {
          require(integer(binding, "length") >= 2, "Trim length must be at least two bytes")
          pattern(binding)
          utf8(binding)
        } else if (hashedGenerators.contains(binding.generator)) {
          require(integer(binding, "column") >= 0, "column must be non-negative")
          if (binding.generator == "hashedString" || binding.generator == "hashedBinary") {
            val length = integer(binding, "length")
            val prefix = integer(binding, "prefix", Some(0))
            require(length > 0, "length must be positive")
            require(prefix >= 0 && prefix < length, "prefix must be between 0 and length - 1")
          }
        }
    }
  }

  def compile(scenario: CaseDef): Plan = {
    val groupingInputs = scenario.groupingKeys.map(_.flatMap {
      sql =>
        CatalystSqlParser.parseExpression(sql).collect {
          case attribute: UnresolvedAttribute => attribute.nameParts.head.toLowerCase(Locale.ROOT)
        }
    }.toSet)
    compile(scenario.inputs, groupingInputs)
  }

  def compile(bindings: Seq[Binding]): Plan = compile(bindings, None)

  private def compile(bindings: Seq[Binding], groupingInputs: Option[Set[String]]): Plan = {

    validate(bindings)
    val projections = bindings.map {
      binding =>
        val projection = standardFields.get(binding.generator) match {
          case Some(field) =>
            val value = standard(binding.generator, integer(binding, "length", Some(10)))
            Plan(
              StructType(Seq(field.copy(name = binding.names.head))),
              (context, rowId, _, _) => InternalRow(value(context, rowId)))
          case None =>
            val input: (Context, Long, Int, Int) => InternalRow = binding.generator match {
              case "nanPayloadDouble" =>
                (_, i, _, _) => InternalRow(JDouble.longBitsToDouble(0x7ff8000000000000L | i))
              case "arrayJoinInput" =>
                val input = arrayJoinInput(fixtureMode(binding))
                (c, i, _, _) => InternalRow(input(i % c.keys))
              case "arrayJoinDelimiter" =>
                val mode = fixtureMode(binding)
                // scalastyle:off nonascii
                val delimiter = UTF8String.fromString(if (mode == "utf8") "分" else "!" + 0.toChar)
                // scalastyle:on nonascii
                (_, _, _, _) => InternalRow(delimiter)
              case "quarterInput" =>
                val mode = fixtureMode(binding)
                val dates = quarterDates(mode)
                (_, i, _, _) => {
                  if (mode == "nullable" && i % 2 == 0) InternalRow.fromSeq(Seq(null))
                  else InternalRow(dates((i % dates.length).toInt))
                }
              case "nullableBoundedInt" =>
                val nulls = integer(binding, "nullPercent", Some(0))
                (c, i, _, _) => {
                  val index = i % c.keys
                  if (nulls != 0 && Math.floorMod(XXH64.hashLong(index, c.seed), 100L) < nulls) {
                    InternalRow.fromSeq(Seq(null))
                  } else {
                    InternalRow((index % 20).toInt)
                  }
                }
              case "hashedLong" =>
                val column = integer(binding, "column")
                (c, i, _, _) => InternalRow(hashedKey(c, i, column))
              case "hashedString" =>
                val column = integer(binding, "column")
                val prefix = "x" * integer(binding, "prefix", Some(0))
                val width = integer(binding, "length") - prefix.length
                val nulls = integer(binding, "nullPercent", Some(0))
                (c, i, _, _) => {
                  if (hashedNull(c, i, column, nulls)) {
                    InternalRow.fromSeq(Seq(null))
                  } else {
                    val key =
                      StringUtils.leftPad(
                        JLong.toHexString(hashedKey(c, i, column)),
                        16,
                        '0')
                    val value = prefix + StringUtils.rightPad(key, width, key).take(width)
                    InternalRow(UTF8String.fromString(value))
                  }
                }
              case "hashedBinary" =>
                val column = integer(binding, "column")
                val length = integer(binding, "length")
                val prefix = integer(binding, "prefix", Some(0))
                val nulls = integer(binding, "nullPercent", Some(0))
                (c, i, _, _) => {
                  if (hashedNull(c, i, column, nulls)) {
                    InternalRow.fromSeq(Seq(null))
                  } else {
                    val key = hashedKey(c, i, column)
                    val value = Array.fill[Byte](length)('x'.toByte)
                    var position = prefix
                    while (position < length) {
                      value(position) = (key >>> (8 * (7 - (position - prefix) % 8))).toByte
                      position += 1
                    }
                    InternalRow(value)
                  }
                }
              case "rtrimPattern" | "ltrimPattern" | "trimPattern" =>
                val size = integer(binding, "length")
                val selected = pattern(binding)
                val nulls = integer(binding, "nullPercent", Some(0))
                val unicode = utf8(binding)
                val leadingSpaces =
                  if (binding.generator == "ltrimPattern") 2
                  else if (binding.generator == "trimPattern") 1
                  else 0
                (c, i, local, count) =>
                  trimPattern(
                    c.seed,
                    i,
                    local,
                    count,
                    size,
                    selected,
                    nulls,
                    unicode,
                    leadingSpaces)
              case "rtrimBoundary" | "ltrimBoundary" => (c, i, _, _) => trimBoundary(c.seed, i)
              case "rtrimCustom" | "ltrimCustom" => (c, i, _, _) => trimCustom(c.seed, i)
            }
            val dataType = binding.generator match {
              case "nanPayloadDouble" => DoubleType
              case "arrayJoinInput" => ArrayType(StringType, containsNull = true)
              case "quarterInput" => DateType
              case "hashedLong" => LongType
              case "hashedBinary" => BinaryType
              case "nullableBoundedInt" => IntegerType
              case _ => StringType
            }
            Plan(StructType(binding.names.map(StructField(_, dataType, nullable = true))), input)
        }
        if (
          groupingInputs.exists(
            keys => !binding.names.exists(n => keys(n.toLowerCase(Locale.ROOT))))
        ) {
          projection.copy(row = (context, rowId, local, count) =>
            projection.row(context.copy(keyCardinality = None), rowId, local, count))
        } else projection
    }
    val generate: (Context, Long, Int, Int) => InternalRow = if (projections.size == 1) {
      projections.head.row
    } else {
      (context, rowId, local, count) =>
        InternalRow.fromSeq(projections.flatMap {
          projection =>
            val row = projection.row(context, rowId, local, count)
            projection.inputSchema.indices.map(i => row.get(i, projection.inputSchema(i).dataType))
        })
    }
    Plan(
      StructType(projections.flatMap(_.inputSchema.fields)),
      (context, rowId, local, count) => {
        require(rowId >= 0 && rowId < context.rows, "rowId is outside the input context")
        require(
          local >= 0 && local < count && local.toLong <= rowId &&
            rowId - local <= context.rows - count,
          "Invalid batch-local position")
        generate(context, rowId, local, count)
      }
    )
  }

  private val arrayJoinModes = Set(
    "empty-array",
    "single-element",
    "many-short",
    "long-elements",
    "empty-elements",
    "mixed-nulls",
    "all-null",
    "utf8",
    "embedded-nul",
    "null-array")
  private val quarterModes =
    Set("quarter-boundaries", "leap-centuries", "pre-epoch", "wide-range", "nullable")

  private def fixtureMode(binding: Binding): String = {
    val modes = binding.generator match {
      case "quarterInput" => quarterModes
      case "arrayJoinDelimiter" => Set("utf8", "embedded-nul")
      case _ => arrayJoinModes
    }
    argument(binding, "mode") match {
      case Some(StringArgument(value)) if modes.contains(value) => value
      case _ => throw new IllegalArgumentException("Invalid mode for " + binding.generator)
    }
  }

  private def arrayJoinInput(mode: String): Long => GenericArrayData = mode match {
    case "null-array" => _ => null
    case "many-short" =>
      val values = Array.tabulate(10000)(n => UTF8String.fromString(n.toString))
      row =>
        new GenericArrayData(
          Array.tabulate[Any](32)(n => values(((row + n) % values.length).toInt)))
    case _ =>
      row => {
        val key = row.toString
        val values: Seq[String] = mode match {
          case "empty-array" => Seq.empty
          case "single-element" => Seq(key)
          case "long-elements" => (0 until 8).map(n => StringUtils.rightPad(s"$key:$n", 128, 'x'))
          case "empty-elements" => Seq("", key, "", "fixed", "")
          case "mixed-nulls" => Seq(null, key, null, "fixed", null)
          case "all-null" => Seq(null, null, null)
          // scalastyle:off nonascii
          case "utf8" => Seq(s"中文$key", "é😀", "")
          // scalastyle:on nonascii
          case "embedded-nul" => Seq(key + 0.toChar + "tail", "", "fixed" + 0.toChar)
        }
        new GenericArrayData(
          values.map(s => if (s == null) null else UTF8String.fromString(s)).toArray)
      }
  }

  private def quarterDates(mode: String): Array[Int] = {
    def day(year: Int, month: Int, dayOfMonth: Int): Int =
      Math.toIntExact(LocalDate.of(year, month, dayOfMonth).toEpochDay)
    mode match {
      case "quarter-boundaries" =>
        (for {
          year <- Seq(1999, 2000, 2003, 2004, 2020)
          month <- Seq(1, 4, 7, 10)
          offset <- -1 to 1
        } yield day(year, month, 1) + offset).toArray
      case "leap-centuries" =>
        (for {
          year <- Seq(1900, 2000, 2100)
          offset <- -2 to 1
        } yield day(
          year,
          3,
          1) + offset).toArray
      case "pre-epoch" =>
        (Seq(day(1582, 9, 30), day(1582, 10, 1)) ++
          (-36525 to -1 by 17)).toArray
      case "wide-range" =>
        (Seq(day(0, 1, 1), day(0, 2, 29), day(-400, 12, 31), day(-1, 4, 1)) ++
          (0 until 366).flatMap(n => Seq(Int.MinValue + n, Int.MaxValue - n))).toArray
      case "nullable" => (0 until 366).map(n => day(2000, 1, 1) + n).toArray
    }
  }

  private def hashedKey(context: Context, rowId: Long, column: Int): Long =
    XXH64.hashLong(rowId % context.keys, context.seed + column.toLong)

  private def hashedNull(context: Context, rowId: Long, column: Int, percent: Int): Boolean =
    percent == 100 || (percent != 0 &&
      Math.floorMod(
        XXH64.hashLong(rowId % context.keys, context.seed ^ (column.toLong + 0x5bd1e995L)),
        100L) < percent)

  private def pad(value: Long, length: Int): String =
    StringUtils.leftPad(value.toString, length, '0')

  private val timestampString = UTF8String.fromString("2020-01-02 03:04:05")

  private def standard(name: String, length: Int): (Context, Long) => Any = {
    // Select the generator once, not once per row; every binding sees the same context and row ID.
    name match {
      case "standard.long" => (c, i) => i % c.keys
      case "standard.int" => (c, i) => (i % c.keys).toInt
      case "standard.string" => (c, i) => UTF8String.fromString(pad(i % c.keys, length))
      case "standard.double" => (c, i) => (i % c.keys).toDouble
      case "standard.timestamp" => (c, i) => Math.multiplyExact(i % c.keys, 1000000L)
      case "standard.date" => (c, i) => Math.toIntExact(Math.addExact(18262L, i % c.keys))
      case "standard.binary" => (c, i) => pad(i % c.keys, 10).getBytes(UTF_8)
      case "standard.intArray" =>
        (c, i) =>
          new GenericArrayData(
            Array((i % c.keys).toInt, ((i + 1) % c.keys).toInt, ((i + 2) % c.keys).toInt))
      case "standard.struct" =>
        (c, i) => InternalRow(i % c.keys, UTF8String.fromString(pad(i % c.keys, 10)))
      case "standard.boundedInt" => (c, i) => (i % c.keys % 20).toInt
      case "standard.stringArray" =>
        (c, i) =>
          new GenericArrayData(
            Array(UTF8String.fromString(pad(i % c.keys, length)), UTF8String.fromString("fixed")))
      case "standard.timestampString" => (_, _) => timestampString
      case "standard.fractionalDouble" =>
        (c, i) => {
          val key = i % c.keys
          val value = key.toDouble + 0.125
          if (key % 2 == 0) value else -value
        }
      case "standard.positiveDouble" => (c, i) => (i % c.keys).toDouble + 0.125
      case "standard.text" =>
        (c, i) => UTF8String.fromString(s"spark SQL ${i % c.keys} caf${0xe9.toChar}")
      case "standard.base64String" =>
        (c, i) =>
          UTF8String.fromBytes(Base64.getEncoder.encode(pad(i % c.keys, 10).getBytes(UTF_8)))
      case "standard.hexString" =>
        (c, i) => UTF8String.fromString(JLong.toHexString(i % c.keys))
      case "standard.jsonArrayString" =>
        (c, i) => {
          val key = i % c.keys
          UTF8String.fromString(if (key % 4 == 0) "[]" else s"[$key,${key + 1},null]")
        }
      case "standard.jsonTupleString" =>
        (c, i) => {
          val key = i % c.keys
          UTF8String.fromString(s"""{"v":"${pad(key, 10)}","n":$key,"b":${key % 2 == 0}}""")
        }
      case "standard.mixedLongArray" =>
        (c, i) => {
          val key = i % c.keys
          if (key % 3 == 0) null
          else if (key % 3 == 1) new GenericArrayData(Array.emptyLongArray)
          else new GenericArrayData(Array(key, key + 1L))
        }
      case "standard.hllSketch" => (_, _) => hllSketch
      case "standard.urlString" =>
        (c, i) =>
          UTF8String.fromString(
            URLEncoder.encode(s"spark SQL/${i % c.keys} caf${0xe9.toChar}", UTF_8.name()))
      case "standard.dateString" =>
        (c, i) => UTF8String.fromString(LocalDate.ofEpochDay(18262L + i % c.keys % 3653L).toString)
      case "standard.intMap" => (c, i) => intMap(c, i)
      case "standard.mapEntries" =>
        (c, i) => {
          val map = intMap(c, i)
          new GenericArrayData((0 until map.numElements()).map {
            entry =>
              InternalRow(map.keyArray.getUTF8String(entry), map.valueArray.get(entry, IntegerType))
          }.toArray)
        }
      case other => throw new IllegalArgumentException(s"Unknown standard projection: $other")
    }
  }

  private def intMap(context: Context, rowId: Long): ArrayBasedMapData = {
    val key = rowId % context.keys
    new ArrayBasedMapData(
      new GenericArrayData(Array(UTF8String.fromString("a"), UTF8String.fromString("b"))),
      new GenericArrayData(
        Array[Any]((key % 20).toInt - 10, if (key % 3 == 0) null else (key % 20).toInt))
    )
  }

  private def trimPattern(
      seed: Long,
      index: Long,
      local: Int,
      count: Int,
      length: Int,
      pattern: String,
      nullPercent: Int,
      utf8: Boolean,
      leadingSpaces: Int): InternalRow = {
    // Penultimate retains the entire original row, including its identity and body bytes.
    val sourceLocal =
      if (pattern == "penultimate" && count > 1 && local == count - 2) count - 1
      else if (pattern == "penultimate" && count > 1 && local == count - 1) count - 2
      else local
    val global = index - local + sourceLocal
    if (
      nullPercent == 100 || (nullPercent != 0 &&
        ((global % 200) * 73 + 37) % 200 < nullPercent * 2)
    ) {
      InternalRow.fromSeq(Seq(null))
    } else {
      val trim = pattern match {
        case "none" => false
        case "first" => sourceLocal == 0
        case "cluster" => sourceLocal < 16
        case "half-even" => sourceLocal % 2 == 0
        case "last" | "penultimate" => sourceLocal == count - 1
        case "all" => true
        case other => throw new IllegalArgumentException(s"Unknown trim pattern: $other")
      }
      val spaces = if (trim) 2 else 0
      val body = length - spaces
      val offset = if (trim) leadingSpaces else 0
      val bytes = new Array[Byte](length)
      Arrays.fill(bytes, 0, offset, ' '.toByte)
      Arrays.fill(bytes, offset + body, length, ' '.toByte)
      val rowIdentity = (global ^ seed) & 0xffffffffL
      val prefix = math.min(8, body)
      var position = 0
      while (position < prefix) {
        val digit = ((rowIdentity >>> ((prefix - position - 1) * 4)) & 15).toInt
        bytes(offset + position) = (if (digit < 10) '0' + digit else 'a' + digit - 10).toByte
        position += 1
      }
      if (utf8) {
        val codePoint = 0x4e00 + Math.floorMod(global + seed, 128L).toInt
        while (position + 3 <= body) {
          bytes(offset + position) = (0xe0 | (codePoint >>> 12)).toByte
          bytes(offset + position + 1) = (0x80 | ((codePoint >>> 6) & 63)).toByte
          bytes(offset + position + 2) = (0x80 | (codePoint & 63)).toByte
          position += 3
        }
      }
      while (position < body) {
        bytes(offset + position) =
          ('a' + Math.floorMod(seed + global * 31 + position * 17, 26L).toInt).toByte
        position += 1
      }
      InternalRow(UTF8String.fromBytes(bytes))
    }
  }

  private val emptyString = UTF8String.fromString("")
  private val boundaryString = UTF8String.fromString("  " + "x" * 60 + "  ")
  private val customXy = UTF8String.fromString("xy")
  private val customSpace = UTF8String.fromString(" ")
  private val customXyValue = UTF8String.fromString("xyvaluexy")
  private val customSpaceValue = UTF8String.fromString(" value ")
  private val customValues: Array[(UTF8String, UTF8String)] = Array(
    (null, customXy),
    (customSpaceValue, null),
    (customXyValue, customXy),
    (customSpaceValue, customSpace),
    (customXyValue, customXy),
    (customSpaceValue, customSpace)
  )

  private def trimBoundary(seed: Long, index: Long): InternalRow = {
    val position = Math.floorMod(index + seed, 1000L)
    InternalRow(if (position % 101 == 0) emptyString else boundaryString)
  }

  private def trimCustom(seed: Long, index: Long): InternalRow = {
    val (value, trim) = customValues(Math.floorMod(index + seed, customValues.length.toLong).toInt)
    InternalRow(value, trim)
  }
}

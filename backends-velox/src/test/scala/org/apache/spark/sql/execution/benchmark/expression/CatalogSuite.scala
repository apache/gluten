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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.IntegerType

import com.fasterxml.jackson.databind.ObjectMapper

import java.io.ByteArrayInputStream
import java.io.FileNotFoundException
import java.io.InputStream
import java.lang.{Double => JDouble, Long => JLong}
import java.net.URLDecoder
import java.nio.charset.CharacterCodingException
import java.nio.charset.StandardCharsets.UTF_8
import java.time.LocalDate
import java.util.Arrays
import java.util.Base64

import scala.collection.mutable

// scalastyle:off nonascii
class CatalogSuite extends SparkFunSuite {
  import Catalog._

  private def table(
      inputs: String = "`input = standard.string(length = 10)`",
      sql: String = "`trim(input)`",
      description: String = "Plain description"): String =
    s"""# Inline fixture
       |
       || case | inputs | expression | description |
       || --- | --- | --- | --- |
       || example | $inputs | $sql | $description |
       |""".stripMargin

  test("aggregate value inputs are independent of key cardinality") {
    val inputs = Seq("v", "k").map(n => Binding(Seq(n), "standard.long", Seq.empty))
    val scenario = CaseDef(
      "stddev/grouped",
      inputs,
      "stddev(v)",
      "Fixture",
      SourceLocation("fixture", 1, 1),
      groupingKeys = Some(Seq("k")))
    val plan = Data.compile(scenario)
    def values(keys: Long): Seq[Long] =
      Data.materializeRows(plan, Data.Context(64, Some(keys)), 8).map(_.getLong(0)).toSeq
    assert(values(2) == values(16))
  }

  test("optional rows validates case defaults") {
    def parseRows(value: String): Option[Int] = parseMarkdown(
      "trim",
      "rows.md",
      table()
        .replace("| description |", "| description | rows |")
        .replace("| --- | --- | --- | --- |", "| --- | --- | --- | --- | --- |")
        .replace("Plain description |", s"Plain description | $value |")
    ).head.rows
    assert(parseMarkdown("trim", "rows.md", table()).head.rows.isEmpty)
    assert(parseRows("-").isEmpty)
    assert(parseRows("2000000").contains(2000000))
    Seq("0", "-1", "1.5", "2147483648", "abc").foreach {
      value => intercept[IllegalArgumentException](parseRows(value))
    }
  }

  test("Markdown preserves one complete code span including SQL punctuation and escapes") {
    val sql = """concat(`input`, 'a,b;(x)', 'quote''s', '\\d+', 'a\|b', '\\\|')"""
    val parsed = parseMarkdown("trim", "inline.md", table(sql = s"``$sql``"))
    assert(parsed.size == 1)
    assert(parsed.head.id == "trim/example")
    assert(parsed.head.sql == sql.replace("\\|", "|"))
    assert(parsed.head.inputs.head.names == Seq("input"))
    assert(parsed.head.sourceLocation.file == "inline.md")
    assert(parsed.head.sourceLocation.line == 5)
    assert(parsed.head.sourceLocation.column > 0)
  }
  test("Markdown rejects duplicate empty and invalid case IDs with physical row context") {
    Seq("example", "", "trim/example", "bad.name").foreach {
      id =>
        val extra = s"| $id | `input = standard.int()` | `input` | extra |\n"
        val error = intercept[IllegalArgumentException] {
          parseMarkdown("trim", "inline.md", table() + extra)
        }
        assert(error.getMessage.contains("inline.md:6:"))
        assert(error.getMessage.contains("case=trim/"))
        assert(error.getCause != null)
    }
  }
  test("input grammar decodes strict JSON strings without splitting their punctuation") {
    val text = """(input, trimChars) = rtrimCustom(); value = standard.int()"""
    val bindings = parseInputs(text, SourceLocation("inline.md", 5, 13), "trim/example")
    assert(bindings.map(_.names) == Seq(Seq("input", "trimChars"), Seq("value")))
    val escapedUnicode = "\\" + "u4e2d"
    val weird = """input = rtrimPattern(length = 10, pattern = "a,b;(x)\"\\""" +
      escapedUnicode + "\", utf8 = true)"
    val error = intercept[IllegalArgumentException] {
      parseInputs(weird, SourceLocation("inline.md", 5, 13), "trim/example")
    }
    assert(error.getCause != null)
    assert(error.getMessage.contains("a,b;(x)\"\\\u4e2d"))
  }
  test("Markdown rejects malformed table and non-code or mixed cells with source context") {
    val invalid = Seq(
      table(inputs = "input = standard.int()"),
      table(inputs = "prefix `input = standard.int()`"),
      table(sql = "[trim](https://example.com)"),
      table(sql = "<b>trim(input)</b>"),
      table(sql = "`trim(input)` extra"),
      table(sql = "``"),
      table(description = "<br>"),
      table(description = "[link](https://example.com)"),
      table().replace("| Plain description |", "| Plain description | extra |"),
      table().replace(" | Plain description |", " |"),
      table().replace("| `trim(input)` |", "||"),
      table() + "\n" + table(),
      table().replace("trim(input)", "trim(\ninput)"),
      table().replace("| inputs | expression |", "| expression | inputs |"),
      "not a table"
    )
    invalid.foreach {
      markdown =>
        withClue(markdown) {
          val error = intercept[IllegalArgumentException] {
            parseMarkdown("trim", "inline.md", markdown)
          }
          assert(error.getMessage.contains("inline.md:"))
          assert(error.getMessage.contains("case="))
          assert(error.getCause != null)
        }
    }
  }
  test("none inputs compile an empty schema and preserve bindings named none") {
    Seq("none", " none ").foreach {
      input =>
        val bindings = parseInputs(input, SourceLocation("inline.md", 5, 13), "rand/example")
        assert(bindings.isEmpty)
        val plan = Data.compile(bindings)
        assert(plan.inputSchema.isEmpty)
        assert(Data.materializeRows(plan, Data.Context(3), 2).forall(_.numFields == 0))
    }
    val parsed = parseMarkdown("rand", "inline.md", table(inputs = "`none`", sql = "`rand()`"))
    assert(parsed.head.inputs.isEmpty && parsed.head.sql == "rand()")
    val named =
      parseInputs("none = standard.long()", SourceLocation("inline.md", 5, 13), "rand/example")
    assert(named.head.names == Seq("none"))
  }
  test("input grammar rejects arbitrary syntax and invalid registered arguments") {
    val invalid = Seq(
      "",
      "NONE",
      "none()",
      "none; none",
      "none; input = standard.int()",
      "input = standard.int(); none",
      "input = unknown()",
      "input = standard.int(value = 1)",
      "input = standard.int(nullPercent = 0)",
      "input = hashedLong(column = 0, nullPercent = 0)",
      "input = standard.int();",
      "input = standard.int() // comment",
      "input = standard.int(); input = standard.int()",
      "input = standard.int(); INPUT = standard.int()",
      "(input, INPUT) = rtrimCustom()",
      "input = rtrimCustom()",
      "(input, chars) = standard.int()",
      "input = standard.string(length = null)",
      "input = standard.string(length = true)",
      "input = standard.string(length = \"10\")",
      "input = standard.string(length = -1)",
      "input = standard.string(length = 2147483648)",
      "input = standard.string(length = 9223372036854775808)",
      "input = standard.string(length = 10, length = 12)",
      "input = standard.string(length = 1.5)",
      "input = standard.int(foo = standard.int())",
      "input = standard.int().toString",
      "in-put = standard.int()",
      "input = rtrimPattern(length = 10, pattern = 'none')",
      "input = rtrimPattern(length = 10, pattern = \"\\q\")",
      "input = rtrimPattern(length = 10, pattern = \"none\", nullPercent = 101)",
      "input = rtrimPattern(length = 1, pattern = \"all\")",
      "input = rtrimPattern(length = 10)",
      "input = rtrimPattern(pattern = \"none\")",
      "input = rtrimPattern(length = 10, pattern = \"none\", utf8 = 1)",
      "input = standard.int(/* comment */)",
      "input = standard.int(); // comment",
      "input = standard.int(); \u4e2d = standard.int()",
      "input = standard.int(x = +1)",
      "input = standard.string(length = 01)",
      "input = standard.string(length = 1L)",
      "input = standard.string(length = 1e1)",
      "input = standard.string(length = 10,)",
      "() = standard.int()",
      "(input,) = standard.int()",
      "((input, chars)) = rtrimCustom()",
      "input = rtrimPattern(length = 10, pattern = \"none\", utf8 = True)",
      "input = rtrimPattern(length = 10, pattern = \"line\tbreak\")",
      "input = rtrimPattern(length = 10, pattern = \"\\uZZZZ\")",
      "input = rtrimPattern(length = 10, pattern = \"none\", nullPercent = -1)"
    )
    invalid.foreach {
      text =>
        withClue(text) {
          val error = intercept[IllegalArgumentException] {
            parseInputs(text, SourceLocation("inline.md", 5, 13), "trim/example")
          }
          assert(error.getMessage.contains("inline.md:5:"))
          assert(error.getMessage.contains("case=trim/example"))
          assert(error.getCause != null)
        }
    }
  }
  test("standard projections preserve calibration values correlations and nested schemas") {
    import Data._
    import org.apache.spark.sql.types._

    val names = Seq(
      "long",
      "int",
      "string",
      "double",
      "timestamp",
      "date",
      "binary",
      "intArray",
      "struct",
      "boundedInt",
      "stringArray",
      "timestampString")
    val input = names.zipWithIndex
      .map {
        case (name, i) =>
          val args = if (name == "string") "length = 3" else ""
          s"c$i = standard.$name($args)"
      }
      .mkString("; ")
    val plan = compile(parseInputs(input, SourceLocation("inline.md", 1, 1), "standard/all"))
    val expected = StructType(
      Seq(
        StructField("c0", LongType, nullable = true),
        StructField("c1", IntegerType, nullable = true),
        StructField("c2", StringType, nullable = true),
        StructField("c3", DoubleType, nullable = true),
        StructField("c4", TimestampType, nullable = true),
        StructField("c5", DateType, nullable = true),
        StructField("c6", BinaryType, nullable = false),
        StructField("c7", ArrayType(IntegerType, containsNull = false), nullable = false),
        StructField(
          "c8",
          StructType(
            Seq(
              StructField("f1", LongType, nullable = false),
              StructField("f2", StringType, nullable = false))),
          nullable = false),
        StructField("c9", IntegerType, nullable = false),
        StructField("c10", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("c11", StringType, nullable = false)
      ))
    assert(plan.inputSchema == expected)
    val context = Context(1005, Some(1001), seed = 7L)
    Seq(0L, 1L, 1000L, 1001L, 1002L).foreach {
      i =>
        val row = plan.row(context, i, 0, 1)
        val key = i % 1001
        val padded = if (key < 1000) f"$key%03d" else key.toString
        assert(row.getLong(0) == key)
        assert(row.getInt(1) == key.toInt)
        assert(row.getUTF8String(2).toString == padded)
        assert(row.getDouble(3) == key.toDouble)
        assert(row.getLong(4) == key * 1000000L)
        assert(row.getInt(5) == 18262 + key.toInt)
        assert(
          new String(row.getBinary(6), UTF_8) == f"$key%010d")
        assert(
          row.getArray(7).toIntArray().toSeq == Seq(
            key.toInt,
            ((i + 1) % 1001).toInt,
            ((i + 2) % 1001).toInt))
        assert(row.getStruct(8, 2).getLong(0) == key)
        assert(row.getStruct(8, 2).getUTF8String(1).toString == f"$key%010d")
        assert(row.getInt(9) == (key % 20).toInt)
        assert(row.getArray(10).getUTF8String(0).toString == f"$key%010d")
        assert(row.getArray(10).getUTF8String(1).toString == "fixed")
        assert(row.getUTF8String(11).toString == "2020-01-02 03:04:05")
        assert((0 until 12).forall(!row.isNullAt(_)))
        assert(row == plan.row(context.copy(seed = 999L), i, 0, 1))
    }
    assert(Context(0).keys == 1)
    assert(Context(8).keys == 8)
    assert(Context(2, Some(100)).keys == 100)
    assert(plan.row(Context(2, Some(100)), 1L, 0, 1).getArray(7).getInt(2) == 3)
    intercept[IllegalArgumentException](Context(-1))
    intercept[IllegalArgumentException](Context(1, Some(0)))
    intercept[IllegalArgumentException](plan.row(Context(0), 0L, 0, 1))
  }
  test("scalar fixtures prepare valid encodings maps and fractional inputs outside evaluation") {
    import Data._
    import org.apache.spark.sql.types._

    val nanPlan = compile(parseInputs(
      "v = nanPayloadDouble()",
      SourceLocation("inline.md", 1, 1),
      "hll/nan-payloads"))
    val nanRows = materializeRows(nanPlan, Context(16), 4)
    assert(nanRows.forall(_.getDouble(0).isNaN))
    assert(nanRows.map(row => JDouble.doubleToRawLongBits(row.getDouble(0))).toSeq ==
      (0L until 16L).map(i => 0x7ff8000000000000L | i))

    val names = Seq(
      "fractionalDouble",
      "positiveDouble",
      "text",
      "base64String",
      "hexString",
      "jsonArrayString",
      "urlString",
      "dateString",
      "intMap",
      "mapEntries")
    val bindings =
      names.zipWithIndex.map { case (name, i) => s"c$i = standard.$name()" }.mkString("; ")
    val plan = compile(parseInputs(bindings, SourceLocation("inline.md", 1, 1), "scalar/fixtures"))
    assert(plan.inputSchema.forall(!_.nullable))
    assert(
      plan.inputSchema(8).dataType == MapType(StringType, IntegerType, valueContainsNull = true))
    val context = Context(25, Some(11), seed = 7L)
    val first = materializeRows(plan, context, 4)
    val second = materializeRows(plan, context, 7)
    assert(first.toSeq == second.toSeq)
    val json = new ObjectMapper()
    first.zipWithIndex.foreach {
      case (row, i) =>
        val key = i % 11
        val number = key.toDouble + 0.125
        assert(row.getDouble(0) == (if (key % 2 == 0) number else -number))
        assert(row.getDouble(1) == number)
        assert(row.getUTF8String(2).toString == s"spark SQL $key caf\u00e9")
        val decoded = Base64.getDecoder.decode(row.getUTF8String(3).toString)
        assert(new String(decoded, UTF_8) == f"$key%010d")
        assert(JLong.parseLong(row.getUTF8String(4).toString, 16) == key)
        val array = json.readTree(row.getUTF8String(5).toString)
        assert(array.isArray && array.size() == (if (key % 4 == 0) 0 else 3))
        if (array.size() != 0) {
          assert(array.get(0).asInt() == key && array.get(1).asInt() == key + 1)
          assert(array.get(2).isNull)
        }
        assert(
          URLDecoder.decode(row.getUTF8String(6).toString, UTF_8.name()) ==
            s"spark SQL/$key caf\u00e9")
        assert(LocalDate.parse(row.getUTF8String(7).toString).toEpochDay == 18262L + key)
        val map = row.getMap(8)
        val entries = row.getArray(9)
        assert(map.numElements() == 2 && entries.numElements() == 2)
        assert(map.keyArray().getUTF8String(0).toString == "a")
        assert(map.keyArray().getUTF8String(1).toString == "b")
        assert(map.valueArray().getInt(0) == key - 10)
        assert(map.valueArray().isNullAt(1) == (key % 3 == 0))
        if (!map.valueArray().isNullAt(1)) assert(map.valueArray().getInt(1) == key)
        (0 until 2).foreach {
          entry =>
            val pair = entries.getStruct(entry, 2)
            assert(pair.getUTF8String(0) == map.keyArray().getUTF8String(entry))
            assert(pair.isNullAt(1) == map.valueArray().isNullAt(entry))
            if (!pair.isNullAt(1)) assert(pair.getInt(1) == map.valueArray().getInt(entry))
        }
    }
    assert(plan.row(Context(60), 59, 0, 1).getUTF8String(7).toString == "2020-02-29")
    assert(plan.row(Context(3654), 3653, 0, 1).getUTF8String(7).toString == "2020-01-01")
    names.foreach {
      name =>
        intercept[IllegalArgumentException] {
          parseInputs(s"input = standard.$name(length=3)", SourceLocation("inline.md", 1, 1), name)
        }
    }
  }
  test("standard projections use independent string widths and defaults regardless of order") {
    import Data._
    val source = SourceLocation("inline.md", 1, 1)
    val inputs = "input = standard.string(); values = standard.stringArray(length = 20); " +
      "needle = standard.int(); items = standard.intArray()"
    val bindings = parseInputs(inputs, source, "standard/order")
    val forward = compile(bindings)
    val reverse = compile(bindings.reverse)
    val context = Context(25, Some(7), seed = Long.MinValue)
    Seq(8L, 0L, 6L, 7L, 1L, 8L).foreach {
      i =>
        val row = forward.row(context, i, 0, 1)
        val other = reverse.row(context, i, 0, 1)
        assert(row.getUTF8String(0).toString == f"${i % 7}%010d")
        assert(row.getArray(1).getUTF8String(0).toString == f"${i % 7}%020d")
        assert(row.getInt(2) == row.getArray(3).getInt(0))
        assert(row.getUTF8String(0) == other.getUTF8String(3))
        assert(row.getArray(1) == other.getArray(2))
        assert(row.getInt(2) == other.getInt(1))
        assert(row.getArray(3) == other.getArray(0))
    }
    val defaults = compile(
      parseInputs("a = standard.string(); b = standard.stringArray()", source, "standard/defaults"))
    assert(defaults.row(Context(1), 0, 0, 1).getUTF8String(0).toString == "0000000000")
    assert(defaults.row(Context(1), 0, 0, 1).getArray(1).getUTF8String(0).toString == "0000000000")
    val zero = compile(parseInputs("input = standard.string(length = 0)", source, "standard/zero"))
    assert(zero.row(Context(1), 0, 0, 1).getUTF8String(0).toString == "0")
    val int = compile(parseInputs("input = standard.int()", source, "standard/int"))
    assert(int.row(Context(Long.MaxValue), 2147483648L, 0, 1).getInt(0) == Int.MinValue)
    intercept[IllegalArgumentException](forward.row(context, 0L, 1, 2))
    intercept[IllegalArgumentException](forward.row(context, 24L, 0, 2))
    intercept[IllegalArgumentException](forward.row(context, 0L, 0, 0))
  }
  test("hashed generators produce independent deterministic columns across batch sizes") {
    val source = SourceLocation("hashed-generators", 1, 1)
    val context = Data.Context(64, Some(16L), 7L)
    Seq(
      ("hashedLong", ""),
      ("hashedString", ", length = 8"),
      ("hashedString", ", length = 64"),
      ("hashedString", ", length = 64, prefix = 56")).foreach {
      case (generator, arguments) =>
        val inputs = (0 until 4)
          .map(column => s"c$column = $generator(column = $column$arguments)")
          .mkString("; ")
        val plan = Data.compile(parseInputs(inputs, source, "hashed/generator"))
        val values = (0 until 64).map {
          rowId =>
            val local = rowId % 7
            val row = plan.row(context, rowId, local, math.min(7, 64 - rowId + local))
            val otherBatch = plan.row(context, rowId, 0, 1)
            assert(row.numFields == 4)
            val generated = plan.inputSchema.fields.zipWithIndex.map {
              case (field, column) =>
                assert(!row.isNullAt(column))
                val value = row.get(column, field.dataType)
                assert(value == otherBatch.get(column, field.dataType))
                if (generator == "hashedString") {
                  val string = row.getUTF8String(column)
                  assert(string.numBytes() == (if (arguments.contains("length = 8")) 8 else 64))
                  if (arguments.contains("prefix")) assert(string.toString.startsWith("x" * 56))
                }
                value
            }.toSeq
            assert(generated.distinct.size == 4)
            generated
        }
        assert(values.take(16) == values.slice(16, 32))
        assert(values.take(16).distinct.size == 16)
        val changed = plan.row(context.copy(seed = 8L), 0L, 0, 1)
        assert(changed.get(0, plan.inputSchema.head.dataType) != values.head.head)
    }
  }
  test("hashed string and binary generators preserve bytes and deterministic NULL masks") {
    val source = SourceLocation("hashed-null-generators", 1, 1)
    val context = Data.Context(2000, Some(2000L), 7L)
    for (generator <- Seq("hashedString", "hashedBinary"); nullPercent <- Seq(0, 10, 50, 100)) {
      val options = s"length=64, prefix=56, nullPercent=$nullPercent"
      val inputs = (0 until 4)
        .map(column => s"c$column = $generator(column=$column, $options)")
        .mkString("; ")
      val plan = Data.compile(parseInputs(inputs, source, "hashed/null-generators"))
      val nulls = Array.fill(4)(0)
      val maskDiffers = Array.fill(4)(0)
      val observedBytes = mutable.Set.empty[Byte]
      for (i <- 0 until 2000) {
        val row = plan.row(context, i, i % 7, math.min(7, 2000 - i + i % 7))
        val otherBatch = plan.row(context, i, 0, 1)
        for (column <- 0 until 4) {
          assert(row.isNullAt(column) == otherBatch.isNullAt(column))
          if (row.isNullAt(column)) {
            nulls(column) += 1
          } else if (generator == "hashedBinary") {
            val bytes = row.getBinary(column)
            assert(bytes.length == 64 && bytes.take(56).forall(_ == 'x'.toByte))
            assert(Arrays.equals(bytes, otherBatch.getBinary(column)))
            observedBytes ++= bytes.drop(56)
          } else {
            assert(row.getUTF8String(column).numBytes() == 64)
            assert(row.getUTF8String(column) == otherBatch.getUTF8String(column))
          }
          if (row.isNullAt(column) != row.isNullAt(0)) maskDiffers(column) += 1
        }
      }
      if (nullPercent == 0) assert(nulls.forall(_ == 0))
      else if (nullPercent == 100) assert(nulls.forall(_ == 2000))
      else {
        assert(nulls.forall(count => math.abs(count.toDouble / 2000 - nullPercent / 100.0) < 0.05))
        assert(maskDiffers.drop(1).forall(_ > 0))
      }
      if (generator == "hashedBinary" && nullPercent != 100) {
        assert(Seq(0.toByte, 0x80.toByte, 0xff.toByte).forall(observedBytes.contains))
      }
    }
    Seq(
      "a = hashedBinary(column=0, length=8, nullPercent=-1)",
      "a = hashedString(column=0, length=8, nullPercent=101)",
      "a = hashedBinary(column=0, length=8, prefix=8)"
    ).foreach {
      text =>
        intercept[IllegalArgumentException] {
          parseInputs(text, source, "hashed/invalid-null-generator")
        }
    }
  }
  test("bounded integer NULL masks are deterministic and batch independent") {
    val source = SourceLocation("bounded-input", 1, 1)
    val context = Data.Context(2101, Some(101L), 7L)
    for (percent <- Seq(0, 50, 100)) {
      val plan = Data.compile(
        parseInputs(s"x = nullableBoundedInt(nullPercent=$percent)", source, "bounded/nullable"))
      assert(plan.inputSchema.head.dataType == IntegerType)
      assert(plan.inputSchema.head.nullable)
      var nulls = 0
      var seedChanges = 0
      for (i <- 0 until 2000) {
        val row = plan.row(context, i, i % 7, 7)
        val otherBatch = plan.row(context, i, 0, 1)
        val repeated = plan.row(context, i + 101L, 0, 1)
        val otherSeed = plan.row(context.copy(seed = 8L), i, 0, 1)
        assert(row.isNullAt(0) == otherBatch.isNullAt(0))
        assert(row.isNullAt(0) == repeated.isNullAt(0))
        if (row.isNullAt(0)) nulls += 1
        else {
          assert(row.getInt(0) == (i % 101 % 20))
          assert(row.getInt(0) == otherBatch.getInt(0))
          assert(row.getInt(0) == repeated.getInt(0))
        }
        if (row.isNullAt(0) != otherSeed.isNullAt(0)) seedChanges += 1
      }
      if (percent == 0) assert(nulls == 0)
      else if (percent == 100) assert(nulls == 2000)
      else {
        assert(nulls > 700 && nulls < 1300)
        assert(seedChanges > 0)
      }
    }
    Seq(-1, 101).foreach {
      percent =>
        intercept[IllegalArgumentException] {
          parseInputs(s"x = nullableBoundedInt(nullPercent=$percent)", source, "bounded/invalid")
        }
    }
  }
  test("single scalar and tuple bindings match merged rows and retain position validation") {
    import Data._
    Seq(
      "input = standard.int()",
      "(input, chars) = rtrimCustom()",
      "(input, chars) = ltrimCustom()").foreach {
      text =>
        val bindings = parseInputs(text, SourceLocation("inline.md", 1, 1), "single/binding")
        val single = compile(bindings)
        val merged = compile(bindings :+ Binding(Seq("extra"), "standard.long", Nil))
        assert(single.inputSchema.fields.toSeq == merged.inputSchema.fields.toSeq.dropRight(1))
        val context = Context(10, seed = 0L)
        (0 until 10).foreach {
          i =>
            val local = i % 4
            val count = math.min(4, 10 - i + local)
            val row = single.row(context, i, local, count)
            val combined = merged.row(context, i, local, count)
            assert(row.numFields == single.inputSchema.length)
            single.inputSchema.fields.zipWithIndex.foreach {
              case (field, ordinal) =>
                assert(row.get(ordinal, field.dataType) == combined.get(ordinal, field.dataType))
            }
            assert(combined.getLong(row.numFields) == i.toLong)
        }
        Seq(single, merged).foreach {
          plan =>
            Seq((-1L, 0, 1), (10L, 0, 1), (0L, 1, 2), (9L, 0, 2), (0L, 0, 0)).foreach {
              case (id, local, count) =>
                intercept[IllegalArgumentException](plan.row(context, id, local, count))
            }
        }
    }
  }
  private val seed = 20260912L

  private lazy val trimCases = Catalog.load().map(c => c.id -> c).toMap

  private def trimPlan(id: String): Data.Plan = Data.compile(trimCases(id).inputs)

  private def rows(
      plan: Data.Plan,
      count: Int,
      batch: Int,
      runSeed: Long = seed): Seq[InternalRow] = {
    val context = Data.Context(count, seed = runSeed)
    (0 until count).map {
      i =>
        val local = i % batch
        val row = plan.row(context, i.toLong, local, math.min(batch, count - (i - local)))
        assert(row.numFields == plan.inputSchema.length)
        assert(
          plan.inputSchema.indices.forall(i => plan.inputSchema(i).nullable || !row.isNullAt(i)))
        row
    }
  }

  test("trim golden values preserve NULL sampling UTF8 and whole-row penultimate swaps") {
    Seq("rtrim", "ltrim").foreach {
      direction =>
        val plain = rows(trimPlan(s"$direction/l10-none"), 300, 7).map(_.getUTF8String(0).toString)
        assert(plain.take(4) == Seq("01352830ct", "01352831hy", "01352832md", "01352833ri"))
        assert(plain.distinct.size == 300)
        assert(
          rows(trimPlan(s"$direction/identity-l10"), 300, 16)
            .map(_.getUTF8String(0).toString) == plain)
        assert(
          rows(trimPlan(s"$direction/l10-none"), 300, 7, seed + 1)
            .map(_.getUTF8String(0).toString) != plain)
        val nullRows = rows(trimPlan(s"$direction/l64-null50"), 200, 200)
        val missing = nullRows.indices.filter(nullRows(_).isNullAt(0))
        assert(missing.take(11) == Seq(0, 3, 5, 6, 8, 9, 11, 14, 16, 17, 19))
        assert(missing == nullRows.indices.filter(i => ((i % 200) * 73 + 37) % 200 < 100))
        assert(missing.size == 100 && missing.count(_ % 2 == 0) == 50)
        val utf = rows(trimPlan(s"$direction/l64-utf8-half-even"), 2, 2)
          .map(_.getUTF8String(0).toString)
        val body = "01352830" + "\u4e30" * 18
        assert(
          utf == Seq(
            if (direction == "rtrim") body + "  " else "  " + body,
            "01352831" + "\u4e31" * 18 + "pg"))
        Seq((1, 1), (2, 2), (17, 8), (16640, 10240)).foreach {
          case (count, batch) =>
            val last = rows(trimPlan(s"$direction/l256-last"), count, batch)
            val swapped = rows(trimPlan(s"$direction/l256-penultimate"), count, batch)
            last.grouped(batch).zip(swapped.grouped(batch)).foreach {
              case (before, after) =>
                assert(
                  after == (if (before.size < 2) before
                            else before.dropRight(2) ++ Seq(before.last, before(before.size - 2))))
            }
        }
        val boundary = if (direction == "rtrim") "leading-boundary" else "trailing-boundary"
        val edges = rows(trimPlan(s"$direction/$boundary"), 1000, 17)
        assert(edges.count(_.getUTF8String(0).numBytes() == 0) == 10)
        assert(
          edges
            .filter(_.getUTF8String(0).numBytes() != 0)
            .forall(_.getUTF8String(0).toString == "  " + "x" * 60 + "  "))
        val custom = rows(trimPlan(s"$direction/custom"), 6, 2, 0L)
        assert(custom.head.isNullAt(0) && !custom.head.isNullAt(1))
        assert(!custom(1).isNullAt(0) && custom(1).isNullAt(1))
        assert(custom(2).getUTF8String(0).toString == "xyvaluexy")
        assert(custom(2).getUTF8String(1).toString == "xy")
    }
  }
  test("loader preserves file-based unique IDs and rejects missing resources and malformed UTF8") {
    def resources(values: Map[String, Array[Byte]]): ClassLoader = new ClassLoader(null) {
      override def getResourceAsStream(name: String): InputStream =
        values.get(name).map(new ByteArrayInputStream(_)).orNull
    }
    val missing = intercept[IllegalArgumentException] {
      load(resources(Map("test/index.txt" -> "trim.md\n".getBytes(UTF_8))), "test")
    }
    assert(missing.getMessage.contains("test/trim.md"))
    assert(missing.getCause.isInstanceOf[FileNotFoundException])
    val utf8 = intercept[IllegalArgumentException] {
      load(resources(Map("test/index.txt" -> Array(0xc3.toByte, 0x28.toByte))), "test")
    }
    assert(utf8.getCause.isInstanceOf[CharacterCodingException])
    val aggregates = load(root = "expression-benchmark/aggregate", aggregate = true)
    assert(aggregates.forall {
      scenario =>
        scenario.id.takeWhile(_ != '/') ==
          scenario.sourceLocation.file.split("/").last.stripSuffix(".md")
    })
    val catalog = load() ++ aggregates
    assert(catalog.map(_.id).distinct.size == catalog.size)
  }

  test("aggregate tables carry explicit SQL grouping without restricting case names") {
    val table = "| case | inputs | expression | description | grouping |\n" +
      "| --- | --- | --- | --- | --- |\n" +
      "| long-g256 | `id = standard.long()` | `sum(id)` | grouped | `pmod(id, 256)` |\n" +
      "| all | `id = standard.long()` | `sum(id)` | global | - |\n"
    val cases = parseMarkdown("sum", "aggregate.md", table, aggregate = true)
    assert(cases.head.groupingKeys.contains(Seq("pmod(id, 256)")))
    assert(cases(1).groupingKeys.contains(Seq.empty))
    assertThrows[IllegalArgumentException](parseMarkdown("sum", "scalar.md", table))
    assertThrows[IllegalArgumentException](parseMarkdown(
      "sum",
      "aggregate.md",
      table.replace("`pmod(id, 256)`", "pmod(id, 256)"),
      aggregate = true))
  }

  test("index rejects duplicates path traversal empty entries and invalid filenames") {
    assert(parseIndex("trim.md\nrtrim.md\n", "index.txt") == Seq("trim.md", "rtrim.md"))
    Seq(
      "",
      "trim.md\ntrim.md",
      "../trim.md",
      "dir/trim.md",
      "TRIM.md",
      "trim.md\n\nrtrim.md",
      "trim.md ").foreach {
      index =>
        val error = intercept[IllegalArgumentException](parseIndex(index, "index.txt"))
        assert(error.getMessage.contains("index.txt:"))
        assert(error.getCause != null)
    }
    val missing = intercept[IllegalArgumentException] {
      load(new ClassLoader(null) {}, "missing")
    }
    assert(missing.getMessage.contains("missing/index.txt"))
    assert(missing.getCause != null)
  }
}

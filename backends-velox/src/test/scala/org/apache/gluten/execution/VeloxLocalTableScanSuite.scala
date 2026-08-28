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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.{Arrays => JArrays}

class VeloxLocalTableScanSuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.ansi.enabled", "false")
      .set("spark.gluten.sql.columnar.localTableScan", "true")
  }

  private def assertHasVeloxLocalTableScan(df: DataFrame): Unit = {
    val found = collect(df.queryExecution.executedPlan) {
      case _: VeloxLocalTableScanTransformer => true
    }
    assert(found.nonEmpty, "Expected VeloxLocalTableScanTransformer in plan")
  }

  private def createDF(rows: Seq[Row], schema: StructType): DataFrame = {
    spark.createDataFrame(JArrays.asList(rows: _*), schema)
  }

  // Builds a genuine batch [[LocalTableScanExec]] via the physical planner. This avoids calling the
  // constructor directly, whose arity differs across Spark versions (Spark 4.0+ adds a required
  // `stream` parameter), keeping this suite compilable on every supported Spark version.
  private def newBatchLocalTableScan(): LocalTableScanExec = {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val df = createDF(Seq(Row(1)), schema)
    df.queryExecution.sparkPlan
      .collectFirst { case l: LocalTableScanExec => l }
      .getOrElse(fail("Expected a LocalTableScanExec in the spark plan"))
  }

  test("basic LocalTableScanExec with int and string columns") {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val rows = Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
    val df = createDF(rows, schema)
    checkAnswer(df, rows)
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan with numeric types") {
    val schema = StructType(
      Seq(
        StructField("lng", LongType),
        StructField("dbl", DoubleType),
        StructField("flt", FloatType),
        StructField("shrt", ShortType),
        StructField("byt", ByteType)))
    val rows = Seq(Row(1L, 1.5, 2.5f, 100.toShort, 42.toByte))
    val df = createDF(rows, schema)
    checkAnswer(df, rows)
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan with boolean and null types") {
    val schema = StructType(
      Seq(StructField("flag", BooleanType), StructField("value", IntegerType, nullable = true)))
    val rows = Seq(Row(true, 1), Row(false, null))
    val df = createDF(rows, schema)
    checkAnswer(df, rows)
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan with empty collection") {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val df = createDF(Seq.empty, schema)
    checkAnswer(df, Seq.empty[Row])
  }

  test("LocalTableScan with aggregation downstream") {
    val schema = StructType(Seq(StructField("key", StringType), StructField("value", IntegerType)))
    val rows = Seq(Row("a", 10), Row("b", 20), Row("a", 30))
    val df = createDF(rows, schema)
    val result = df.groupBy("key").sum("value")
    checkAnswer(result, Seq(Row("a", 40), Row("b", 20)))
    assertHasVeloxLocalTableScan(result)
  }

  test("LocalTableScan with filter downstream") {
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val rows = Seq(Row(1), Row(2), Row(3), Row(4), Row(5))
    val df = createDF(rows, schema).filter("x > 3")
    checkAnswer(df, Seq(Row(4), Row(5)))
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan with join") {
    val leftSchema =
      StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val rightSchema =
      StructType(Seq(StructField("id", IntegerType), StructField("score", IntegerType)))
    val left = createDF(Seq(Row(1, "a"), Row(2, "b")), leftSchema)
    val right = createDF(Seq(Row(1, 100), Row(2, 200)), rightSchema)
    val result = left.join(right, "id")
    checkAnswer(result, Seq(Row(1, "a", 100), Row(2, "b", 200)))
    assertHasVeloxLocalTableScan(result)
  }

  test("LocalTableScan with all supported primitive types in one schema") {
    val schema = StructType(
      Seq(
        StructField("bool", BooleanType),
        StructField("byte", ByteType),
        StructField("short", ShortType),
        StructField("int", IntegerType),
        StructField("long", LongType),
        StructField("float", FloatType),
        StructField("double", DoubleType),
        StructField("string", StringType),
        StructField("date", DateType),
        StructField("timestamp", TimestampType),
        StructField("binary", BinaryType),
        StructField("decimal", DecimalType(10, 2))
      ))
    val rows = Seq(
      Row(
        true,
        1.toByte,
        2.toShort,
        3,
        4L,
        5.0f,
        6.0,
        "hello",
        java.sql.Date.valueOf("2024-01-01"),
        java.sql.Timestamp.valueOf("2024-01-01 12:00:00"),
        Array[Byte](1, 2, 3),
        new java.math.BigDecimal("123.45")
      ),
      Row(
        false,
        (-1).toByte,
        (-2).toShort,
        -3,
        -4L,
        -5.0f,
        -6.0,
        "world",
        java.sql.Date.valueOf("1970-01-01"),
        java.sql.Timestamp.valueOf("1970-01-01 00:00:00"),
        Array[Byte](),
        new java.math.BigDecimal("-123.45")
      )
    )
    val df = createDF(rows, schema)
    checkAnswer(df, rows)
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan with array type") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("arr", ArrayType(IntegerType, containsNull = true), nullable = true)))
    val rows =
      Seq(Row(1, Seq(10, 20, 30)), Row(2, Seq.empty[Int]), Row(3, null), Row(4, Seq(-1, 0, 1)))
    val df = createDF(rows, schema)
    checkAnswer(df, rows)
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan with map type falls back") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField(
          "m",
          MapType(StringType, IntegerType, valueContainsNull = true),
          nullable = true)))
    val rows = Seq(Row(1, Map("a" -> 1, "b" -> 2)), Row(2, Map.empty[String, Int]), Row(3, null))
    val df = createDF(rows, schema)
    checkAnswer(df, rows)
    // MapType is not supported in Arrow export path - should fall back
    val cnt = collect(df.queryExecution.executedPlan) {
      case _: VeloxLocalTableScanTransformer => true
    }
    assert(cnt.isEmpty, "Expected fallback - MapType not supported in Arrow export")
  }

  test("LocalTableScan with nested struct type") {
    val innerSchema = StructType(
      Seq(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true)))
    val schema = StructType(
      Seq(StructField("id", IntegerType), StructField("person", innerSchema, nullable = true)))
    val rows = Seq(Row(1, Row("alice", 30)), Row(2, Row("bob", null)), Row(3, null))
    val df = createDF(rows, schema)
    checkAnswer(df, rows)
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan falls back for unsupported types") {
    val schema = StructType(
      Seq(StructField("id", IntegerType), StructField("duration", DayTimeIntervalType())))
    val rows = Seq(Row(1, java.time.Duration.ofHours(3)), Row(2, java.time.Duration.ofDays(1)))
    val df = createDF(rows, schema)
    // Should still produce correct results via fallback to vanilla Spark
    checkAnswer(df, rows)
    val cnt = collect(df.queryExecution.executedPlan) {
      case _: VeloxLocalTableScanTransformer => true
    }
    assert(cnt.isEmpty, "Expected fallback - VeloxLocalTableScanTransformer should NOT be in plan")
  }

  test("LocalTableScan idempotent re-reads") {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val rows = Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d"), Row(5, "e"))
    val df = createDF(rows, schema)
    // Read twice to verify idempotency
    checkAnswer(df, rows)
    checkAnswer(df, rows)
    assertHasVeloxLocalTableScan(df)
  }

  test("LocalTableScan falls back when localTableScan offload is disabled") {
    withSQLConf("spark.gluten.sql.columnar.localTableScan" -> "false") {
      val schema =
        StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
      val rows = Seq(Row(1, "a"), Row(2, "b"))
      val df = createDF(rows, schema)
      checkAnswer(df, rows)
      val cnt = collect(df.queryExecution.executedPlan) {
        case _: VeloxLocalTableScanTransformer => true
      }
      assert(cnt.isEmpty, "Expected fallback when localTableScan offload is disabled")
    }
  }

  test("isSupportLocalTableScanExec skips deserialized plan with null transient rows") {
    // Simulates a plan that was serialized and shipped across an RPC boundary (e.g. an AQE
    // sub-plan), where the @transient rows field becomes null after Java serialization.
    // Offload must be skipped to avoid a downstream NPE.
    val plan = newBatchLocalTableScan()

    // Serialize and deserialize to null out the @transient rows field.
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(plan)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[LocalTableScanExec]

    assert(deserialized.rows == null, "Expected null rows after deserialization")

    // Should NOT throw NPE - this is the bug the v2 change fixes.
    val api = BackendsApiManager.getSparkPlanExecApiInstance
    val result = api.isSupportLocalTableScanExec(deserialized)
    assert(!result, "Deserialized plan with null transient rows should not be offloaded")
  }

  test("getLocalTableScanStream returns None for a batch LocalTableScanExec (no streaming skip)") {
    // The streaming-source skip in isSupportLocalTableScanExec only triggers on Spark 4.0+, where
    // LocalTableScanExec may carry a streaming SparkDataStream. This version-agnostic test asserts
    // the shim classifies an ordinary batch plan as non-streaming (None) on every supported Spark
    // version, so the streaming guard does not falsely skip batch offload. The true streaming path
    // (getLocalTableScanStream(plan).isDefined) is exercised by Spark 4.x profiles only.
    val plan = newBatchLocalTableScan()

    val stream = SparkShimLoader.getSparkShims.getLocalTableScanStream(plan)
    assert(
      stream.isEmpty,
      "A batch LocalTableScanExec must not be classified as a streaming source")

    withSQLConf("spark.gluten.sql.columnar.localTableScan" -> "true") {
      val api = BackendsApiManager.getSparkPlanExecApiInstance
      assert(
        api.isSupportLocalTableScanExec(plan),
        "Batch plan must not be skipped by the streaming-source guard")
    }
  }

  test("isSupportLocalTableScanExec returns true for normal plan") {
    withSQLConf("spark.gluten.sql.columnar.localTableScan" -> "true") {
      val plan = newBatchLocalTableScan()

      val api = BackendsApiManager.getSparkPlanExecApiInstance
      val result = api.isSupportLocalTableScanExec(plan)
      assert(result, "Normal plan should be supported for offload")
    }
  }

  test("isSupportLocalTableScanExec returns false when localTableScan offload is disabled") {
    withSQLConf("spark.gluten.sql.columnar.localTableScan" -> "false") {
      val plan = newBatchLocalTableScan()

      val api = BackendsApiManager.getSparkPlanExecApiInstance
      assert(!api.isSupportLocalTableScanExec(plan))
    }
  }
}

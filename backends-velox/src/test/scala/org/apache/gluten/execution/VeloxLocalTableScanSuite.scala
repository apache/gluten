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

import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import java.util.{Arrays => JArrays}

class VeloxLocalTableScanSuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.ansi.enabled", "false")
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
}

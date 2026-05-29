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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.AttachDistributedSequence
import org.apache.spark.sql.classic.ClassicDataset
import org.apache.spark.sql.execution.python.AttachDistributedSequenceExec
import org.apache.spark.sql.types.LongType

class VeloxAttachDistributedSequenceExecSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "3")
      .set("spark.default.parallelism", "3")
  }

  /**
   * Build a DataFrame that prepends a distributed-sequence id column using a directly constructed
   * [[AttachDistributedSequence]] logical node. This avoids depending on pandas-on-Spark / PySpark
   * in JVM tests.
   */
  private def attachSequence(df: DataFrame, name: String = "id"): DataFrame = {
    val attr = AttributeReference(name, LongType, nullable = false)()
    ClassicDataset.ofRows(spark, AttachDistributedSequence(attr, df.queryExecution.analyzed))
  }

  test("contiguous ids for a single partition") {
    val df = attachSequence(spark.range(0, 7, 1, 1).toDF("v"))
    val ids = df.select("id").collect().map(_.getLong(0)).toSeq
    assert(ids == Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L))
  }

  test("contiguous ids across multiple partitions of equal size") {
    val df = attachSequence(spark.range(0, 12, 1, 4).toDF("v"))
    val ids = df.select("id").collect().map(_.getLong(0)).toSeq.sorted
    assert(ids == (0L until 12L))
    // Check the offload happened.
    val plan = df.queryExecution.executedPlan
    val matched = plan.collectFirst {
      case e: ColumnarAttachDistributedSequenceExec => e
    }
    assert(matched.isDefined, s"Expected ColumnarAttachDistributedSequenceExec in:\n$plan")
  }

  test("contiguous ids across multiple partitions of unequal size") {
    val base = spark.range(0, 100, 1, 8).toDF("v").filter("v % 3 = 0")
    val df = attachSequence(base)
    val rows = df.collect()
    val ids = rows.map(_.getAs[Long]("id")).toSeq.sorted
    assert(ids == (0L until rows.length))
  }

  test("empty input produces empty output") {
    val df = attachSequence(spark.range(0, 0, 1, 4).toDF("v"))
    assert(df.collect().isEmpty)
  }

  test("id is paired with the correct row payload") {
    val df = attachSequence(spark.range(0, 5, 1, 1).toDF("v"))
    val rows = df.select("id", "v").collect().map(r => (r.getLong(0), r.getLong(1))).toSeq
    assert(rows == Seq((0L, 0L), (1L, 1L), (2L, 2L), (3L, 3L), (4L, 4L)))
  }

  test("falls back to vanilla exec when columnar attach-distributed-sequence is disabled") {
    withSQLConf(
      "spark.gluten.sql.columnar.attachDistributedSequence" -> "false"
    ) {
      val df = attachSequence(spark.range(0, 4, 1, 2).toDF("v"))
      val plan = df.queryExecution.executedPlan
      assert(
        plan.find(_.isInstanceOf[ColumnarAttachDistributedSequenceExec]).isEmpty,
        s"Expected no ColumnarAttachDistributedSequenceExec in:\n$plan")
      val ids = df.select("id").collect().map(_.getLong(0)).toSeq.sorted
      assert(ids == Seq(0L, 1L, 2L, 3L))
    }
  }

  test("GlutenConfig getter returns default true") {
    assert(GlutenConfig.get.enableColumnarAttachDistributedSequence)
  }

  test("vanilla exec construction does not break offload pattern") {
    // Sanity: confirm vanilla exec class is available and constructible (used in offload).
    val attr = AttributeReference("id", LongType, nullable = false)()
    val child = spark.range(0, 1, 1, 1).queryExecution.executedPlan
    val vanilla = AttachDistributedSequenceExec(attr, child)
    assert(vanilla.output.head.name == "id")
  }
}

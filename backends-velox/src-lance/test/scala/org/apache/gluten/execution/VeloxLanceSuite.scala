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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * Exercises the read-only Lance offload: a columnar Lance scan is handed to Velox through the Arrow
 * C stream ([[LanceScanTransformer]]), while scans that the export path cannot serve (pushed
 * aggregation, full-text query) fall back to vanilla Spark.
 *
 * Requires the lance-spark runtime (and its native library) on the classpath; see
 * docs/get-started/VeloxLance.md for the supported platforms.
 */
class VeloxLanceSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      // Path-based reads use the DataSource directly; the catalog is registered for parity with
      // lance-spark's own tests and any namespace-qualified access.
      .set("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
  }

  /** Writes a small Lance dataset and registers it as a temp view. */
  private def withLanceView(view: String, rows: Int)(f: => Unit): Unit = {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/$view.lance"
        spark
          .range(rows)
          .selectExpr(
            "cast(id as int) as id",
            "cast(id * 2 as int) as v",
            "concat('n', cast(id as string)) as name")
          .write
          .format("lance")
          .option("path", uri)
          .save()

        spark.read.format("lance").option("path", uri).load().createOrReplaceTempView(view)
        try f
        finally spark.catalog.dropTempView(view)
    }
  }

  private def isLanceScan(plan: SparkPlan): Boolean = plan match {
    case _: LanceScanTransformer => true
    case InputAdapter(child) => isLanceScan(child)
    case _ => false
  }

  /** The operators that directly consume the Lance scan (looking through codegen InputAdapters). */
  private def lanceScanParents(df: DataFrame): Seq[SparkPlan] =
    getExecutedPlan(df)
      .filterNot(_.isInstanceOf[InputAdapter])
      .filter(_.children.exists(isLanceScan))

  test("lance scan offloads to LanceScanTransformer") {
    withLanceView("lance_basic", 100) {
      runQueryAndCompare("select id, v, name from lance_basic") {
        df =>
          checkGlutenPlan[LanceScanTransformer](df)
          // With no Velox consumer, the Arrow Java batches are converted to rows directly by
          // vanilla ColumnarToRowExec, with no round trip through Velox.
          val parents = lanceScanParents(df)
          assert(
            parents.nonEmpty && parents.forall(_.isInstanceOf[ColumnarToRowExec]),
            s"Expected ColumnarToRowExec over the Lance scan, got:\n${df.queryExecution.executedPlan}"
          )
      }
    }
  }

  test("lance scan feeds Velox operators without a row conversion") {
    withLanceView("lance_velox_agg", 100) {
      // A grouped aggregation is not pushed into Lance, so it runs as a Velox aggregate on top of
      // the scan: Arrow Java batches -> OffloadArrowDataExec -> ArrowColumnarToVeloxColumnarExec.
      runQueryAndCompare(
        "select id % 10 as k, sum(v) as s, count(*) as c from lance_velox_agg group by id % 10") {
        df =>
          val plan = df.queryExecution.executedPlan
          checkGlutenPlan[LanceScanTransformer](df)
          checkGlutenPlan[HashAggregateExecBaseTransformer](df)
          val parents = lanceScanParents(df)
          assert(
            parents.nonEmpty && parents.forall(_.isInstanceOf[OffloadArrowDataExec]),
            s"Expected OffloadArrowDataExec over the Lance scan, got:\n$plan")
          assert(
            getExecutedPlan(df).exists {
              case p: ArrowColumnarToVeloxColumnarExec =>
                p.child.isInstanceOf[OffloadArrowDataExec] && isLanceScan(p.child.children.head)
              case _ => false
            },
            s"Expected ArrowColumnarToVeloxColumnarExec(OffloadArrowDataExec(scan)), got:\n$plan"
          )
          assert(
            !getExecutedPlan(df).exists {
              case c: ColumnarToRowExec => isLanceScan(c.child)
              case _ => false
            },
            s"Lance scan output must not be converted to rows before Velox, got:\n$plan"
          )
      }
    }
  }

  test("lance scan offloads with projection and pushed filter") {
    withLanceView("lance_filtered", 100) {
      runQueryAndCompare("select id, v from lance_filtered where id < 50") {
        df =>
          checkGlutenPlan[LanceScanTransformer](df)
          checkAnswer(df, (0 until 50).map(i => Row(i, i * 2)))
      }
    }
  }

  test("lance scan falls back when an aggregation is pushed down") {
    withLanceView("lance_agg", 100) {
      // A filtered COUNT(*) is pushed into the Lance scan; the Arrow C stream export cannot serve a
      // pushed aggregation, so the offload guard must leave it on a vanilla BatchScanExec.
      val df: DataFrame = spark.sql("select count(*) from lance_agg where id < 50")
      checkSparkPlan[BatchScanExec](df)
      assert(
        !getExecutedPlan(df).exists(_.isInstanceOf[LanceScanTransformer]),
        "Lance scan with a pushed aggregation must not be offloaded")
      checkAnswer(df, Seq(Row(50L)))
    }
  }
}

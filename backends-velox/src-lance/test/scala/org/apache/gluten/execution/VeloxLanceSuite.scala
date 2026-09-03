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
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * Exercises the read-only Lance offload: a columnar Lance scan is handed to Velox through the Arrow
 * C stream ([[LanceScanTransformer]]), while scans that the export path cannot serve (pushed
 * aggregation, full-text query) fall back to vanilla Spark.
 *
 * Requires the lance-spark runtime (and its native library) on the classpath, which is only
 * published for linux-x86-64. On other platforms these tests do not run; see
 * docs/get-started/VeloxLance.md for the supported local/CI environment.
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

  test("lance scan offloads to LanceScanTransformer") {
    withLanceView("lance_basic", 100) {
      runQueryAndCompare("select id, v, name from lance_basic") {
        checkGlutenPlan[LanceScanTransformer]
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

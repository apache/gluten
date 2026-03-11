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
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.{InMemoryTableScanExec, SparkCacheUtil}
import org.apache.spark.sql.types.{LongType, Metadata, MetadataBuilder, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class VeloxColumnarCacheSuite extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // A common practice as well as in Spark tests, to clear the cache serializer
    // in case it was already set as the default row-based serializer.
    SparkCacheUtil.clearCacheSerializer()
    createTPCHNotNullTables()
  }

  override protected def afterAll(): Unit = {
    SparkCacheUtil.clearCacheSerializer()
    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "3")
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
  }

  private def checkColumnarTableCache(plan: SparkPlan): Unit = {
    assert(
      find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.isDefined,
      plan)
    assert(
      collect(plan) { case v: VeloxColumnarToRowExec => v }.size <= 1,
      plan
    )
  }

  test("Input columnar batch") {
    TPCHTables.map(_.name).foreach {
      table =>
        runQueryAndCompare(s"SELECT * FROM $table", cache = true) {
          df => checkColumnarTableCache(df.queryExecution.executedPlan)
        }
    }
  }

  test("Input columnar batch and column pruning") {
    val expected = sql("SELECT l_partkey FROM lineitem").collect()
    val cached = sql("SELECT * FROM lineitem").cache()
    try {
      val df = cached.select("l_partkey")
      checkAnswer(df, expected)
      checkColumnarTableCache(df.queryExecution.executedPlan)
    } finally {
      cached.unpersist()
    }
  }

  testWithMinSparkVersion("input row", "3.2") {
    withTable("t") {
      sql("CREATE TABLE t USING json AS SELECT * FROM values(1, 'a', (2, 'b'), (3, 'c'))")
      runQueryAndCompare("SELECT * FROM t", cache = true) {
        df => checkColumnarTableCache(df.queryExecution.executedPlan)
      }
    }
  }

  test("Input vanilla Spark columnar batch") {
    withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
      val df = spark.table("lineitem")
      val expected = df.collect()
      val actual = df.cache()
      try {
        checkAnswer(actual, expected)
      } finally {
        actual.unpersist()
      }
    }
  }

  // See issue https://github.com/apache/incubator-gluten/issues/8497.
  test("Input fallen back vanilla Spark columnar scan") {
    def withId(id: Int): Metadata =
      new MetadataBuilder().putLong("parquet.field.id", id).build()

    withTempDir {
      dir =>
        val readSchema =
          new StructType()
            .add("l_orderkey_read", LongType, true, withId(1))
        val writeSchema =
          new StructType()
            .add("l_orderkey_write", LongType, true, withId(1))
        withSQLConf("spark.sql.parquet.fieldId.read.enabled" -> "true") {
          // Write a table with metadata information that Gluten Velox backend doesn't support,
          // to emulate the scenario that a Spark columnar scan is not offload-able so fallen back,
          // then user tries to cache it.
          spark
            .createDataFrame(
              spark.sql("select l_orderkey from lineitem").collect().toList.asJava,
              writeSchema)
            .write
            .mode("overwrite")
            .parquet(dir.getCanonicalPath)
          val df = spark.read.schema(readSchema).parquet(dir.getCanonicalPath)
          df.cache()
          assert(df.collect().length == 60175)
        }
    }
  }

  test("CachedColumnarBatch serialize and deserialize") {
    val df = spark.table("lineitem")
    val expected = df.collect()
    val actual = df.persist(StorageLevel.DISK_ONLY)
    try {
      checkAnswer(actual, expected)
    } finally {
      actual.unpersist()
    }
  }

  test("Support transform count(1) with table cache") {
    val cached = spark.table("lineitem").cache()
    try {
      val df = spark.sql("SELECT COUNT(*) FROM lineitem")
      checkAnswer(df, Row(60175))
      assert(
        find(df.queryExecution.executedPlan) {
          case _: RowToVeloxColumnarExec => true
          case _ => false
        }.isEmpty
      )
    } finally {
      cached.unpersist()
    }
  }

  test("no ColumnarToRow for table cache") {
    val cached = spark.table("lineitem").cache()
    withSQLConf(GlutenConfig.COLUMNAR_HASHAGG_ENABLED.key -> "false") {
      try {
        val df = spark.sql("SELECT COUNT(*) FROM lineitem")
        checkAnswer(df, Row(60175))
        assert(
          find(df.queryExecution.executedPlan) {
            case VeloxColumnarToRowExec(child: SparkPlan) if PlanUtil.isGlutenTableCache(child) =>
              true
            case _ => false
          }.isEmpty
        )
      } finally {
        cached.unpersist()
      }
    }
  }

  test("Columnar table cache should compatible with TableCacheQueryStage") {
    withSQLConf(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1") {
      val cached = spark.table("lineitem").cache()
      try {
        val df = cached.filter(row => row.getLong(0) > 0)
        assert(df.count() == 60175)
        assert(find(df.queryExecution.executedPlan) {
          case _: ColumnarToRowExec => true
          case _ => false
        }.isEmpty)
        assert(find(df.queryExecution.executedPlan) {
          case _: RowToVeloxColumnarExec => true
          case _ => false
        }.isEmpty)
      } finally {
        cached.unpersist()
      }
    }
  }

  test("Fix cache output if selectedAttributes has wrong ordering with cacheAttributes") {
    withTempPath {
      path =>
        spark
          .range(10)
          .selectExpr("id as c1", "id % 3 as c2", "id % 5 as c3")
          .write
          .parquet(path.getCanonicalPath)

        val df = spark.read.parquet(path.getCanonicalPath)
        val expected = df.select("c3", "c2", "c1").collect()
        try {
          val result = df.cache().select("c3", "c2", "c1")
          checkAnswer(result, expected)
        } finally {
          df.unpersist()
        }
    }
  }

  test("Fix miss RowToColumnar with columnar table cache in AQE") {
    withSQLConf(
      "spark.sql.adaptive.forceApply" -> "true",
      GlutenConfig.EXPRESSION_BLACK_LIST.key -> "add",
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1") {
      runQueryAndCompare("SELECT l_partkey + 1 FROM lineitem", cache = true) {
        df =>
          val plan = df.queryExecution.executedPlan
          val tableCache = find(plan)(_.isInstanceOf[InMemoryTableScanExec])
          assert(tableCache.isDefined)
          val cachedPlan =
            tableCache.get.asInstanceOf[InMemoryTableScanExec].relation.cachedPlan
          assert(find(cachedPlan) {
            _.isInstanceOf[ProjectExecTransformer]
          }.isEmpty)
      }
    }
  }
}

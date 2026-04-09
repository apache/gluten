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
import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.config.VeloxConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenTestUtils, Row}
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, FormattedMode}

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets

abstract class VeloxTPCHTableSupport extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"

  // TODO: result comparison is not supported currently.
  protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set(VeloxConfig.COLUMNAR_VELOX_MEM_INIT_CAPACITY.key, "1m")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    // TODO Should enable this after fix the issue of native plan detail occasional disappearance
    // .set("spark.gluten.sql.injectNativePlanStringToExplain", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }
}

abstract class VeloxTPCHSuite extends VeloxTPCHTableSupport {
  // here we only get spark major version
  private lazy val formatSparkVersion: String = spark.version.replace(".", "").substring(0, 2)

  private def formatMaterializedPlan(plan: String): String = {
    plan
      .replaceAll("#[0-9]*L*", "#X")
      .replaceAll("plan_id=[0-9]*", "plan_id=X")
      .replaceAll("Statistics[(A-Za-z0-9=. ,+)]*", "Statistics(X)")
      .replaceAll("WholeStageCodegenTransformer[0-9 ()]*", "WholeStageCodegenTransformer (X)")
      .replaceAll("\\[file:[.\\-/a-zA-z0-9= ,_%]*]", "[*]")
      // for unexpected blank
      .replaceAll("Scan parquet ", "Scan parquet")
      // Spark QueryStageExec will take it's id as argument, replace it with X
      .replaceAll("Arguments: [0-9]+, [0-9]+, [0-9]+", "Arguments: X, X, X")
      .replaceAll("Arguments: [0-9]+, [0-9]+", "Arguments: X, X")
      .replaceAll("Arguments: [0-9]+", "Arguments: X")
      // mask PullOutPostProject and PullOutPreProject id
      .replaceAll("_pre_[0-9]*", "_pre_X")
      .replaceAll("_post_[0-9]*", "_post_X")
      .trim
  }

  private def getGoldenFile(path: String): String = {
    FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8).trim
  }

  def subType(): String = ""
  def shouldCheckGoldenFiles(): Boolean = {
    Seq("v1", "v1-bhj", "v1-ras", "v1-bhj-ras").contains(subType()) && (
      formatSparkVersion match {
        case "32" => true
        case "33" => true
        case "34" => true
        case "35" => true
        case _ => false
      }
    )
  }

  private def checkGoldenFile(df: DataFrame, id: Int): Unit = {
    // skip checking golden file for non-ready subtype and spark version
    if (!shouldCheckGoldenFiles) {
      return
    }
    val file = s"tpch-approved-plan/${subType()}/spark$formatSparkVersion/$id.txt"
    val actual = formatMaterializedPlan(df.queryExecution.explainString(FormattedMode))
    val path = s"$rootPath$file"
    // due to assert throw too much info
    // let's check and print diff manually
    if (!getGoldenFile(path).equals(actual)) {
      val actualFile = new File(FileUtils.getTempDirectory, file)
      new File(actualFile.getParent).mkdirs()
      FileUtils.writeStringToFile(actualFile, actual, StandardCharsets.UTF_8)
      org.scalatest.Assertions.fail(
        s"Mismatch for query $id\n" +
          s"Actual Plan path: ${actualFile.getAbsolutePath}\n" +
          s"Golden Plan path: $path")
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true")
  }

  test("TPC-H q1") {
    runTPCHQuery(1, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 1)
    }
  }

  test("TPC-H q2") {
    runTPCHQuery(2, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      _ => // due to tpc-h q2 will generate multiple plans, skip checking golden file for now
    }
  }

  test("TPC-H q3") {
    runTPCHQuery(3, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 3)
    }
  }

  test("TPC-H q4") {
    runTPCHQuery(4, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 4)
    }
  }

  test("TPC-H q5") {
    runTPCHQuery(5, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 5)
    }
  }

  test("TPC-H q6") {
    runTPCHQuery(6, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 6)
    }
  }

  test("TPC-H q7") {
    runTPCHQuery(7, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 7)
    }
  }

  test("TPC-H q8") {
    runTPCHQuery(8, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 8)
    }
  }

  test("TPC-H q9") {
    runTPCHQuery(9, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 9)
    }
  }

  test("TPC-H q10") {
    runTPCHQuery(10, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 10)
    }
  }

  test("TPC-H q11") {
    runTPCHQuery(11, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 11)
    }
  }

  test("TPC-H q12") {
    runTPCHQuery(12, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 12)
    }
  }

  test("TPC-H q13") {
    runTPCHQuery(13, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 13)
    }
  }

  test("TPC-H q14") {
    runTPCHQuery(14, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 14)
    }
  }

  test("TPC-H q15") {
    runTPCHQuery(15, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 15)
    }
  }

  test("TPC-H q16") {
    runTPCHQuery(16, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 16)
    }
  }

  test("TPC-H q17") {
    runTPCHQuery(17, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 17)
    }
  }

  test("TPC-H q18") {
    runTPCHQuery(18, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 18)
    }
  }

  test("TPC-H q19") {
    runTPCHQuery(19, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 19)
    }
  }

  test("TPC-H q20") {
    runTPCHQuery(20, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 20)
    }
  }

  test("TPC-H q21") {
    runTPCHQuery(21, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 21)
    }
  }

  test("TPC-H q22") {
    runTPCHQuery(22, tpchQueries, queriesResults, compareResult = false, noFallBack = true) {
      checkGoldenFile(_, 22)
    }
  }
}

class VeloxTPCHDistinctSpillSuite extends VeloxTPCHTableSupport {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "50m")
      // to trigger distinct spill early
      .set(GlutenCoreConfig.COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO.key, "0.9")
      .set(GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key, "8k")
  }

  test("distinct spill") {
    val df = spark.sql("select count(distinct *) from lineitem limit 1")
    GlutenTestUtils.compareAnswers(df.collect(), Seq(Row(60175)))
  }
}

class VeloxTPCHMiscSuite extends VeloxTPCHTableSupport {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
  }

  test("order by limit") {
    val df = spark.sql(
      """
        |select n_nationkey from nation order by n_nationkey limit 5
        |""".stripMargin
    )
    val sortExec = df.queryExecution.executedPlan.collect {
      case sortExec: TakeOrderedAndProjectExecTransformer => sortExec
    }
    assert(sortExec.size == 1)
    val result = df.collect()
    df.explain(true)
    val expectedResult = Seq(Row(0), Row(1), Row(2), Row(3), Row(4))
    GlutenTestUtils.compareAnswers(result, expectedResult)
  }
}

class VeloxTPCHV1Suite extends VeloxTPCHSuite {
  override def subType(): String = "v1"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}

class VeloxTPCHV1GlutenShuffleManagerSuite extends VeloxTPCHSuite {
  override def subType(): String = "v1-gluten-shuffle-manager"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.GlutenShuffleManager")
  }

  override protected def runQueryAndCompare(
      sqlStr: String,
      compareResult: Boolean,
      noFallBack: Boolean,
      cache: Boolean)(customCheck: DataFrame => Unit): DataFrame = {
    assert(noFallBack)
    super.runQueryAndCompare(sqlStr, compareResult, noFallBack, cache) {
      df =>
        assert(df.queryExecution.executedPlan.collect {
          case p if p.isInstanceOf[ColumnarShuffleExchangeExec] => p
        }.nonEmpty)
        customCheck(df)
    }
  }
}

class VeloxTPCHV1BhjSuite extends VeloxTPCHSuite {
  override def subType(): String = "v1-bhj"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
  }
}

/** BroadcastBuildSideRelation use off-heap. */
class VeloxTPCHV1BhjOffheapSuite extends VeloxTPCHSuite {
  override def subType(): String = "v1-bhj-off-heap"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
      .set(VeloxConfig.VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP.key, "true")
  }
}

class VeloxTPCHV2Suite extends VeloxTPCHSuite {
  override def subType(): String = "v2"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}

class VeloxTPCHV2BhjSuite extends VeloxTPCHSuite {
  override def subType(): String = "v2-bhj"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
  }
}

class VeloxPartitionedTableTPCHSuite extends VeloxTPCHSuite {
  override def subType(): String = "partitioned"

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.memory.offHeap.size", "4g")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    TPCHTableDataFrames = TPCHTables.map {
      table =>
        val tableDir = getClass.getResource(resourcePath).getFile
        val tablePath = new File(tableDir, table.name).getAbsolutePath
        val tableDF = spark.read.format(fileFormat).load(tablePath)

        tableDF.write
          .format(fileFormat)
          .partitionBy(table.partitionColumns: _*)
          .mode("append")
          .saveAsTable(table.name)
        (table.name, tableDF)
    }.toMap
  }

  override protected def afterAll(): Unit = {
    if (TPCHTableDataFrames != null) {
      TPCHTableDataFrames.keys.foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))
    }
    super.afterAll()
  }
}

class VeloxTPCHV1GlutenBhjVanillaBeSuite extends VeloxTPCHSuite {
  override def subType(): String = "gluten-bhj-vanilla-be"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
      .set(GlutenConfig.COLUMNAR_BROADCAST_JOIN_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_BROADCAST_EXCHANGE_ENABLED.key, "false")
  }
}

class VeloxTPCHV1VanillaBhjGlutenBeSuite extends VeloxTPCHSuite {
  override def subType(): String = "vanilla-bhj-gluten-be"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
      .set(GlutenConfig.COLUMNAR_BROADCAST_JOIN_ENABLED.key, "false")
      .set(GlutenConfig.COLUMNAR_BROADCAST_EXCHANGE_ENABLED.key, "true")
  }
}

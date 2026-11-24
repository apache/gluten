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
package org.apache.gluten.execution.cache

import org.apache.gluten.backendsapi.clickhouse.CHConfig._
import org.apache.gluten.execution._
import org.apache.gluten.utils.CacheTestHelper

import org.apache.spark.SparkConf

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset

abstract class GlutenClickHouseCacheBaseTestSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with TPCHDatabase
  with TPCHParquetResult {

  protected val remotePath: String

  // Abstract methods to be implemented by subclasses
  protected def cleanupCache(): Unit =
    cacheHelper.deleteCache(spark, s"$remotePath/lineitem", s"$remotePath/$SPARK_DIR_NAME")

  protected def copyDataIfNeeded(): Unit

  // Initialize the cache helper - accessible to subclasses
  protected val cacheHelper = new CacheTestHelper(TMP_PREFIX)

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .setCHConfig("use_local_format", true)
      .set(prefixOf("shuffle.hash.algorithm"), "sparkMurmurHash3_32")
      .set("spark.sql.adaptive.enabled", "false")

    // Apply cache configuration using the helper
    cacheHelper.setCacheConfig(conf)
  }

  override protected def createTestTables(): Unit = {
    createTPCHTables(remotePath)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    copyDataIfNeeded()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanupCache()
  }

  override protected def afterAll(): Unit = {
    cleanupCache()
    super.afterAll()
  }

  def runWithoutCache(): Unit = {
    customCheck(6) {
      df =>
        val plans = df.queryExecution.executedPlan.collect {
          case scanExec: FileSourceScanExecTransformer => scanExec
        }
        assert(plans.size == 1)
        assert(plans.head.metrics("readMissBytes").value != 0)
    }
  }

  def runWithCache(): Unit = {
    customCheck(6) {
      df =>
        val plans = df.queryExecution.executedPlan.collect {
          case scanExec: FileSourceScanExecTransformer => scanExec
        }
        assert(plans.size == 1)
        assert(plans.head.metrics("readMissBytes").value == 0)
        assert(plans.head.metrics("readCacheBytes").value != 0)
    }
  }

  test("test cache") {
    runWithoutCache()
    runWithCache()
  }

  test("test cache file command") {
    runSql(s"CACHE FILES select * from '$remotePath/lineitem'", noFallBack = false) { _ => }
    runWithCache()
  }

  test("test no cache by query") {
    withSQLConf(
      runtimeSettings("read_from_filesystem_cache_if_exists_otherwise_bypass_cache") -> "true") {
      runWithoutCache()
    }

    runWithoutCache()
    runWithCache()
  }

  test("GLUTEN-7542: Fix cache refresh") {
    withTable("issue_7542") {
      withSQLConf("spark.sql.hive.manageFilesourcePartitions" -> "false") {
        val filePath = s"$remotePath/$SPARK_DIR_NAME/issue_7542/"
        val targetDirs = new Path(filePath)
        val fs = targetDirs.getFileSystem(spark.sessionState.newHadoopConf())
        fs.mkdirs(targetDirs)
        val out = fs.create(new Path(s"$filePath/00000_0"))
        IOUtils.write("1\n2\n3\n4\n5", out, Charset.defaultCharset())
        out.close()
        sql(s"""
               |CREATE external TABLE `issue_7542`(
               |  `c_custkey` int )
               |using CSV
               |LOCATION
               |  '$filePath/'
               |""".stripMargin)

        sql(s"""select * from issue_7542""").collect()
        fs.delete(new Path(s"$filePath/00000_0"), false)
        val out2 = fs.create(new Path(s"$filePath/00000_0"))
        IOUtils.write("1\n2\n3\n4\n3\n3\n3", out2, Charset.defaultCharset())
        out2.close()
        val df = sql(s"""select count(*) from issue_7542 where c_custkey=3""")
        // refresh list file
        collect(df.queryExecution.executedPlan) {
          case scanExec: FileSourceScanExecTransformer => scanExec.relation.location.refresh()
        }
        val result = df.collect()
        assert(result.length == 1)
        assert(result.head.getLong(0) == 4)
      }
    }
  }

  test("test set_read_util_position") {
    val tableName = "read_until_test"
    val tablePath = s"$remotePath/$SPARK_DIR_NAME/$tableName/"
    val targetFile = new Path(remotePath)
    val fs = targetFile.getFileSystem(spark.sessionState.newHadoopConf())
    fs.delete(new Path(tablePath), true)
    sql(s"""
           | CREATE TABLE $tableName
           | USING csv
           | LOCATION '$tablePath'
           | as
           | select * from lineitem
           |""".stripMargin)

    val sql_str =
      s"""
         |SELECT
         |    sum(l_extendedprice * l_discount) AS revenue
         |FROM
         |    $tableName
         |WHERE
         |    l_shipdate >= date'1994-01-01'
         |    AND l_shipdate < date'1994-01-01' + interval 1 year
         |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
         |    AND l_quantity < 24;
         |
         |""".stripMargin

    withSQLConf("spark.sql.files.maxPartitionBytes" -> "1M") {
      compareResultsAgainstVanillaSpark(sql_str, compareResult = true, _ => {})
    }

    fs.delete(new Path(tablePath), true)
  }
}

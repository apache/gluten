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

import org.apache.gluten.backendsapi.clickhouse.RuntimeConfig
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.{HDFSTestHelper, MinioTestHelper, UTSystemParameters}

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}
import org.apache.spark.internal.Logging

import org.apache.commons.io.FileUtils
import org.scalatest.Tag

import java.io.File

trait TestDatabase {

  /** source parquet data */
  val testParquetAbsolutePath: String

  /** data path for tests, depends on diskOutputDataPath config */
  val dataHome: String

  /** Path for storing query files */
  // val queryPath: String

  protected def prepareTestTables(): Unit
}

class GlutenClickHouseWholeStageTransformerSuite
  extends WholeStageTransformerSuite
  with TestDatabase
  with Logging {

  val DBL_EPSILON = 2.2204460492503131e-16
  val DBL_RELAX_EPSILON: Double = Math.pow(10, -11)
  val FLT_EPSILON = 1.19209290e-07f

  private val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }
  val SPARK_DIR_NAME: String = sparkVersion.replace(".", "-")

  protected val TMP_PREFIX = s"/tmp/gluten/$SPARK_DIR_NAME"

  val BUCKET_NAME: String = SPARK_DIR_NAME
  val minioHelper = new MinioTestHelper(TMP_PREFIX)
  val hdfsHelper = new HDFSTestHelper(TMP_PREFIX, SPARK_DIR_NAME)

  val CH_DEFAULT_STORAGE_DIR = "/data"

  protected def spark32: Boolean = sparkVersion.equals("3.2")
  protected def spark33: Boolean = sparkVersion.equals("3.3")
  protected def spark35: Boolean = sparkVersion.equals("3.5")

  def AlmostEqualsIsRel(expected: Double, actual: Double, EPSILON: Double = DBL_EPSILON): Unit = {
    val diff = Math.abs(expected - actual)
    val epsilon = EPSILON * Math.max(Math.abs(expected), Math.abs(actual))
    if (diff > epsilon) {
      fail(s"""
              |expected: $expected
              |actual:   $actual
              | abs(expected-expected) ~ epsilon = $diff ~ $epsilon
              |""".stripMargin)
    }
  }

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    val conf = super.sparkConf
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .setCHConfig("user_defined_path", "/tmp/user_defined")
      .set(RuntimeConfig.PATH.key, UTSystemParameters.diskOutputDataPath)
      .set(RuntimeConfig.TMP_PATH.key, s"/tmp/libch/$SPARK_DIR_NAME")
    if (UTSystemParameters.testMergeTreeOnObjectStorage) {
      minioHelper.setFileSystem(conf)
      minioHelper.setStoreConfig(conf, BUCKET_NAME)
      hdfsHelper.setFileSystem(conf)
      hdfsHelper.setStoreConfig(conf)
    } else {
      conf
    }
  }

  def clearDataPath(dataPath: String): Unit = {
    val dataPathDir = new File(dataPath)
    if (dataPathDir.exists()) FileUtils.forceDelete(dataPathDir)
  }

  override def beforeAll(): Unit = {
    // if not exists may cause some ut error
    assert(new File(CH_DEFAULT_STORAGE_DIR).exists())

    // prepare working paths
    val basePathDir = new File(dataHome)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    FileUtils.forceMkdir(new File("/tmp/user_defined"))
    FileUtils.forceMkdir(new File(s"/tmp/libch/$SPARK_DIR_NAME"))
    super.beforeAll()
    spark.sparkContext.setLogLevel(logLevel)
    prepareTestTables()
  }

  /**
   * Root path for class resources. Usually, it's the path of the class:
   * `${ProjectDir}/backends-clickhouse/target/scala-2.13/test-classes/`
   */
  final protected val resPath: String = this.getClass.getResource("/").getPath

  // source parquet data
  private val testDataPath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"
  final lazy val testParquetAbsolutePath =
    new File(s"$resPath$testDataPath").getCanonicalPath

  /** Path for storing query files - appends "queries" to `resPath` */
  final protected lazy val queryPath: String = s"${resPath}queries"

  /** data path for tests, depends on diskOutputDataPath config */
  final lazy val dataHome: String =
    if (UTSystemParameters.diskOutputDataPath.equals("/")) resPath + "tests-working-home"
    else UTSystemParameters.diskOutputDataPath + "/" + resPath + "tests-working-home"

  /** Spark warehouse directory for tests */
  final protected val warehouse: String = dataHome + "/spark-warehouse"

  /** Path for storing metadata */
  final protected val metaStorePathAbsolute: String = dataHome + "/meta"

  protected val hiveMetaStoreDB: String =
    s"$metaStorePathAbsolute/${getClass.getSimpleName}/metastore_db"

  final override protected val resourcePath: String = "" // ch not need this
  override protected val fileFormat: String = "parquet"

  protected def testSparkVersionLE33(testName: String, testTag: Tag*)(testFun: => Any): Unit = {
    if (isSparkVersionLE("3.3")) {
      test(testName, testTag: _*)(testFun)
    } else {
      ignore(s"[$SPARK_VERSION_SHORT]-$testName", testTag: _*)(testFun)
    }
  }

  lazy val pruningTimeValueSpark: Int = if (isSparkVersionLE("3.3")) -1 else 0

  override protected def prepareTestTables(): Unit = {}
}

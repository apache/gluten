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
package org.apache.gluten.execution.mergetree

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

import org.scalatest.DoNotDiscover

trait TPCHObjectStore extends TPCHDatabase {
  // parquet data source
  override protected def parquetSourceDB: String = "parquet_source"
  val policy: String
  val remotePath: String

  override protected def createTestTables(): Unit = {
    createTPCHTables(
      s"$remotePath/default",
      format = "clickhouse",
      isNull = false,
      props = Map("storage_policy" -> s"'$policy'") // use the policy to create tables
    )
    insertIntoTPCHTables(parquetSourceDB)
  }
}

/**
 * An experimental configuration trait to find a way to reduce suite configuration for MergeTree
 * data format in Spark tests.
 *
 * This trait centralizes common configuration settings needed across multiple test suites working
 * with MergeTree format, eliminating the need to duplicate these settings in each test class. By
 * extending this trait, test suites gain a standardized configuration foundation while only needing
 * to implement the `useOnePipeline` variable.
 *
 * The trait handles several aspects of configuration:
 *   - Sets up the columnar shuffle management system
 *   - Configures Delta Lake session extensions
 *   - Establishes ClickHouse catalog implementation
 *   - Enables native writer optimizations with appropriate pipeline settings
 *
 * Test suites can customize behavior by defining the `useOnePipeline` value based on their specific
 * testing requirements or Spark version compatibility.
 */

trait WriteMergeTreeConf extends SharedSparkSession {
  val useOnePipeline: Boolean
  override protected def sparkConf: SparkConf = {
    import org.apache.spark.shuffle.sort.ColumnarShuffleManager
    import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog
    import org.apache.spark.sql.internal.SQLConf
    import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
    import io.delta.sql.DeltaSparkSessionExtension

    super.sparkConf
      .set("spark.shuffle.manager", classOf[ColumnarShuffleManager].getName)
      .set(SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[ClickHouseSparkCatalog].getName)
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, useOnePipeline.toString)
  }
}

abstract class GlutenClickHouseMergeTreeTPCHSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with TPCHMergeTreeResult
  with TPCHObjectStore
  with WriteMergeTreeConf {

  override val useOnePipeline: Boolean = spark35
  final override val testCases: Seq[Int] = Seq(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
  )
  setupTestCase()
}

@DoNotDiscover
class MinioTCPHTestSuite extends GlutenClickHouseMergeTreeTPCHSuite {
  override val policy: String = minioHelper.STORE_POLICY_NOCACHE
  override val remotePath: String = s"s3a://$BUCKET_NAME"

  override def beforeAll(): Unit = {
    if (minioHelper.bucketExists(BUCKET_NAME)) {
      minioHelper.clearBucket(BUCKET_NAME)
    }
    minioHelper.createBucket(BUCKET_NAME)
    minioHelper.resetMeta()
    super.beforeAll()
  }
}

@DoNotDiscover
class HDFSTCPHTestSuite extends GlutenClickHouseMergeTreeTPCHSuite {

  override val policy: String = hdfsHelper.STORE_POLICY_ROCKSDB
  override val remotePath: String = hdfsHelper.independentHdfsURL()

  override def beforeAll(): Unit = {
    hdfsHelper.deleteDir(remotePath)
    hdfsHelper.resetMeta()
    super.beforeAll()
  }
}

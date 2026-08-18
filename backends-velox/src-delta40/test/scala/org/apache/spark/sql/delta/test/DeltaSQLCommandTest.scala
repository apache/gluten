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
package org.apache.spark.sql.delta.test

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.config.VeloxDeltaConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sql.DeltaSparkSessionExtension

// spotless:off
/**
 * A trait for tests that are testing a fully set up SparkSession with all of Delta's requirements,
 * such as the configuration of the DeltaCatalog and the addition of all Delta extensions.
 */
trait DeltaSQLCommandTest extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf

    // Delta.
    conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)

    // Gluten.
    conf.set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, "true")
      .set("spark.databricks.delta.snapshotPartitions", "2")
      .set("spark.gluten.sql.fallbackUnexpectedMetadataParquet", "true")
      // Validate every operator's output vector. A Delta deletion-vector write scans only
      // synthesized columns with a pushed-down filter, and on that path Velox emits a
      // RowVector whose row-index child has no rows; the child is then wrapped in a
      // dictionary, and reading it goes out of bounds and yields arbitrary heap values.
      // Without this the failure is intermittent and reports a meaningless row index, which
      // is how it went unnoticed for so long. With it, the scan is caught the moment it
      // produces the malformed vector, so the suite fails deterministically and names the
      // operator responsible.
      //   Velox issue: https://github.com/facebookincubator/velox/issues/18535
      //   Velox fix:   https://github.com/facebookincubator/velox/pull/18536
      // Remove this once that fix is picked up, at which point the suite must go green
      // again -- which is what validates the fix.
      .set("spark.gluten.sql.columnar.backend.velox.validateOutputFromOperators", "true")
  }
}
// spotless:on

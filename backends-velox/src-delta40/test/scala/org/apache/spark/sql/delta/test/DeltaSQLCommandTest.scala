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
      // Bound native memory so a runaway suite (e.g. DeletionVectorsSuite's
      // "2B rows" read, whose Velox native grows to ~13G) hits a HARD per-task
      // cap and throws a clean Velox OOM -- a deterministic, baselineable test
      // failure -- instead of growing until the kernel OOM-kills the whole fork
      // (which wedges sbt into the chronic shard hang). `memory.isolation=true`
      // makes the off-heap pool a hard per-task cap (= offHeap / executorCores);
      // `overAcquiredMemoryRatio=0` drops Gluten's 30% over-acquire backup. The
      // pool is bumped 2g->4g so the per-task cap stays a reasonable ~1-2G while
      // the total managed off-heap stays <= 4g, keeping the fork well under the
      // ~16G runner. NOTE: this is a HARD cap, so other genuinely memory-heavy
      // tests may also start throwing clean OOMs -- the net failure delta is to
      // be measured from the run, not assumed.
      .set("spark.memory.offHeap.size", "4g")
      .set("spark.gluten.memory.isolation", "true")
      .set("spark.gluten.memory.overAcquiredMemoryRatio", "0")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key, "true")
      .set("spark.databricks.delta.snapshotPartitions", "2")
      .set("spark.gluten.sql.fallbackUnexpectedMetadataParquet", "true")
  }
}
// spotless:on

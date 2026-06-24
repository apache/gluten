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

import org.apache.spark.{SparkConf, SparkException, SparkThrowable}
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
  }

  /**
   * Spark master now routes some invalid Delta table-property updates through the newer
   * UNSUPPORTED_TABLE_CHANGE error class while released Spark versions still report the legacy
   * temporary error. Both variants preserve the same message payload.
   */
  protected def checkInvalidBooleanTablePropertyError(
      error: SparkException,
      invalidValue: String): Unit = {
    val sparkThrowable = error.asInstanceOf[SparkThrowable]
    val errorClass = sparkThrowable.getErrorClass()
    val expectedMessage = "For input string: \"" + invalidValue + "\""
    assert(
      Set("_LEGACY_ERROR_TEMP_2045", "UNSUPPORTED_TABLE_CHANGE").contains(errorClass),
      s"Unexpected error class $errorClass with parameters " +
        s"${sparkThrowable.getMessageParameters()}")
    assert(sparkThrowable.getMessageParameters().get("message") == expectedMessage)
  }
}
// spotless:on

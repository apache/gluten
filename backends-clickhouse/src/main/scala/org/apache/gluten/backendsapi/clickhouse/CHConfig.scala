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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.config.{ConfigRegistry, GlutenConfig}

import org.apache.spark.SparkConf
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf

object CHConfig extends ConfigRegistry {
  private[clickhouse] val BACKEND_NAME: String = "ch"
  private[clickhouse] val CONF_PREFIX: String = GlutenConfig.prefixOf(BACKEND_NAME)
  private val RUNTIME_SETTINGS: String = s"$CONF_PREFIX.runtime_settings"
  private val RUNTIME_CONFIG = s"$CONF_PREFIX.runtime_config"
  implicit class GlutenCHConf(conf: SparkConf) {
    def setCHSettings(settings: (String, String)*): SparkConf = {
      settings.foreach { case (k, v) => conf.set(runtimeSettings(k), v) }
      conf
    }

    def setCHSettings[T](k: String, v: T): SparkConf = {
      conf.set(runtimeSettings(k), v.toString)
      conf
    }

    def setCHConfig(config: (String, String)*): SparkConf = {
      config.foreach { case (k, v) => conf.set(runtimeConfig(k), v) }
      conf
    }

    def setCHConfig[T](k: String, v: T): SparkConf = {
      conf.set(runtimeConfig(k), v.toString)
      conf
    }
  }

  /** CH configuration prefix at Java side */
  def prefixOf(key: String): String = s"$CONF_PREFIX.$key"
  def runtimeConfig(key: String): String = s"$RUNTIME_CONFIG.$key"
  def runtimeSettings(key: String): String = s"$RUNTIME_SETTINGS.$key"

  def startWithSettingsPrefix(key: String): Boolean = key.startsWith(RUNTIME_SETTINGS)
  def removeSettingsPrefix(key: String): String = key.substring(RUNTIME_SETTINGS.length + 1)

  def get: CHConfig = new CHConfig(SQLConf.get)

  val ENABLE_ONEPIPELINE_MERGETREE_WRITE =
    buildConf(prefixOf("mergetree.write.pipeline"))
      .doc("Using one pipeline to write data to MergeTree table in Spark 3.5")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_CH_SHUFFLE_SPILL_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.backend.ch.spillThreshold")
      .internal()
      .doc("Shuffle spill threshold on ch backend")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0MB")

  val COLUMNAR_CH_MAX_SORT_BUFFER_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.ch.maxSortBufferSize")
      .internal()
      .doc("The maximum size of sort shuffle buffer in CH backend.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_CH_FORCE_MEMORY_SORT_SHUFFLE =
    buildConf("spark.gluten.sql.columnar.backend.ch.forceMemorySortShuffle")
      .internal()
      .doc("Whether to force to use memory sort shuffle in CH backend. ")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_CH_REWRITE_DATE_CONVERSION =
    buildConf("spark.gluten.sql.columnar.backend.ch.rewrite.dateConversion")
      .internal()
      .doc(
        "Rewrite the conversion between date and string."
          + "For example `to_date(from_unixtime(unix_timestamp(stringType, 'yyyyMMdd')))`"
          + " will be rewritten to `to_date(stringType)`")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_GLUTEN_LOCAL_FILE_CACHE =
    buildConf(runtimeConfig("gluten_cache.local.enabled"))
      .internal()
      .doc("Enable local cache for CH backend.")
      .booleanConf
      .createWithDefault(false)

  val EXTENDED_COLUMNAR_TRANSFORM_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.transform.rules")
      .internal()
      .withAlternative("spark.gluten.sql.columnar.extended.columnar.pre.rules")
      .doc("A comma-separated list of classes for the extended columnar transform rules.")
      .stringConf
      .createWithDefaultString("")

  val EXTENDED_COLUMNAR_POST_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.post.rules")
      .internal()
      .doc("A comma-separated list of classes for the extended columnar post rules.")
      .stringConf
      .createWithDefaultString("")

  val EXTENDED_EXPRESSION_TRAN_CONF =
    buildConf("spark.gluten.sql.columnar.extended.expressions.transformer")
      .internal()
      .doc("A class for the extended expressions transformer.")
      .stringConf
      .createWithDefaultString("")
}

class CHConfig(conf: SQLConf) extends GlutenConfig(conf) {
  import CHConfig._

  def enableOnePipelineMergeTreeWrite: Boolean =
    getConf(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE)

  def chColumnarShuffleSpillThreshold: Long = {
    val threshold = getConf(COLUMNAR_CH_SHUFFLE_SPILL_THRESHOLD)
    if (threshold == 0) {
      (taskOffHeapMemorySize * 0.9).toLong
    } else {
      threshold
    }
  }

  def chColumnarMaxSortBufferSize: Long = getConf(COLUMNAR_CH_MAX_SORT_BUFFER_SIZE)

  def chColumnarForceMemorySortShuffle: Boolean =
    getConf(COLUMNAR_CH_FORCE_MEMORY_SORT_SHUFFLE)

  def enableCHRewriteDateConversion: Boolean =
    getConf(ENABLE_CH_REWRITE_DATE_CONVERSION)

  def enableGlutenLocalFileCache: Boolean = getConf(ENABLE_GLUTEN_LOCAL_FILE_CACHE)

  // A comma-separated list of classes for the extended columnar pre rules
  def extendedColumnarTransformRules: String = getConf(EXTENDED_COLUMNAR_TRANSFORM_RULES)

  // A comma-separated list of classes for the extended columnar post rules
  def extendedColumnarPostRules: String = getConf(EXTENDED_COLUMNAR_POST_RULES)

  def extendedExpressionTransformer: String = getConf(EXTENDED_EXPRESSION_TRAN_CONF)
}

object GlutenObjectStorageConfig {
  val S3_DISK_TYPE: String = "s3_gluten"
  val HDFS_DISK_TYPE: String = "hdfs_gluten"
}

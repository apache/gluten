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
package org.apache.gluten.config

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf

import java.util
import java.util.Locale
import java.util.concurrent.TimeUnit

/*
 * Note: Gluten configiguration.md is automatically generated from this code.
 * Make sure to run dev/gen_all_config_docs.sh after making changes to this file.
 */
class BoltConfig(conf: SQLConf) extends GlutenConfig(conf) {
  import BoltConfig._

  def boltSpillFileSystem: String = getConf(COLUMNAR_BOLT_SPILL_FILE_SYSTEM)

  def boltResizeBatchesShuffleInput: Boolean =
    getConf(COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT)

  def boltResizeBatchesShuffleOutput: Boolean =
    getConf(COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_OUTPUT)

  case class ResizeRange(min: Int, max: Int) {
    assert(max >= min)
    assert(min > 0, "Min batch size should be larger than 0")
    assert(max > 0, "Max batch size should be larger than 0")
  }

  def boltResizeBatchesShuffleInputOutputRange: ResizeRange = {
    val standardSize = getConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE)
    val defaultMinSize: Int = (0.25 * standardSize).toInt.max(1)
    val minSize = getConf(COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT_OUTPUT_MIN_SIZE)
      .getOrElse(defaultMinSize)
    ResizeRange(minSize, Int.MaxValue)
  }

  def boltBloomFilterMaxNumBits: Long = getConf(COLUMNAR_BOLT_BLOOM_FILTER_MAX_NUM_BITS)

  def castFromVarcharAddTrimNode: Boolean = getConf(CAST_FROM_VARCHAR_ADD_TRIM_NODE)

  def enableBoltFlushablePartialAggregation: Boolean =
    getConf(BOLT_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED)

  def enableBroadcastBuildRelationInOffheap: Boolean =
    getConf(BOLT_BROADCAST_BUILD_RELATION_USE_OFFHEAP)

  def broadcastBuildMergeBatches: Boolean =
    getConf(BOLT_BROADCAST_BUILD_MERGE_BATCHES)

  def boltOrcScanEnabled: Boolean =
    getConf(BOLT_ORC_SCAN_ENABLED)

  def enablePropagateIgnoreNullKeys: Boolean =
    getConf(BOLT_PROPAGATE_IGNORE_NULL_KEYS_ENABLED)

  def floatingPointMode: String = getConf(FLOATING_POINT_MODE)

  def enableRewriteCastArrayToString: Boolean =
    getConf(ENABLE_REWRITE_CAST_ARRAY_TO_STRING)

  def enableRewriteUnboundedWindow: Boolean = getConf(ENABLE_REWRITE_UNBOUNDED_WINDOW)

  def enableColumnarProjectPushdown: Boolean = getConf(ENABLE_COLUMNAR_PROJECT_PUSHDOWN)

  def enableColumnarProjectRemove: Boolean = getConf(ENABLE_COLUMNAR_PROJECT_REMOVE)

  def parquetWriterBufferGrowRatio: Double = getConf(GLUTEN_PARQUET_WRITER_BUFFER_GROW_RATIO)

  def parquetWriterBufferReserveRatio: Double =
    getConf(GLUTEN_PARQUET_WRITER_BUFFER_RESERVE_RATIO)

  def enableEnhancedFeatures(): Boolean = ConfigJniWrapper.isEnhancedFeaturesEnabled &&
    getConf(ENABLE_ENHANCED_FEATURES)

  def boltPreferredBatchBytes: Long = getConf(COLUMNAR_BOLT_PREFERRED_BATCH_BYTES)

  def cudfEnableTableScan: Boolean = getConf(CUDF_ENABLE_TABLE_SCAN)

  def forceShuffleWriterType: Int = {
    getConf(FORCE_SHUFFLE_WRITER_TYPE)
  }

  def columnarShuffleCompressionMode: String =
    getConf(COLUMNAR_SHUFFLE_COMPRESSION_MODE)

  def useV2PreallocSizeThreshold: Int =
    getConf(USE_V2_PREALLOC_SIZE_THRESHOLD)

  def rowVectorModeCompressionMinColumns: Int =
    getConf(ROWVECTOR_MODE_COMPRESSION_MIN_COLUMNS)

  def rowvectorModeCompressionMaxBufferSize: Int =
    getConf(ROWVECTOR_MODE_COMPRESSION_MAX_BUFFER_SIZE)

  def accumulateBatchMaxColumns: Int =
    getConf(ACCUMULATE_BATCH_IN_SHUFFLE_V1_MAX_COLUMNS)

  def accumulateBatchMaxBatches: Int =
    getConf(ACCUMULATE_BATCH_IN_SHUFFLE_V1_MAX_BATCHES)

  def enableVectorCombination: Boolean =
    getConf(COMBINED_VECTOR_ENABLED)

  def recommendedColumn2RowSize: Int = {
    getConf(RECOMMENDED_COLUMN2ROW_SIZE)
  }

  def shuffleCheckRatio: Double = {
    getConf(COLUMNAR_BOLT_SHUFFLE_CHECK_RATIO)
  }

  def maxShuffleBatchByteSize: Int = getConf(COLUMNAR_MAX_SHUFFLE_BATCH_BYTE_SIZE)

  def shuffleInsideBolt: Boolean =
    getConf(GLUTEN_SHUFFLE_INSIDE_BOLT)

  def orcUseColumnNames: Boolean = getConf(ORC_USE_COLUMN_NAMES)

  def parquetUseColumnNames: Boolean = getConf(PARQUET_USE_COLUMN_NAMES)
}

object BoltConfig extends ConfigRegistry {
  val GLUTEN_PARALLEL_ENABLED_KEY = "spark.gluten.parallel.enabled"
  val GLUTEN_PARALLEL_ENABLED_KEY_DEFAULT = false
  val USE_BOLT_MEMORY_MANAGER_KEY = "spark.gluten.useBoltMemoryManager"
  val BOLT_MEMORY_MANAGER_MAX_WAIT_TIME_WHEN_FREE_KEY =
    "spark.gluten.boltMemoryManager.maxWaitTimeWhenFree"
  val BOLT_EXECUTION_POOL_MIN_MEMORY_MAX_WAIT_TIME_KEY =
    "spark.gluten.boltExecutionPool.minMemoryMaxWaitTime"
  val GLUTEN_PREFETCH_MEMORY_PERCENT_CONF = "spark.gluten.bolt.prefetch.memory.percent"
  val GLUTEN_PRELOAD_ENABLED_CONF = "spark.gluten.sql.columnar.backend.bolt.preload.enabled"
  val PARQUET_ROW_NUM_IN_EACH_BLOCK = "parquet.block.rowNumInEachBlock"
  val GLUTEN_MAX_SHUFFLE_BATCH_BYTE_SIZE_KEY = "spark.gluten.sql.columnar.maxShuffleBatchByteSize"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_DECRYPT_ENABLED =
    "spark.hadoop.parquet.encryption.decrypt.enabled"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INS_URL =
    "spark.hadoop.parquet.encryption.kms.instance.url"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INS_ID =
    "spark.hadoop.parquet.encryption.kms.instance.id"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_ACCESS_TOKEN =
    "spark.hadoop.parquet.encryption.key.access.token"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_CACHE_LIFETIME_SECONDS =
    "spark.hadoop.parquet.encryption.cache.lifetime.seconds"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_MAX_RETRY_TIMES =
    "spark.hadoop.parquet.encryption.kms.client.max.retry.times"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INSTANCE_VERSION =
    "spark.hadoop.parquet.encryption.kms.instance.version"
  val SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_CLIENT_CLASS =
    "spark.hadoop.parquet.encryption.kms.client.class"
  val NATIVE_ARROW_READER_ENABLED =
    buildConf("spark.gluten.sql.native.arrow.reader.enabled")
      .doc("This is config to specify whether to enable the native columnar csv reader")
      .booleanConf
      .createWithDefault(false)
  val ENABLE_COLUMNAR_PROJECT_PUSHDOWN =
    buildConf("spark.gluten.sql.columnar.project.pushdown")
      .internal()
      .doc("Splits project to function and field access and pushdown function before generate")
      .booleanConf
      .createWithDefault(false)
  val ENABLE_COLUMNAR_PROJECT_REMOVE =
    buildConf("spark.gluten.sql.columnar.project.remove")
      .internal()
      .doc("remove unnecessary project before generate")
      .booleanConf
      .createWithDefault(false)
  val GLUTEN_PARQUET_WRITER_BUFFER_GROW_RATIO =
    buildConf("spark.gluten.sql.parquet.writer.bufferGrowRatio")
      .internal()
      .doubleConf
      .createWithDefault(1)
  val GLUTEN_PARQUET_WRITER_BUFFER_RESERVE_RATIO =
    buildConf("spark.gluten.sql.parquet.writer.bufferReserveRatio")
      .internal()
      .doubleConf
      .createWithDefault(0)

  override def get: BoltConfig = {
    new BoltConfig(SQLConf.get)
  }

  lazy val EXTRA_NATIVE_SESSION_CONF_KEYS: Set[String] = Set(
    GLUTEN_PARALLEL_ENABLED_KEY,
    USE_BOLT_MEMORY_MANAGER_KEY,
    BOLT_MEMORY_MANAGER_MAX_WAIT_TIME_WHEN_FREE_KEY,
    BOLT_EXECUTION_POOL_MIN_MEMORY_MAX_WAIT_TIME_KEY,
    BOLT_MEMORY_MANAGER_ENABLE_DYNAMIC_MEMORY_QUOTA_MANAGER.key,
    BOLT_MEMORY_MANAGER_DYNAMIC_MEMORY_QUOTA_MANAGER_RATIOS.key,
    GLUTEN_MAX_SHUFFLE_BATCH_BYTE_SIZE_KEY,
    SPARK_HADOOP_PARQUET_ENCRYPTION_DECRYPT_ENABLED,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INS_URL,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INS_ID,
    SPARK_HADOOP_PARQUET_ENCRYPTION_ACCESS_TOKEN,
    SPARK_HADOOP_PARQUET_ENCRYPTION_CACHE_LIFETIME_SECONDS,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_MAX_RETRY_TIMES,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INSTANCE_VERSION,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_CLIENT_CLASS
  )

  lazy val EXTRA_NATIVE_BACKEND_CONF_KEYS: Set[String] = Set(
    "spark.executor.cores",
    "spark.vcore.boost.ratio",
    GLUTEN_PARALLEL_ENABLED_KEY,
    GLUTEN_MAX_SHUFFLE_BATCH_BYTE_SIZE_KEY,
    SPARK_HADOOP_PARQUET_ENCRYPTION_DECRYPT_ENABLED,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INS_URL,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INS_ID,
    SPARK_HADOOP_PARQUET_ENCRYPTION_ACCESS_TOKEN,
    SPARK_HADOOP_PARQUET_ENCRYPTION_CACHE_LIFETIME_SECONDS,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_MAX_RETRY_TIMES,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_INSTANCE_VERSION,
    SPARK_HADOOP_PARQUET_ENCRYPTION_KMS_CLIENT_CLASS,
    GLUTEN_PARQUET_WRITER_BUFFER_GROW_RATIO.key,
    GLUTEN_PARQUET_WRITER_BUFFER_RESERVE_RATIO.key
  )

  def postProcessNativeBackendConf(nativeConfMap: util.Map[String, String]): Unit = {
    val numTaskSlots = nativeConfMap.getOrDefault(
      GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR.key,
      GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR.defaultValueString)
    val defaults = Seq(
      "spark.gluten.bolt.fs.s3a.connect.timeout" -> "200s",
      "spark.gluten.bolt.fs.s3a.retry.mode" -> "legacy",
      "spark.gluten.sql.columnar.backend.bolt.IOThreads" -> numTaskSlots,
      "spark.gluten.sql.columnar.backend.bolt.fileHandleCacheEnabled" -> "false",
      "spark.gluten.bolt.awsSdkLogLevel" -> "FATAL",
      "spark.gluten.bolt.s3UseProxyFromEnv" -> "false",
      "spark.gluten.bolt.s3PayloadSigningPolicy" -> "Never",
      GLUTEN_PARALLEL_ENABLED_KEY -> GLUTEN_PARALLEL_ENABLED_KEY_DEFAULT.toString
    )

    defaults.foreach {
      case (key, defaultValue) =>
        if (!nativeConfMap.containsKey(key)) {
          nativeConfMap.put(key, defaultValue)
        }
    }
  }

  // bolt caching options.
  val COLUMNAR_BOLT_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.cacheEnabled")
      .doc(
        "Enable Bolt cache, default off. It's recommended to enable" +
          "soft-affinity as well when enable bolt cache.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_MEM_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.memCacheSize")
      .doc("The memory cache size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_BOLT_MEM_INIT_CAPACITY =
    buildConf("spark.gluten.sql.columnar.backend.bolt.memInitCapacity")
      .doc("The initial memory capacity to reserve for a newly created Bolt query memory pool.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  val COLUMNAR_BOLT_MEM_RECLAIM_MAX_WAIT_MS =
    buildConf("spark.gluten.sql.columnar.backend.bolt.reclaimMaxWaitMs")
      .doc("The max time in ms to wait for memory reclaim.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(60))

  val COLUMNAR_BOLT_MEMORY_POOL_CAPACITY_TRANSFER_ACROSS_TASKS =
    buildConf("spark.gluten.sql.columnar.backend.bolt.memoryPoolCapacityTransferAcrossTasks")
      .doc("Whether to allow memory capacity transfer between memory pools from different tasks.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BOLT_SSD_CACHE_PATH =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdCachePath")
      .doc("The folder to store the cache files, better on SSD")
      .stringConf
      .createWithDefault("/tmp")

  val COLUMNAR_BOLT_SSD_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdCacheSize")
      .doc("The SSD cache size, will do memory caching only if this value = 0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_BOLT_SSD_CACHE_SHARDS =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdCacheShards")
      .doc("The cache shards")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_BOLT_SSD_CACHE_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdCacheIOThreads")
      .doc("The IO threads for cache promoting")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_BOLT_SSD_ODIRECT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdODirect")
      .doc("The O_DIRECT flag for cache writing")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_SSD_CHCEKPOINT_DISABLE_FILE_COW =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdDisableFileCow")
      .doc("True if copy on write should be disabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_SSD_CHCEKPOINT_CHECKSUM_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdChecksumEnabled")
      .doc("If true, checksum write to SSD is enabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_SSD_CHCEKPOINT_CHECKSUM_READ_VERIFICATION_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdChecksumReadVerificationEnabled")
      .doc("If true, checksum read verification from SSD is enabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_SSD_CHCEKPOINT_INTERVAL_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.ssdCheckpointIntervalBytes")
      .doc(
        "Checkpoint after every 'checkpointIntervalBytes' for SSD cache. " +
          "0 means no checkpointing.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_BOLT_CONNECTOR_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.IOThreads")
      .doc(
        "The Size of the IO thread pool in the Connector. " +
          "This thread pool is used for split preloading and DirectBufferedInput. " +
          "By default, the value is the same as the maximum task slots per Spark executor.")
      .intConf
      .createWithDefault(16)

  val COLUMNAR_BOLT_ASYNC_TIMEOUT =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.asyncTimeoutOnTaskStopping")
      .doc(
        "Timeout for asynchronous execution when task is being stopped in Bolt backend. " +
          "It's recommended to set to a number larger than network connection timeout that the " +
          "possible aysnc tasks are relying on.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(30000)

  val COLUMNAR_BOLT_SPLIT_PRELOAD_PER_DRIVER =
    buildConf("spark.gluten.sql.columnar.backend.bolt.SplitPreloadPerDriver")
      .doc("The split preload per task")
      .intConf
      .createWithDefault(2)

  val COLUMNAR_BOLT_GLOG_VERBOSE_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.bolt.glogVerboseLevel")
      .internal()
      .doc("Set glog verbose level in Bolt backend, same as FLAGS_v.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_BOLT_GLOG_SEVERITY_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.bolt.glogSeverityLevel")
      .internal()
      .doc("Set glog severity level in Bolt backend, same as FLAGS_minloglevel.")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_BOLT_SPILL_STRATEGY =
    buildConf("spark.gluten.sql.columnar.backend.bolt.spillStrategy")
      .doc("none: Disable spill on Bolt backend; " +
        "auto: Let Spark memory manager manage Bolt's spilling")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("none", "auto"))
      .createWithDefault("auto")

  val COLUMNAR_BOLT_MAX_SPILL_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxSpillLevel")
      .doc("The max allowed spilling level with zero being the initial spilling level")
      .intConf
      .createWithDefault(4)

  val COLUMNAR_BOLT_MAX_SPILL_FILE_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxSpillFileSize")
      .doc("The maximum size of a single spill file created")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_BOLT_SPILL_FILE_SYSTEM =
    buildConf("spark.gluten.sql.columnar.backend.bolt.spillFileSystem")
      .doc(
        "The filesystem used to store spill data. local: The local file system. " +
          "heap-over-local: Write file to JVM heap if having extra heap space. " +
          "Otherwise write to local file system.")
      .stringConf
      .checkValues(Set("local", "heap-over-local"))
      .createWithDefaultString("local")

  val COLUMNAR_BOLT_MAX_SPILL_RUN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxSpillRunRows")
      .doc("The maximum row size of a single spill run")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("3M")

  val COLUMNAR_BOLT_MAX_SPILL_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxSpillBytes")
      .doc("The maximum file size of a query")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("100G")

  val MAX_PARTITION_PER_WRITERS_SESSION =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxPartitionsPerWritersSession")
      .doc("Maximum number of partitions per a single table writer instance.")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(10000)

  val COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT =
    buildConf("spark.gluten.sql.columnar.backend.bolt.resizeBatches.shuffleInput")
      .doc(
        s"If true, combine small columnar batches together before sending to shuffle. " +
          s"The default minimum output batch size is equal to 0.25 * " +
          s"${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key}")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_OUTPUT =
    buildConf("spark.gluten.sql.columnar.backend.bolt.resizeBatches.shuffleOutput")
      .doc(
        s"If true, combine small columnar batches together right after shuffle read. " +
          s"The default minimum output batch size is equal to 0.25 * " +
          s"${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key}")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.bolt.resizeBatches.shuffleInput.minSize")
      .doc(
        s"The minimum batch size for shuffle. If size of an input batch is " +
          s"smaller than the value, it will be combined with other " +
          s"batches before sending to shuffle. Only functions when " +
          s"${COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT.key} is set to true. " +
          s"Default value: 0.25 * <max batch size>")
      .intConf
      .createOptional

  val COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT_OUTPUT_MIN_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.bolt.resizeBatches.shuffleInputOuptut.minSize")
      .doc(
        s"The minimum batch size for shuffle input and output. " +
          s"If size of an input batch is " +
          s"smaller than the value, it will be combined with other " +
          s"batches before sending to shuffle. " +
          s"The same applies for batches output by shuffle read. " +
          s"Only functions when " +
          s"${COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT.key} or " +
          s"${COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_OUTPUT.key} is set to true. " +
          s"Default value: 0.25 * <max batch size>")
      .fallbackConf(COLUMNAR_BOLT_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE)

  val COLUMNAR_BOLT_ENABLE_USER_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.bolt.enableUserExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for user type of BoltException")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BOLT_SHOW_TASK_METRICS_WHEN_FINISHED =
    buildConf("spark.gluten.sql.columnar.backend.bolt.showTaskMetricsWhenFinished")
      .doc("Show bolt full task metrics when finished.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_TASK_METRICS_TO_EVENT_LOG_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.backend.bolt.taskMetricsToEventLog.threshold")
      .internal()
      .doc("Sets the threshold in seconds for writing task statistics to the event log if the " +
        "task runs longer than this value. Configuring the value >=0 can enable the feature. " +
        "0 means all tasks report and save the metrics to eventlog. value <0 disable the feature.")
      .timeConf(TimeUnit.SECONDS)
      .createOptional

  val COLUMNAR_BOLT_MEMORY_USE_HUGE_PAGES =
    buildConf("spark.gluten.sql.columnar.backend.bolt.memoryUseHugePages")
      .doc("Use explicit huge pages for Bolt memory allocation.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_BOLT_ENABLE_SYSTEM_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.bolt.enableSystemExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for system type of BoltException")
      .booleanConf
      .createWithDefault(true)

  val BOLT_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.bolt.flushablePartialAggregation")
      .doc(
        "Enable flushable aggregation. If true, Gluten will try converting regular aggregation " +
          "into Bolt's flushable aggregation when applicable. A flushable aggregation could " +
          "emit intermediate result at anytime when memory is full / data reduction ratio is low."
      )
      .booleanConf
      .createWithDefault(true)

  val MAX_PARTIAL_AGGREGATION_MEMORY =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxPartialAggregationMemory")
      .doc(
        "Set the max memory of partial aggregation in bytes. When this option is set to a " +
          "value greater than 0, it will override spark.gluten.sql.columnar.backend.bolt." +
          "maxPartialAggregationMemoryRatio. Note: this option only works when flushable " +
          "partial aggregation is enabled. Ignored when spark.gluten.sql.columnar.backend." +
          "bolt.flushablePartialAggregation=false."
      )
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val MAX_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxPartialAggregationMemoryRatio")
      .doc(
        "Set the max memory of partial aggregation as "
          + "maxPartialAggregationMemoryRatio of offheap size. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.bolt.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.1)

  val MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxExtendedPartialAggregationMemoryRatio")
      .doc(
        "Set the max extended memory of partial aggregation as "
          + "maxExtendedPartialAggregationMemoryRatio of offheap size. Note: this option only " +
          "works when flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.bolt.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.15)

  val ABANDON_PARTIAL_AGGREGATION_MIN_PCT =
    buildConf("spark.gluten.sql.columnar.backend.bolt.abandonPartialAggregationMinPct")
      .doc(
        "If partial aggregation aggregationPct greater than this value, "
          + "partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.bolt.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(90)

  val ABANDON_PARTIAL_AGGREGATION_MIN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.bolt.abandonPartialAggregationMinRows")
      .doc(
        "If partial aggregation input rows number greater than this value, "
          + " partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.bolt.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(100000)

  val COLUMNAR_BOLT_BLOOM_FILTER_EXPECTED_NUM_ITEMS =
    buildConf("spark.gluten.sql.columnar.backend.bolt.bloomFilter.expectedNumItems")
      .doc(
        "The default number of expected items for the bolt bloomfilter: " +
          "'spark.bloom_filter.expected_num_items'")
      .longConf
      .createWithDefault(1000000L)

  val COLUMNAR_BOLT_BLOOM_FILTER_NUM_BITS =
    buildConf("spark.gluten.sql.columnar.backend.bolt.bloomFilter.numBits")
      .doc(
        "The default number of bits to use for the bolt bloom filter: " +
          "'spark.bloom_filter.num_bits'")
      .longConf
      .createWithDefault(8388608L)

  val COLUMNAR_BOLT_BLOOM_FILTER_MAX_NUM_BITS =
    buildConf("spark.gluten.sql.columnar.backend.bolt.bloomFilter.maxNumBits")
      .doc(
        "The max number of bits to use for the bolt bloom filter: " +
          "'spark.bloom_filter.max_num_bits'")
      .longConf
      .createWithDefault(4194304L)

  val COLUMNAR_BOLT_FILE_HANDLE_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.fileHandleCacheEnabled")
      .doc(
        "Disables caching if false. File handle cache should be disabled " +
          "if files are mutable, i.e. file content may change while file path stays the same.")
      .booleanConf
      .createWithDefault(false)

  val DIRECTORY_SIZE_GUESS =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.directorySizeGuess")
      .doc("Deprecated, rename to spark.gluten.sql.columnar.backend.bolt.footerEstimatedSize")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FOOTER_ESTIMATED_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.footerEstimatedSize")
      .doc("Set the footer estimated size for bolt file scan, " +
        "refer to Bolt's footer-estimated-size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FILE_PRELOAD_THRESHOLD =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.filePreloadThreshold")
      .doc("Set the file preload threshold for bolt file scan, " +
        "refer to Bolt's file-preload-threshold")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1MB")

  val PREFETCH_ROW_GROUPS =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.prefetchRowGroups")
      .doc("Set the prefetch row groups for bolt file scan")
      .intConf
      .createWithDefault(6)

  val LOAD_QUANTUM =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.loadQuantum")
      .doc("Set the load quantum for bolt file scan, recommend to use the default value (256MB) " +
        "for performance consideration. If Bolt cache is enabled, it can be 8MB at most.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256MB")

  val MAX_COALESCED_DISTANCE_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.maxCoalescedDistance")
      .doc(" Set the max coalesced distance bytes for bolt file scan")
      .stringConf
      .createWithDefaultString("512KB")

  val MAX_COALESCED_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.maxCoalescedBytes")
      .doc("Set the max coalesced bytes for bolt file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64MB")

  val CACHE_PREFETCH_MINPCT =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.cachePrefetchMinPct")
      .doc("Set prefetch cache min pct for bolt file scan")
      .intConf
      .createWithDefault(0)

  val AWS_SDK_LOG_LEVEL =
    buildConf("spark.gluten.bolt.awsSdkLogLevel")
      .internal()
      .doc("Log granularity of AWS C++ SDK in bolt.")
      .stringConf
      .createWithDefault("FATAL")

  val AWS_S3_RETRY_MODE =
    buildConf("spark.gluten.bolt.fs.s3a.retry.mode")
      .internal()
      .doc("Retry mode for AWS s3 connection error: legacy, standard and adaptive.")
      .stringConf
      .createWithDefault("legacy")

  val AWS_S3_CONNECT_TIMEOUT =
    buildConf("spark.gluten.bolt.fs.s3a.connect.timeout")
      .doc("Timeout for AWS s3 connection.")
      .stringConf
      .createWithDefault("200s")

  val BOLT_ORC_SCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.bolt.orc.scan.enabled")
      .doc("Enable bolt orc scan. If disabled, vanilla spark orc scan will be used.")
      .booleanConf
      .createWithDefault(true)

  val CAST_FROM_VARCHAR_ADD_TRIM_NODE =
    buildConf("spark.gluten.bolt.castFromVarcharAddTrimNode")
      .doc(
        "If true, will add a trim node " +
          "which has the same sementic as vanilla Spark to CAST-from-varchar." +
          "Otherwise, do nothing.")
      .booleanConf
      .createWithDefault(false)

  val BOLT_BROADCAST_BUILD_RELATION_USE_OFFHEAP =
    buildConf("spark.gluten.bolt.offHeapBroadcastBuildRelation.enabled")
      .experimental()
      .doc("Experimental: If enabled, broadcast build relation will use offheap memory. " +
        "Otherwise, broadcast build relation will use onheap memory.")
      .booleanConf
      .createWithDefault(false)

  val BOLT_BROADCAST_BUILD_MERGE_BATCHES =
    buildConf("spark.gluten.bolt.broadcastBuild.mergeBatches")
      .doc(
        "If enabled, all columnar batches in a broadcast build relation will be " +
          "serialized into a single buffer to reduce the number of addInput calls in " +
          "HashBuild operator. This can significantly improve BHJ performance when " +
          "the broadcast table has many small batches, but may increase driver-side " +
          "peak memory and is not suitable for very large broadcasts.")
      .booleanConf
      .createWithDefault(false)

  val BOLT_HASHMAP_ABANDON_BUILD_DUPHASH_MIN_ROWS =
    buildConf("spark.gluten.bolt.abandonbuild.noduphashminrows")
      .experimental()
      .doc("Experimental: abandon hashmap build if duplicated rows more than this number.")
      .intConf
      .createWithDefault(100000)

  val BOLT_HASHMAP_ABANDON_BUILD_DUPHASH_MIN_PCT =
    buildConf("spark.gluten.bolt.abandonbuild.noduphashminpct")
      .experimental()
      .doc("Experimental: abandon hashmap build if duplicated rows are more than this percentile.")
      .doubleConf
      .createWithDefault(0)

  val QUERY_TRACE_ENABLED = buildConf("spark.gluten.sql.columnar.backend.bolt.queryTraceEnabled")
    .doc("Enable query tracing flag.")
    .booleanConf
    .createWithDefault(false)

  val QUERY_TRACE_DIR = buildConf("spark.gluten.sql.columnar.backend.bolt.queryTraceDir")
    .internal()
    .doc("Base dir of a query to store tracing data.")
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_NODE_IDS = buildConf("spark.gluten.sql.columnar.backend.bolt.queryTraceNodeIds")
    .internal()
    .doc("A comma-separated list of plan node ids whose input data will be traced. " +
      "Empty string if only want to trace the query metadata.")
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_MAX_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.bolt.queryTraceMaxBytes")
      .internal()
      .doc("The max trace bytes limit. Tracing is disabled if zero.")
      .longConf
      .createWithDefault(0)

  val QUERY_TRACE_TASK_REG_EXP =
    buildConf("spark.gluten.sql.columnar.backend.bolt.queryTraceTaskRegExp")
      .internal()
      .doc("The regexp of traced task id. We only enable trace on a task if its id matches.")
      .stringConf
      .createWithDefault("")

  val OP_TRACE_DIRECTORY_CREATE_CONFIG =
    buildConf("spark.gluten.sql.columnar.backend.bolt.opTraceDirectoryCreateConfig")
      .internal()
      .doc(
        "Config used to create operator trace directory. This config is provided to" +
          " underlying file system and the config is free form. The form should be " +
          "defined by the underlying file system.")
      .stringConf
      .createWithDefault("")

  val BOLT_PROPAGATE_IGNORE_NULL_KEYS_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.bolt.propagateIgnoreNullKeys")
      .doc(
        "If enabled, we will identify aggregation followed by an inner join " +
          "on the grouping keys, and mark the ignoreNullKeys flag to true to " +
          "avoid unnecessary aggregation on null keys.")
      .booleanConf
      .createWithDefault(true)

  val FLOATING_POINT_MODE =
    buildConf("spark.gluten.sql.columnar.backend.bolt.floatingPointMode")
      .doc(
        "Config used to control the tolerance of floating point operations alignment with Spark. " +
          "When the mode is set to strict, flushing is disabled for sum(float/double)" +
          "and avg(float/double). When set to loose, flushing will be enabled.")
      .stringConf
      .checkValues(Set("loose", "strict"))
      .createWithDefault("loose")

  val COLUMNAR_BOLT_MEMORY_CHECK_USAGE_LEAK =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.checkUsageLeak")
      .doc("Enable check memory usage leak.")
      .booleanConf
      .createWithDefault(true)

  val CUDF_MEMORY_RESOURCE =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.cudf.memoryResource")
      .doc("GPU RMM memory resource.")
      .stringConf
      .checkValues(Set("cuda", "pool", "async", "arena", "managed", "managed_pool"))
      .createWithDefault("async")

  val CUDF_MEMORY_PERCENT =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.cudf.memoryPercent")
      .doc("The initial percent of GPU memory to allocate for memory resource for one thread.")
      .intConf
      .createWithDefault(50)

  val CUDF_ENABLE_TABLE_SCAN =
    buildStaticConf("spark.gluten.sql.columnar.backend.bolt.cudf.enableTableScan")
      .doc("Enable cudf table scan")
      .booleanConf
      .createWithDefault(false)

  val MEMORY_DUMP_ON_EXIT =
    buildConf("spark.gluten.monitor.memoryDumpOnExit")
      .internal()
      .doc(
        "Whether to trigger native memory dump when executor exits. Currently it uses jemalloc" +
          " for memory profiling, so if you want to enable it, also need to  build gluten" +
          " with `--enable_jemalloc_stats=ON`.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_REWRITE_CAST_ARRAY_TO_STRING =
    buildConf("spark.gluten.sql.rewrite.castArrayToString")
      .doc(
        "When true, rewrite `cast(array as String)` to" +
          " `concat('[', array_join(array, ', ', null), ']')` to allow offloading to Bolt.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_REWRITE_UNBOUNDED_WINDOW =
    buildConf("spark.gluten.sql.rewrite.unboundedWindow")
      .internal()
      .doc("When true, rewrite unbounded window to an equivalent aggregate join operation" +
        " to avoid OOM.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_ENHANCED_FEATURES =
    buildConf("spark.gluten.sql.enable.enhancedFeatures")
      .doc("Enable some features including iceberg native write and other features.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BOLT_PREFERRED_BATCH_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.bolt.preferredBatchBytes")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("10MB")

  val BOLT_MAX_COMPILED_REGEXES =
    buildConf("spark.gluten.sql.columnar.backend.bolt.maxCompiledRegexes")
      .doc(
        "Controls maximum number of compiled regular expression patterns per function " +
          "instance per thread of execution.")
      .intConf
      .createWithDefault(100)

  val USE_BOLT_MEMORY_MANAGER =
    buildConf(USE_BOLT_MEMORY_MANAGER_KEY)
      .internal()
      .doc("Use bolt memory manager to manage offheap memory.")
      .booleanConf
      .createWithDefault(true)

  val BOLT_MEMORY_MANAGER_MAX_WAIT_TIME_WHEN_FREE =
    buildConf(BOLT_MEMORY_MANAGER_MAX_WAIT_TIME_WHEN_FREE_KEY).longConf
      .createWithDefault(180000)

  val BOLT_MEMORY_MANAGER_ENABLE_DYNAMIC_MEMORY_QUOTA_MANAGER =
    buildConf("spark.gluten.boltMemoryManager.enableDynamicMemoryQuotaManager")
      .internal()
      .doc(
        "Decide whether to enable the DynamicMemoryQuotaManager function in "
          + "BoltMemoryManager. This function calculates the memory hole ratio of the "
          + "process by monitoring the RSS of the process, and then increases the memory" +
          " quota in the same proportion, which can increase memory utilization.")
      .booleanConf
      .createWithDefault(false)

  val BOLT_MEMORY_MANAGER_DYNAMIC_MEMORY_QUOTA_MANAGER_RATIOS =
    buildConf("spark.gluten.boltMemoryManager.dynamicMemoryQuotaManager.ratios")
      .internal()
      .doc(
        "The last character of the parameter is a delimiter. The program will "
          + "use the delimiter to split the parameter list. If the number of parameters is"
          + " incorrect, an error will be reported."
          + "The first digit is the threshold for triggering the dynamicMemoryQuotaManager "
          + "function. The value ranges from 0 to 1.0. The default value is 0.5. The purpose "
          + "is to use as much memory as possible before calculating the memory physical page "
          + "mapping ratio. The mapping ratio will be relatively accurate."
          + "The second and third digits are the expected mapping ratios. The values are "
          + "between 0 and 1.0. The default values are 0.9 and 1.0 respectively. The parameters "
          + "indicate that if the memory physical page mapping ratio is less than 0.9 or greater"
          + " than 1.0, the dynamicMemoryQuotaManager function will be triggered."
          + "The 4th and 5th digits are the upper and lower limits of the over-issuance "
          + "ratio of the final Memory Quota. There is no limit on the value. The default values "
          + "are 1.0 and 6.0. The lower limit of over-issuance is 1.0 times (no over-issuance), "
          + "and the upper limit of over-issuance is 6.0 times."
          + "The 6th digit is the scaling ratio, which ranges from 0 to 1.0, and the default "
          + "value is 1.0, which means that the calculated Memory Quota over-issuance value is "
          + "added to the original Quota without scaling or enlarging it."
          + "The 7th digit is the sampling ratio, which ranges from 0 to 1.0, and the default "
          + "value is 0.1, which means that a detection is performed when the cumulative increase "
          + "in Quota is greater than 10%. This parameter can effectively reduce the occurrence "
          + "of CgroupKill errors, but if the parameter is too small, it will increase the cost "
          + "of observation and adjustment."
          + "The 8th digit is the threshold for the change ratio, which ranges from 0 to 1.0, "
          + "and the default value is 0.0, which means that no matter how much the calculated "
          + "over-issuance value changes from the previous Quota over-issuance value, it will "
          + "be adopted. If it is set to 0.1, it means that the original Quota value must be "
          + "increased by more than 10% or decreased by more than 10% compared with the previous "
          + "value, and the calculated Quota over-issuance value will be adopted."
          + "The 9th digit is the log printing frequency control parameter, with a value between"
          + " 0 and 1.0. The default value is 0.05, which means that a log is printed with a "
          + "probability of 5%. A value greater than 1.0 means that a log is definitely printed."
          + " The purpose of this parameter is to control the log printing frequency, because"
          + " the test found that the log printing overhead is roughly equivalent to the quota"
          + " adjustment overhead.")
      .stringConf
      .createWithDefault("0.5|0.9|1.0|1.0|6.0|1.0|0.05|0.0|0.05|")

  val BOLT_EXECUTION_POOL_MIN_MEMORY_MAX_WAIT_TIME =
    buildConf(BOLT_EXECUTION_POOL_MIN_MEMORY_MAX_WAIT_TIME_KEY).longConf
      .createWithDefault(300000)

  val GLUTEN_PREFETCH_MEMORY_PERCENT =
    buildConf(GLUTEN_PREFETCH_MEMORY_PERCENT_CONF)
      .internal()
      .doc("The memory percent to prefetch.")
      .intConf
      .checkValue(value => value >= 0 && value <= 100, "must be between 0 and 100")
      .createWithDefault(50)

  val GLUTEN_PRELOAD_ENABLED =
    buildConf(GLUTEN_PRELOAD_ENABLED_CONF)
      .internal()
      .doc("Enable preload or not, 0 for disable, 1 for adaptive enable, -1 for force enable")
      .intConf
      .checkValue(value => value == 0 || value == 1 || value == -1, "must be 0, 1 or -1")
      .createWithDefault(1)

  val GLUTEN_PARALLEL_ENABLED =
    buildConf(GLUTEN_PARALLEL_ENABLED_KEY)
      .internal()
      .doc("Enable parallel executio of Bolt tasks in Gluten")
      .booleanConf
      .createWithDefault(GLUTEN_PARALLEL_ENABLED_KEY_DEFAULT)

  val COLUMNAR_SHUFFLE_COMPRESSION_MODE =
    buildConf("spark.gluten.sql.columnar.shuffle.compressionMode")
      .internal()
      .doc("buffer means compress each buffer to pre allocated big buffer," +
        "rowvector means to copy the buffers to a big buffer, and then compress the buffer")
      .stringConf
      .checkValues(Set("buffer", "rowvector"))
      .createWithDefault("rowvector")

  val FORCE_SHUFFLE_WRITER_TYPE =
    buildConf("spark.gluten.sql.columnar.shuffle.forceShuffleWriterType")
      .internal()
      .intConf
      .checkValue(
        v => v >= 0 && v <= 3,
        "ShuffleWriterType should be 0(adaptive), 1(V1) or 2(V2) or 3(Sort-Based Row-Format)")
      .createWithDefault(0)

  val USE_V2_PREALLOC_SIZE_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.useV2PreallocSizeThreshold")
      .internal()
      .intConf
      .createWithDefault(2000)

  val ROWVECTOR_MODE_COMPRESSION_MIN_COLUMNS =
    buildConf("spark.gluten.sql.columnar.shuffle.rowvectorCompressionModeMinColumns")
      .internal()
      .intConf
      .createWithDefault(5)

  val ROWVECTOR_MODE_COMPRESSION_MAX_BUFFER_SIZE =
    buildConf("spark.gluten.sql.columnar.shuffle.rowvectorCompressionModeMaxBufferSize")
      .internal()
      .intConf
      .createWithDefault(5 * 1024 * 1024)

  val ACCUMULATE_BATCH_IN_SHUFFLE_V1_MAX_COLUMNS =
    buildConf("spark.gluten.sql.columnar.shuffle.accumulateBatchMaxColumns")
      .internal()
      .intConf
      .createWithDefault(8)

  val ACCUMULATE_BATCH_IN_SHUFFLE_V1_MAX_BATCHES =
    buildConf("spark.gluten.sql.columnar.shuffle.accumulateBatchMaxBatches")
      .internal()
      .intConf
      .createWithDefault(65535)

  val COMBINED_VECTOR_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffle.combinedVectorEnabled")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val RECOMMENDED_COLUMN2ROW_SIZE =
    buildConf("spark.gluten.sql.columnar.shuffle.recommendedColumn2RowSize")
      .internal()
      .intConf
      .createWithDefault(400 * 1024)

  val COLUMNAR_BOLT_SHUFFLE_CHECK_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.bolt.shuffle.check.ratio")
      .internal()
      .doc("The ratio in [0, 1] for enabling bolt shuffle debug check.")
      .doubleConf
      .checkValue(v => v >= 0.0 && v <= 1.0, "must be in [0, 1]")
      .createWithDefault(0)

  val COLUMNAR_MAX_SHUFFLE_BATCH_BYTE_SIZE =
    buildConf(GLUTEN_MAX_SHUFFLE_BATCH_BYTE_SIZE_KEY)
      .internal()
      .intConf
      .createWithDefault(41943040)

  val BOLT_USE_ICU_REGEX =
    buildConf("spark.gluten.sql.columnar.backend.bolt.useICURegex")
      .internal()
      .doc("When true, use ICU as the regex engine in Bolt backend, otherwise use RE2.")
      .booleanConf
      .createWithDefault(true)

  val GLUTEN_SHUFFLE_INSIDE_BOLT =
    buildConf("spark.gluten.shuffle.inside.bolt")
      .internal()
      .doc("run shuffle inside bolt")
      .booleanConf
      .createWithDefault(false)

  val ORC_USE_COLUMN_NAMES =
    buildConf("spark.gluten.sql.columnar.backend.bolt.orcUseColumnNames")
      .doc("Maps table field names to file field names using names, not indices for ORC files.")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_USE_COLUMN_NAMES =
    buildConf("spark.gluten.sql.columnar.backend.bolt.parquetUseColumnNames")
      .doc("Maps table field names to file field names using names, not indices for Parquet files.")
      .booleanConf
      .createWithDefault(true)
}

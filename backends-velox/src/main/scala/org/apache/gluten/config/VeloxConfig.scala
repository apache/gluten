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

import java.util.Locale
import java.util.concurrent.TimeUnit

/*
 * Note: Gluten configiguration.md is automatically generated from this code.
 * Make sure to run dev/gen-all-config-docs.sh after making changes to this file.
 */
class VeloxConfig(conf: SQLConf) extends GlutenConfig(conf) {
  import VeloxConfig._

  def veloxSpillFileSystem: String = getConf(COLUMNAR_VELOX_SPILL_FILE_SYSTEM)

  def veloxResizeBatchesShuffleInput: Boolean =
    getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT)

  def veloxResizeBatchesShuffleOutput: Boolean =
    getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT)

  case class ResizeRange(min: Int, max: Int) {
    assert(max >= min)
    assert(min > 0, "Min batch size should be larger than 0")
    assert(max > 0, "Max batch size should be larger than 0")
  }

  def veloxResizeBatchesShuffleInputOutputRange: ResizeRange = {
    val standardSize = getConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE)
    val defaultMinSize: Int = (0.25 * standardSize).toInt.max(1)
    val minSize = getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_OUTPUT_MIN_SIZE)
      .getOrElse(defaultMinSize)
    ResizeRange(minSize, Int.MaxValue)
  }

  def castFromVarcharAddTrimNode: Boolean = getConf(CAST_FROM_VARCHAR_ADD_TRIM_NODE)

  def enableVeloxFlushablePartialAggregation: Boolean =
    getConf(VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED)

  def enableBroadcastBuildRelationInOffheap: Boolean =
    getConf(VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP)

  def enableBroadcastBuildOncePerExecutor: Boolean =
    getConf(VELOX_BROADCAST_BUILD_HASHTABLE_ONCE_PER_EXECUTOR)

  def veloxBroadcastHashTableBuildThreads: Int =
    getConf(COLUMNAR_VELOX_BROADCAST_HASH_TABLE_BUILD_THREADS)

  def veloxOrcScanEnabled: Boolean =
    getConf(VELOX_ORC_SCAN_ENABLED)

  def floatingPointMode: String = getConf(FLOATING_POINT_MODE)

  def enableRewriteCastArrayToString: Boolean =
    getConf(ENABLE_REWRITE_CAST_ARRAY_TO_STRING)

  def enableRewriteUnboundedWindow: Boolean = getConf(ENABLE_REWRITE_UNBOUNDED_WINDOW)

  def enableEnhancedFeatures(): Boolean = ConfigJniWrapper.isEnhancedFeaturesEnabled &&
    getConf(ENABLE_ENHANCED_FEATURES)

  def veloxPreferredBatchBytes: Long = getConf(COLUMNAR_VELOX_PREFERRED_BATCH_BYTES)

  def cudfEnableTableScan: Boolean = getConf(CUDF_ENABLE_TABLE_SCAN)

  def cudfEnableValidation: Boolean = getConf(CUDF_ENABLE_VALIDATION)

  def cudfBatchSize: Int = getConf(CUDF_BATCH_SIZE)

  def cudfShuffleMaxPrefetchBytes: Long = getConf(CUDF_SHUFFLE_MAX_PREFETCH_BYTES)

  def orcUseColumnNames: Boolean = getConf(ORC_USE_COLUMN_NAMES)

  def parquetUseColumnNames: Boolean = getConf(PARQUET_USE_COLUMN_NAMES)

  def hashProbeBloomFilterPushdownMaxSize: Long = getConf(HASH_PROBE_BLOOM_FILTER_PUSHDOWN_MAX_SIZE)

  def hashProbeDynamicFilterPushdownEnabled: Boolean =
    getConf(HASH_PROBE_DYNAMIC_FILTER_PUSHDOWN_ENABLED)

  def valueStreamDynamicFilterEnabled: Boolean =
    getConf(VALUE_STREAM_DYNAMIC_FILTER_ENABLED)
}

object VeloxConfig extends ConfigRegistry {
  override def get: VeloxConfig = {
    new VeloxConfig(SQLConf.get)
  }

  // velox caching options.
  val COLUMNAR_VELOX_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cacheEnabled")
      .doc(
        "Enable Velox cache, default off. It's recommended to enable" +
          "soft-affinity as well when enable velox cache.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_MEM_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.memCacheSize")
      .doc("The memory cache size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_MEM_INIT_CAPACITY =
    buildConf("spark.gluten.sql.columnar.backend.velox.memInitCapacity")
      .doc("The initial memory capacity to reserve for a newly created Velox query memory pool.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  val COLUMNAR_VELOX_MEM_RECLAIM_MAX_WAIT_MS =
    buildConf("spark.gluten.sql.columnar.backend.velox.reclaimMaxWaitMs")
      .doc("The max time in ms to wait for memory reclaim.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(60))

  val COLUMNAR_VELOX_MEMORY_POOL_CAPACITY_TRANSFER_ACROSS_TASKS =
    buildConf("spark.gluten.sql.columnar.backend.velox.memoryPoolCapacityTransferAcrossTasks")
      .doc("Whether to allow memory capacity transfer between memory pools from different tasks.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_SSD_CACHE_PATH =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCachePath")
      .doc("The folder to store the cache files, better on SSD")
      .stringConf
      .createWithDefault("/tmp")

  val COLUMNAR_VELOX_SSD_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheSize")
      .doc("The SSD cache size, will do memory caching only if this value = 0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_SHARDS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheShards")
      .doc("The cache shards")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_CACHE_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads")
      .doc("The IO threads for cache promoting")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_ODIRECT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdODirect")
      .doc("The O_DIRECT flag for cache writing")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_DISABLE_FILE_COW =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdDisableFileCow")
      .doc("True if copy on write should be disabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_CHECKSUM_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdChecksumEnabled")
      .doc("If true, checksum write to SSD is enabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_CHECKSUM_READ_VERIFICATION_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdChecksumReadVerificationEnabled")
      .doc("If true, checksum read verification from SSD is enabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_INTERVAL_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCheckpointIntervalBytes")
      .doc(
        "Checkpoint after every 'checkpointIntervalBytes' for SSD cache. " +
          "0 means no checkpointing.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_CONNECTOR_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.IOThreads")
      .doc(
        "The Size of the IO thread pool in the Connector. " +
          "This thread pool is used for split preloading and DirectBufferedInput. " +
          "By default, the value is the same as the maximum task slots per Spark executor.")
      .intConf
      .createOptional

  val COLUMNAR_VELOX_BROADCAST_HASH_TABLE_BUILD_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.broadcastHashTableBuildThreads")
      .doc("The number of threads used to build the broadcast hash table. " +
        "If not set or set to 0, it will use the default number of threads (available processors).")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_ASYNC_TIMEOUT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.asyncTimeoutOnTaskStopping")
      .doc(
        "Timeout for asynchronous execution when task is being stopped in Velox backend. " +
          "It's recommended to set to a number larger than network connection timeout that the " +
          "possible aysnc tasks are relying on.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(30000)

  val COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER =
    buildConf("spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver")
      .doc("The split preload per task")
      .intConf
      .createWithDefault(2)

  val COLUMNAR_VELOX_GLOG_VERBOSE_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogVerboseLevel")
      .internal()
      .doc("Set glog verbose level in Velox backend, same as FLAGS_v.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_GLOG_SEVERITY_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogSeverityLevel")
      .internal()
      .doc("Set glog severity level in Velox backend, same as FLAGS_minloglevel.")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SPILL_STRATEGY =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillStrategy")
      .doc("none: Disable spill on Velox backend; " +
        "auto: Let Spark memory manager manage Velox's spilling")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("none", "auto"))
      .createWithDefault("auto")

  val COLUMNAR_VELOX_MAX_SPILL_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillLevel")
      .doc("The max allowed spilling level with zero being the initial spilling level")
      .intConf
      .createWithDefault(4)

  val COLUMNAR_VELOX_MAX_SPILL_FILE_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillFileSize")
      .doc("The maximum size of a single spill file created")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SPILL_FILE_SYSTEM =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillFileSystem")
      .doc(
        "The filesystem used to store spill data. local: The local file system. " +
          "heap-over-local: Write file to JVM heap if having extra heap space. " +
          "Otherwise write to local file system.")
      .stringConf
      .checkValues(Set("local", "heap-over-local"))
      .createWithDefaultString("local")

  val COLUMNAR_VELOX_MAX_SPILL_RUN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillRunRows")
      .doc("The maximum row size of a single spill run")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("3M")

  val COLUMNAR_VELOX_MAX_SPILL_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillBytes")
      .doc("The maximum file size of a query")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("100G")

  val MAX_PARTITION_PER_WRITERS_SESSION =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartitionsPerWritersSession")
      .doc("Maximum number of partitions per a single table writer instance.")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(10000)

  val MAX_TARGET_FILE_SIZE_SESSION =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxTargetFileSize")
      .doc(
        "The target file size for each output file when writing data. " +
          "0 means no limit on target file size, and the actual file size will be determined by " +
          "other factors such as max partition number and shuffle batch size.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ >= 0, "must be a non-negative number")
      .createWithDefault(0)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput")
      .doc(
        s"If true, combine small columnar batches together before sending to shuffle. " +
          s"The default minimum output batch size is equal to 0.25 * " +
          s"${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key}")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleOutput")
      .doc(
        s"If true, combine small columnar batches together right after shuffle read. " +
          s"The default minimum output batch size is equal to 0.25 * " +
          s"${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key}")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput.minSize")
      .doc(
        s"The minimum batch size for shuffle. If size of an input batch is " +
          s"smaller than the value, it will be combined with other " +
          s"batches before sending to shuffle. Only functions when " +
          s"${COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key} is set to true. " +
          s"Default value: 0.25 * <max batch size>")
      .intConf
      .createOptional

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_OUTPUT_MIN_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInputOuptut.minSize")
      .doc(
        s"The minimum batch size for shuffle input and output. " +
          s"If size of an input batch is " +
          s"smaller than the value, it will be combined with other " +
          s"batches before sending to shuffle. " +
          s"The same applies for batches output by shuffle read. " +
          s"Only functions when " +
          s"${COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key} or " +
          s"${COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT.key} is set to true. " +
          s"Default value: 0.25 * <max batch size>")
      .fallbackConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE)

  val COLUMNAR_VELOX_ENABLE_USER_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for user type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_SHOW_TASK_METRICS_WHEN_FINISHED =
    buildConf("spark.gluten.sql.columnar.backend.velox.showTaskMetricsWhenFinished")
      .doc("Show velox full task metrics when finished.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_TASK_METRICS_TO_EVENT_LOG_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.backend.velox.taskMetricsToEventLog.threshold")
      .internal()
      .doc("Sets the threshold in seconds for writing task statistics to the event log if the " +
        "task runs longer than this value. Configuring the value >=0 can enable the feature. " +
        "0 means all tasks report and save the metrics to eventlog. value <0 disable the feature.")
      .timeConf(TimeUnit.SECONDS)
      .createOptional

  val COLUMNAR_VELOX_MEMORY_USE_HUGE_PAGES =
    buildConf("spark.gluten.sql.columnar.backend.velox.memoryUseHugePages")
      .doc("Use explicit huge pages for Velox memory allocation.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_ENABLE_SYSTEM_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableSystemExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for system type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation")
      .doc(
        "Enable flushable aggregation. If true, Gluten will try converting regular aggregation " +
          "into Velox's flushable aggregation when applicable. A flushable aggregation could " +
          "emit intermediate result at anytime when memory is full / data reduction ratio is low."
      )
      .booleanConf
      .createWithDefault(true)

  val MAX_PARTIAL_AGGREGATION_MEMORY =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemory")
      .doc(
        "Set the max memory of partial aggregation in bytes. When this option is set to a " +
          "value greater than 0, it will override spark.gluten.sql.columnar.backend.velox." +
          "maxPartialAggregationMemoryRatio. Note: this option only works when flushable " +
          "partial aggregation is enabled. Ignored when spark.gluten.sql.columnar.backend." +
          "velox.flushablePartialAggregation=false."
      )
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val MAX_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemoryRatio")
      .doc(
        "Set the max memory of partial aggregation as "
          + "maxPartialAggregationMemoryRatio of offheap size. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.1)

  val MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemory")
      .doc(
        "Set the max extended memory of partial aggregation in bytes. When this option is set " +
          "to a value greater than 0, it will override spark.gluten.sql.columnar.backend.velox." +
          "maxExtendedPartialAggregationMemoryRatio. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemoryRatio")
      .doc(
        "Set the max extended memory of partial aggregation as "
          + "maxExtendedPartialAggregationMemoryRatio of offheap size. Note: this option only " +
          "works when flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.15)

  val ABANDON_PARTIAL_AGGREGATION_MIN_PCT =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct")
      .doc(
        "If partial aggregation aggregationPct greater than this value, "
          + "partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(90)

  val ABANDON_PARTIAL_AGGREGATION_MIN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows")
      .doc(
        "If partial aggregation input rows number greater than this value, "
          + " partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(100000)

  val HASH_PROBE_BLOOM_FILTER_PUSHDOWN_MAX_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashProbe.bloomFilterPushdown.maxSize")
      .doc("The maximum byte size of Bloom filter that can be generated from hash probe. When " +
        "set to 0, no Bloom filter will be generated. To achieve optimal performance, this should" +
        " not be too larger than the CPU cache size on the host.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(0)

  val HASH_PROBE_DYNAMIC_FILTER_PUSHDOWN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashProbe.dynamicFilterPushdown.enabled")
      .doc(
        "Whether hash probe can generate any dynamic filter (including Bloom filter) and push" +
          " down to upstream operators.")
      .booleanConf
      .createWithDefault(true)

  val VALUE_STREAM_DYNAMIC_FILTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.valueStream.dynamicFilter.enabled")
      .doc(
        "Whether to apply dynamic filters pushed down from hash probe in the ValueStream" +
          " (shuffle reader) operator to filter rows before they reach the hash join.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled")
      .doc(
        "Disables caching if false. File handle cache should be disabled " +
          "if files are mutable, i.e. file content may change while file path stays the same.")
      .booleanConf
      .createWithDefault(false)

  val DIRECTORY_SIZE_GUESS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.directorySizeGuess")
      .doc("Deprecated, rename to spark.gluten.sql.columnar.backend.velox.footerEstimatedSize")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FOOTER_ESTIMATED_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.footerEstimatedSize")
      .doc("Set the footer estimated size for velox file scan, " +
        "refer to Velox's footer-estimated-size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FILE_PRELOAD_THRESHOLD =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.filePreloadThreshold")
      .doc("Set the file preload threshold for velox file scan, " +
        "refer to Velox's file-preload-threshold")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1MB")

  val PREFETCH_ROW_GROUPS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.prefetchRowGroups")
      .doc("Set the prefetch row groups for velox file scan")
      .intConf
      .createWithDefault(1)

  val LOAD_QUANTUM =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.loadQuantum")
      .doc("Set the load quantum for velox file scan, recommend to use the default value (256MB) " +
        "for performance consideration. If Velox cache is enabled, it can be 8MB at most.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256MB")

  val MAX_COALESCED_DISTANCE_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedDistance")
      .doc(" Set the max coalesced distance bytes for velox file scan")
      .stringConf
      .createWithDefaultString("512KB")

  val MAX_COALESCED_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes")
      .doc("Set the max coalesced bytes for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64MB")

  val CACHE_PREFETCH_MINPCT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cachePrefetchMinPct")
      .doc("Set prefetch cache min pct for velox file scan")
      .intConf
      .createWithDefault(0)

  val AWS_SDK_LOG_LEVEL =
    buildConf("spark.gluten.velox.awsSdkLogLevel")
      .internal()
      .doc("Log granularity of AWS C++ SDK in velox.")
      .stringConf
      .createWithDefault("FATAL")

  val AWS_S3_RETRY_MODE =
    buildConf("spark.gluten.velox.fs.s3a.retry.mode")
      .internal()
      .doc("Retry mode for AWS s3 connection error: legacy, standard and adaptive.")
      .stringConf
      .createWithDefault("legacy")

  val VELOX_ORC_SCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.orc.scan.enabled")
      .doc("Enable velox orc scan. If disabled, vanilla spark orc scan will be used.")
      .booleanConf
      .createWithDefault(true)

  val CAST_FROM_VARCHAR_ADD_TRIM_NODE =
    buildConf("spark.gluten.velox.castFromVarcharAddTrimNode")
      .doc(
        "If true, will add a trim node " +
          "which has the same sementic as vanilla Spark to CAST-from-varchar." +
          "Otherwise, do nothing.")
      .booleanConf
      .createWithDefault(false)

  val VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP =
    buildConf("spark.gluten.velox.offHeapBroadcastBuildRelation.enabled")
      .experimental()
      .doc("Experimental: If enabled, broadcast build relation will use offheap memory. " +
        "Otherwise, broadcast build relation will use onheap memory.")
      .booleanConf
      .createWithDefault(false)

  val VELOX_HASHMAP_ABANDON_BUILD_DUPHASH_MIN_ROWS =
    buildConf("spark.gluten.velox.abandonDedupHashMap.minRows")
      .experimental()
      .doc("Experimental: abandon hashmap build if duplicated rows more than this number.")
      .intConf
      .createWithDefault(100000)

  val VELOX_HASHMAP_ABANDON_BUILD_DUPHASH_MIN_PCT =
    buildConf("spark.gluten.velox.abandonDedupHashMap.minPct")
      .experimental()
      .doc(
        "Experimental: abandon hashmap build if duplicated rows are more than this percentile. " +
          "Value is integer based and range is [0, 100].")
      .intConf
      .createWithDefault(0)

  val VELOX_BROADCAST_BUILD_HASHTABLE_ONCE_PER_EXECUTOR =
    buildConf("spark.gluten.velox.buildHashTableOncePerExecutor.enabled")
      .internal()
      .doc(
        "When enabled, the hash table is " +
          "constructed once per executor. If not enabled, " +
          "the hash table is rebuilt for each task.")
      .booleanConf
      .createWithDefault(true)

  val QUERY_TRACE_ENABLED = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceEnabled")
    .doc("Enable query tracing flag.")
    .booleanConf
    .createWithDefault(false)

  val QUERY_TRACE_DIR = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceDir")
    .internal()
    .doc("Base dir of a query to store tracing data.")
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_NODE_IDS = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceNodeIds")
    .internal()
    .doc("A comma-separated list of plan node ids whose input data will be traced. " +
      "Empty string if only want to trace the query metadata.")
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_MAX_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceMaxBytes")
      .internal()
      .doc("The max trace bytes limit. Tracing is disabled if zero.")
      .longConf
      .createWithDefault(0)

  val QUERY_TRACE_TASK_REG_EXP =
    buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceTaskRegExp")
      .internal()
      .doc("The regexp of traced task id. We only enable trace on a task if its id matches.")
      .stringConf
      .createWithDefault("")

  val OP_TRACE_DIRECTORY_CREATE_CONFIG =
    buildConf("spark.gluten.sql.columnar.backend.velox.opTraceDirectoryCreateConfig")
      .internal()
      .doc(
        "Config used to create operator trace directory. This config is provided to" +
          " underlying file system and the config is free form. The form should be " +
          "defined by the underlying file system.")
      .stringConf
      .createWithDefault("")

  val FLOATING_POINT_MODE =
    buildConf("spark.gluten.sql.columnar.backend.velox.floatingPointMode")
      .doc(
        "Config used to control the tolerance of floating point operations alignment with Spark. " +
          "When the mode is set to strict, flushing is disabled for sum(float/double)" +
          "and avg(float/double). When set to loose, flushing will be enabled.")
      .stringConf
      .checkValues(Set("loose", "strict"))
      .createWithDefault("loose")

  val COLUMNAR_VELOX_MEMORY_CHECK_USAGE_LEAK =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.checkUsageLeak")
      .doc("Enable check memory usage leak.")
      .booleanConf
      .createWithDefault(true)

  val CUDF_MEMORY_RESOURCE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.memoryResource")
      .doc("GPU RMM memory resource.")
      .stringConf
      .checkValues(Set("cuda", "pool", "async", "arena", "managed", "managed_pool"))
      .createWithDefault("async")

  val CUDF_MEMORY_PERCENT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.memoryPercent")
      .doc("The initial percent of GPU memory to allocate for memory resource for one thread.")
      .intConf
      .createWithDefault(50)

  val CUDF_ENABLE_TABLE_SCAN =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.enableTableScan")
      .doc("Enable cudf table scan")
      .booleanConf
      .createWithDefault(false)

  val CUDF_ENABLE_VALIDATION =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.enableValidation")
      .doc(
        "Heuristics you can apply to validate a cuDF/GPU plan and only offload when " +
          "the entire stage can be fully and profitably executed on GPU")
      .booleanConf
      .createWithDefault(true)

  val CUDF_BATCH_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.cudf.batchSize")
      .doc("Cudf input batch size after shuffle reader")
      .intConf
      .createWithDefault(Integer.MAX_VALUE)

  val CUDF_SHUFFLE_MAX_PREFETCH_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.cudf.shuffleMaxPrefetchBytes")
      .doc("Maximum bytes to prefetch in CPU memory during GPU shuffle read while waiting" +
        "for GPU available.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1028MB")

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
          " `concat('[', array_join(array, ', ', null), ']')` to allow offloading to Velox.")
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

  val COLUMNAR_VELOX_PREFERRED_BATCH_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.preferredBatchBytes")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("10MB")

  val VELOX_MAX_COMPILED_REGEXES =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxCompiledRegexes")
      .doc(
        "Controls maximum number of compiled regular expression patterns per function " +
          "instance per thread of execution.")
      .intConf
      .createWithDefault(100)

  val ORC_USE_COLUMN_NAMES =
    buildConf("spark.gluten.sql.columnar.backend.velox.orcUseColumnNames")
      .doc("Maps table field names to file field names using names, not indices for ORC files.")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_USE_COLUMN_NAMES =
    buildConf("spark.gluten.sql.columnar.backend.velox.parquetUseColumnNames")
      .doc("Maps table field names to file field names using names, not indices for Parquet files.")
      .booleanConf
      .createWithDefault(true)
}

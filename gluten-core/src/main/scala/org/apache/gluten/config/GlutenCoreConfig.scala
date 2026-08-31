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

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.{SQLConf, SQLConfProvider}

class GlutenCoreConfig(conf: SQLConf) extends Logging {
  import GlutenCoreConfig._

  private lazy val configProvider = new SQLConfProvider(conf)

  def getConf[T](entry: ConfigEntry[T]): T = {
    require(ConfigRegistry.containsEntry(entry), s"$entry is not registered")
    entry.readFrom(configProvider)
  }

  def enableGluten: Boolean = getConf(GLUTEN_ENABLED)

  def costModel: String = getConf(GLUTEN_COST_MODEL)

  def memoryUntracked: Boolean = getConf(COLUMNAR_MEMORY_UNTRACKED)

  def offHeapMemorySize: Long = getConf(COLUMNAR_OFFHEAP_SIZE_IN_BYTES)

  def taskOffHeapMemorySize: Long = getConf(COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES).getOrElse(0L)

  def conservativeTaskOffHeapMemorySize: Long =
    getConf(COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES)

  def memoryIsolation: Boolean = getConf(COLUMNAR_MEMORY_ISOLATION)

  def memoryOverAcquiredRatio: Double = getConf(COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO)

  def memoryReservationBlockSize: Long = getConf(COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE)

  def dynamicOffHeapSizingEnabled: Boolean =
    getConf(DYNAMIC_OFFHEAP_SIZING_ENABLED)

  def dynamicOffHeapSizingMemoryFraction: Double =
    getConf(DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION)
}

/*
 * Note: Gluten configuration.md is automatically generated from this code.
 * Make sure to run dev/gen-all-config-docs.sh after making changes to this file.
 */
object GlutenCoreConfig extends ConfigRegistry {
  def get: GlutenCoreConfig = {
    new GlutenCoreConfig(activeSQLConf)
  }

  private[gluten] def activeSQLConf: SQLConf = {
    SparkSession.getActiveSession
      .filterNot(_.sparkContext.isStopped)
      .map(_.sessionState.conf)
      .getOrElse(SQLConf.get)
  }

  val SPARK_OFFHEAP_SIZE_KEY = "spark.memory.offHeap.size"
  val SPARK_OFFHEAP_ENABLED_KEY = "spark.memory.offHeap.enabled"

  // A Spark key without a Gluten ConfigEntry, read while the native backend is initialized.
  registerStaticConf(SPARK_OFFHEAP_ENABLED_KEY)
    .doc("Whether Spark off-heap memory is enabled, read by native backend initialization.")
    .booleanConf
    .passToNative()
    .createOptional

  val GLUTEN_ENABLED =
    buildConf("spark.gluten.enabled")
      .doc(
        "Whether to enable gluten. Default value is true. Just an experimental property." +
          " Recommend to enable/disable Gluten through the setting for spark.plugins.")
      .booleanConf
      .createWithDefault(true)

  val GLUTEN_COST_MODEL =
    buildConf("spark.gluten.costModel")
      .doc(
        "The class name of user-defined cost model that will be used by Gluten's transition " +
          "planner. If not specified, a legacy built-in cost model will be " +
          "used. The legacy cost model exhaustively offloads computations, and " +
          "helps transition planner choose columnar-to-columnar transition over others.")
      .stringConf
      .createWithDefaultString("legacy")

  val COLUMNAR_MEMORY_UNTRACKED =
    buildStaticConf("spark.gluten.memory.untracked")
      .internal()
      .doc(
        "When enabled, turn all native memory allocations in Gluten into untracked. Spark " +
          "will be unaware of the allocations so will not trigger spill-to-disk operations " +
          "or Spark OOMs. Should only be used for testing or other non-production use cases.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_MEMORY_ISOLATION =
    buildConf("spark.gluten.memory.isolation")
      .doc("Enable isolated memory mode. If true, Gluten controls the maximum off-heap memory " +
        "can be used by each task to X, X = executor memory / max task slots. It's recommended " +
        "to set true if Gluten serves concurrent queries within a single session, since not all " +
        "memory Gluten allocated is guaranteed to be spillable. In the case, the feature should " +
        "be enabled to avoid OOM.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_OVERHEAD_SIZE_IN_BYTES =
    buildStaticConf("spark.gluten.memoryOverhead.size.in.bytes")
      .internal()
      .passToNative()
      .doc(
        "Memory overhead available to the Velox global memory manager, set by VeloxListenerApi " +
          "from the actual resource configuration. No default value: the value cannot be derived " +
          "without a SparkConf at hand, and native treats the key's absence as 'unbounded' - " +
          "`VeloxBackend::init` falls back to `kMaxMemory`, so delivering a placeholder 0 would " +
          "instead build the global memory manager with zero capacity. Absent for a " +
          "non-execution operation (e.g. org.apache.spark.sql.Dataset#summary), which does not " +
          "propagate configurations via SQLExecution#withSQLConfPropagated.")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  // No `passToNative`: native declares `kSparkOffHeapMemory` but reads it nowhere. The ClickHouse
  // backend does consume it, but JVM-side from the conf map through this entry, so it does not need
  // the native channel.
  val COLUMNAR_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.task.offHeap.size.in.bytes")
      .internal()
      .passToNative()
      .doc(
        "Per-task off-heap memory, set by GlutenPlugin from the actual resource configuration. " +
          "No default value: native treats the key's absence as 'unbounded' - " +
          "`WholeStageResultIterator` falls back to `kMaxMemory` when sizing partial " +
          "aggregation, and ClickHouse leaves its own defaults in place - so delivering a " +
          "placeholder 0 would collapse those limits instead. Absent for a non-execution " +
          "operation (e.g. org.apache.spark.sql.Dataset#summary), which does not propagate " +
          "configurations via SQLExecution#withSQLConfPropagated.")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.conservative.task.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO =
    buildConf("spark.gluten.memory.overAcquiredMemoryRatio")
      .doc("If larger than 0, Velox backend will try over-acquire this ratio of the total " +
        "allocated memory as backup to avoid OOM.")
      .doubleConf
      .checkValue(d => d >= 0.0d, "Over-acquired ratio should be larger than or equals 0")
      .createWithDefault(0.3d)

  val COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE =
    buildConf("spark.gluten.memory.reservationBlockSize")
      .doc("Block size of native reservation listener reserve memory from Spark.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  val NUM_TASK_SLOTS_PER_EXECUTOR =
    buildConf("spark.gluten.numTaskSlotsPerExecutor")
      .passToNative()
      .doc(
        "Number of task slots per executor, set by GlutenPlugin from the actual resource " +
          "configuration. No default value: the value cannot be derived without a SparkConf at " +
          "hand, and native rejects a placeholder - `GLUTEN_CHECK(numTaskSlotsPerExecutor >= 0)` " +
          "in VeloxBackend. Native warns and falls back to 1 when the key is absent, which is " +
          "what non-execution operations (e.g. org.apache.spark.sql.Dataset#summary) hit, since " +
          "they do not propagate configurations via SQLExecution#withSQLConfPropagated.")
      .intConf
      .createOptional

  // Since https://github.com/apache/gluten/issues/5439.
  val DYNAMIC_OFFHEAP_SIZING_ENABLED =
    buildStaticConf("spark.gluten.memory.dynamic.offHeap.sizing.enabled")
      .experimental()
      .doc(
        "Experimental: When set to true, the offheap config (spark.memory.offHeap.size) will " +
          "be ignored and instead we will consider onheap and offheap memory in combination, " +
          "both counting towards the executor memory config (spark.executor.memory). We will " +
          "make use of JVM APIs to determine how much onheap memory is use, alongside tracking " +
          "offheap allocations made by Gluten. We will then proceed to enforcing a total memory " +
          "quota, calculated by the sum of what memory is committed and in use in the Java " +
          "heap. Since the calculation of the total quota happens as offheap allocation happens " +
          "and not as JVM heap memory is allocated, it is possible that we can oversubscribe " +
          "memory. Additionally, note that this change is experimental and may have performance " +
          "implications.")
      .booleanConf
      .createWithDefault(false)

  // Since https://github.com/apache/gluten/issues/5439.
  val DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION =
    buildStaticConf("spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction")
      .experimental()
      .doc(
        "Experimental: Determines the memory fraction used to determine the total " +
          "memory available for offheap and onheap allocations when the dynamic offheap " +
          "sizing feature is enabled. The default is set to match spark.executor.memoryFraction.")
      .doubleConf
      .checkValue(v => v >= 0 && v <= 1, "offheap sizing memory fraction must between [0, 1]")
      .createWithDefault(0.6)
}

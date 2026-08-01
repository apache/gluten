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
package org.apache.gluten.memory

import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.config.VeloxConfig._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SparkConfigUtil._
import org.apache.spark.util.SparkResourceUtil

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import scala.util.control.NonFatal

/**
 * Keeps the Velox memCache within the memory Spark can currently spare.
 *
 * All dealings with Spark's memory manager stay here, because ordering alone makes them safe --
 * neither direction ever needs undoing:
 *   - growing: borrow first (this may be refused), then raise the cap, which always takes effect;
 *   - shrinking: lower the cap first, which always takes effect but frees only what is not pinned,
 *     then repay whatever it did free.
 *
 * @param maxStorageBytes
 *   How large Spark's storage pool could be right now: the total less what execution holds. This
 *   counts what storage already uses, this reservation included, not only what is free. 0 when
 *   SparkEnv is gone.
 * @param setCapacity
 *   moves the capacity from the first value to the second, returns the capacity in effect
 *   afterwards.
 */
class PeriodicMemoryChecker(
    checkIntervalMs: Long,
    cacheStorageRatio: Double,
    stepBytes: Long,
    minCacheBytes: Long,
    maxCacheBytes: Long,
    maxStorageBytes: () => Long,
    setCapacity: (Long, Long) => Long,
    reservation: CacheStorageReservation)
  extends Logging {

  /**
   * The only state two threads share, so volatile is the only synchronisation needed. `reserved`
   * looks shared but is not: written once before the scheduler exists, then only by its thread.
   */
  @volatile private var stopped: Boolean = false

  /**
   * What Spark has lent us, and the cap the cache is admitted against -- the same number by
   * construction, since the cap is raised only after Spark agrees and lowered only with the
   * repayment.
   *
   * It can briefly sit below what the cache holds: the cap is read once on the way into an
   * allocation, so a call already under way still lands. Nothing corrects that. The lower cap is in
   * force by then, so nothing further is admitted and eviction closes the gap.
   */
  private var reserved: Long = 0L

  @volatile private var scheduler: ScheduledExecutorService = _

  /** Long enough for a round that is mid-JNI to finish, short enough not to delay a shutdown. */
  private val StopTimeoutMs = 5000L

  /**
   * The cap to aim for next, given how much storage Spark can currently spare.
   *
   * Halving on the way down and a fixed step on the way up, because a query that has to spill costs
   * far more than the cache misses that shrinking causes. Sustained demand therefore walks the
   * cache to its floor, which is the point: execution has first claim on memory.
   */
  private[memory] def nextCapacity(current: Long, maxStorage: Long): Long = {
    val underPressure = current >= (cacheStorageRatio * maxStorage).toLong
    if (underPressure) {
      val room = current - minCacheBytes
      // Once halving would take less than a step, go the rest of the way: creeping costs a round
      // of eviction and a turn of Spark's memory lock each time, and stopping short leaves the
      // cache holding memory it is under pressure to give up.
      if (room <= 2 * stepBytes) minCacheBytes else current - room / 2
    } else {
      math.min(current + stepBytes, maxCacheBytes)
    }
  }

  private[memory] def onTick(): Unit = {
    try {
      if (!stopped) {
        adjust()
      }
    } catch {
      case NonFatal(t) => logWarning("Cache capacity adjustment failed; continuing", t)
    }
  }

  private def adjust(): Unit = {
    val maxStorage = maxStorageBytes()
    if (maxStorage <= 0) {
      // Driver, or the executor is tearing down.
      return
    }

    val target = nextCapacity(reserved, maxStorage)
    if (target == reserved) {
      return
    }
    if (target > reserved) grow(target) else shrink(target)
  }

  /**
   * Borrows first; the cap only follows once Spark has agreed. Spark's pool grants the whole
   * request or nothing, so a refusal leaves the cache where it is for the next round to retry.
   */
  private def grow(target: Long): Unit = {
    if (reservation.borrow(target - reserved) > 0) {
      // Before the cap moves, so that a throw leaves us holding memory we did not use -- wasteful,
      // and retried -- rather than forgetting a borrow, which loses it for good.
      reserved = target
      moveCapTo(target)
    }
  }

  /** Lowers the cap, which is what makes the cache evict, then repays what was given up. */
  private def shrink(target: Long): Unit = moveCapTo(target)

  /**
   * Moves the cap to `target` and hands back whatever that freed.
   *
   * What comes back is never below what the cache holds, so repaying down to it cannot hand Spark
   * memory still in use. Pinned entries make it land above the target, and only what was really
   * freed is repaid; the rest follows once the readers finish and the cap already set takes effect.
   *
   * Repaying before lowering `reserved` means a throw leaves it too high -- wasteful, and retried
   * -- rather than dropping the difference for good.
   */
  private def moveCapTo(target: Long): Unit = {
    val applied = setCapacity(reserved, target)
    // Being above the cap is ordinary while readers hold their entries, so speak up only once the
    // gap is worth a round of its own. Nothing is done about it either way.
    if (applied - target > stepBytes) {
      logWarning(
        s"Velox cache holds $applied bytes against a cap of $target. Entries are pinned, or an " +
          s"allocation already under way outran the change. Closes on its own as they are " +
          s"released; persistent gaps mean readers are holding the cache open.")
    }
    if (applied < reserved) {
      reservation.repay(reserved - applied)
      reserved = applied
    }
  }

  /**
   * Books what the cache already occupies, then begins adjusting.
   *
   * The allocator is at the configured cache size from the moment it is built, so that has to be
   * paid for before anything else happens. Bringing it down is the rounds' job, not startup's.
   */
  def start(): Unit = {
    reserveInitial(maxCacheBytes)

    // Spark's own ThreadUtils would do, but it is private[spark] and this class is not in a Spark
    // package; reaching it would cost a forwarder for no behaviour we need.
    val exec = Executors.newSingleThreadScheduledExecutor(
      (r: Runnable) => {
        val t = new Thread(r, "gluten-velox-cache-checker")
        t.setDaemon(true)
        t
      })
    exec.scheduleWithFixedDelay(
      () => onTick(),
      checkIntervalMs,
      checkIntervalMs,
      TimeUnit.MILLISECONDS)
    scheduler = exec
    logInfo(s"Velox cache checker started, floor $minCacheBytes ceiling $maxCacheBytes bytes.")
  }

  /**
   * Stops adjusting, and does nothing else. Must run before the native backend tears down.
   *
   * A repayment is always worked out from a reading that goes stale as it returns; an ordinary
   * round can live with that because the next one corrects it, and there is no next one here. It
   * would leave Spark believing memory is free while the cache still holds it -- the one direction
   * that lets something else take it.
   *
   * Capping the cache at zero would not earn that back: it evicts entries running tasks are about
   * to read again, and returns the memory to the operating system rather than to Spark, moments
   * before the process exits and releases everything anyway.
   */
  def stop(): Unit = {
    // Flagged before the scheduler is torn down, so that a round already in progress finds it set
    // rather than having to be interrupted mid-way.
    stopped = true
    val exec = scheduler
    if (exec != null) {
      exec.shutdownNow()
      // shutdownNow() only interrupts; a round already inside the JNI call would run on. The
      // native backend is torn down from a JVM shutdown hook that fires after this, and
      // AsyncDataCache::shutdown() clears its shards without locking, so a tick still adjusting
      // the capacity would be racing it.
      if (!exec.awaitTermination(StopTimeoutMs, TimeUnit.MILLISECONDS)) {
        logWarning(
          s"Velox cache checker did not stop within $StopTimeoutMs ms; a capacity adjustment may " +
            s"still be running as the native backend is torn down.")
      }
      scheduler = null
    }
  }

  /**
   * Books what the cache already occupies with Spark.
   *
   * The allocator holds the configured cache size from the moment it is built, so that is what has
   * to be paid for; the rounds that follow are what bring it down. Spark's pool grants the whole
   * request or nothing, and there is nothing smaller worth falling back to -- the cache is already
   * that big whether or not anyone booked it -- so a refusal is a configuration error.
   *
   * Nothing is said to the allocator here: it is built at the configured size and reports it, so
   * the two already agree. The size is a parameter rather than a read of `maxCacheBytes` only so
   * that tests can start from other points.
   */
  private[memory] def reserveInitial(initialBytes: Long): Unit = {
    if (reservation.borrow(initialBytes) == 0) {
      throw new IllegalArgumentException(
        s"Spark cannot spare the $initialBytes bytes the Velox cache already occupies. Give the " +
          s"executor more memory, lower ${COLUMNAR_VELOX_MEM_CACHE_SIZE.key}, or disable " +
          s"${COLUMNAR_VELOX_CACHE_PUSHBACK_ENABLED.key}.")
    }
    reserved = initialBytes
    logInfo(s"Velox cache starting at $reserved bytes.")
  }

  /** What Spark has lent us, for assertions. */
  private[memory] def reservedBytes: Long = reserved
}

/** Executor-level entry point, wired from VeloxListenerApi at executor start and shutdown. */
object PeriodicMemoryChecker extends Logging {

  // Executor start and shutdown, once each and never at the same time.
  private var checker: PeriodicMemoryChecker = _

  /**
   * How large the storage pool could be right now, on the account where Gluten tracks native
   * memory: the total less what execution holds. It shrinks as execution takes more, which is the
   * pressure the cache responds to, and it counts what storage already uses -- this reservation
   * included -- so the share taken below is a share of the whole pool rather than of its free part.
   *
   * Returns 0 only when SparkEnv is gone, during teardown. It cannot be 0 while a reservation is
   * held: execution is capped at the total less min(storage used, storage region), and this
   * reservation is not a real block so nothing can evict it out of the storage figure.
   */
  private def maxStorageMemory(): Long = {
    val env = SparkEnv.get
    if (env == null) {
      0L
    } else if (GlutenCoreConfig.get.dynamicOffHeapSizingEnabled) {
      // Same predicate GlobalOffHeapMemoryTarget and MemoryTargets.newConsumer use, so the cache
      // lands on the same books as queries.
      env.memoryManager.maxOnHeapStorageMemory
    } else {
      env.memoryManager.maxOffHeapStorageMemory
    }
  }

  def start(conf: SparkConf): Unit = {
    if (checker != null) {
      return
    }
    if (!conf.get(COLUMNAR_VELOX_CACHE_PUSHBACK_ENABLED)) {
      return
    }
    if (!conf.get(COLUMNAR_VELOX_CACHE_ENABLED)) {
      logWarning("Velox cache pushback is enabled but the cache is not; skipping.")
      return
    }

    // The native allocator is built from this same value, so there is nothing to ask it for.
    val staticCapacity = conf.get(COLUMNAR_VELOX_MEM_CACHE_SIZE)
    val minCache = math.min(conf.get(COLUMNAR_VELOX_CACHE_PUSHBACK_MIN_CACHE_SIZE), staticCapacity)
    checkConfiguration(conf, minCache, staticCapacity)

    val cacheStorageRatio = conf.get(COLUMNAR_VELOX_CACHE_PUSHBACK_CACHE_RATIO)

    val g = new PeriodicMemoryChecker(
      checkIntervalMs = conf.get(COLUMNAR_VELOX_CACHE_PUSHBACK_CHECK_INTERVAL_MS),
      cacheStorageRatio = cacheStorageRatio,
      stepBytes = conf.get(COLUMNAR_VELOX_CACHE_PUSHBACK_STEP_SIZE),
      minCacheBytes = minCache,
      maxCacheBytes = staticCapacity,
      maxStorageBytes = () => maxStorageMemory(),
      setCapacity = (from, to) => VeloxCacheJniWrapper.setCacheCapacity(from, to),
      reservation = CacheStorageReservation
    )
    g.start()
    checker = g
  }

  /** Velox's page size; capacities and allocations are both measured in whole pages. */
  private val PageSize = 4096L

  private def roundUpToPage(bytes: Long): Long = (bytes + PageSize - 1) / PageSize * PageSize

  /** A floor this large is almost certainly a misreading of the setting rather than a choice. */
  private val SuspiciousFloor = 1L << 30

  /**
   * Rejects settings the feature cannot work with, before anything starts. A floor too small for
   * the reads running against it would otherwise surface much later and under load, looking nothing
   * like the configuration mistake it is.
   */
  private[memory] def checkConfiguration(conf: SparkConf, minCache: Long, maxCache: Long): Unit = {
    // Sustained pressure walks the cache to its floor by design, and a floor below what the
    // concurrent reads need leaves it holding nothing but pinned entries: allocation stops being
    // satisfiable and Velox raises kNoCacheSpace, which nothing catches, so the query fails.
    //
    // Sizing that need:
    //   - each read takes at most one load quantum, capped at 8MB by VeloxListenerApi while the
    //     cache is on. Whole-file preload is smaller still, since it only runs for files under
    //     filePreloadThreshold (1MB here, 8MB in Velox);
    //   - the count is the task slots. Prefetch runs on the connector's IO pool, which defaults to
    //     the same number and is larger only if set so (VeloxBackend.cc:234);
    //   - both sides in whole pages, since a read rounds up and the capacity rounds down. Comparing
    //     bytes would pass a floor short by up to a page per reader.
    val taskSlots = SparkResourceUtil.getTaskSlots(conf)
    val quantum = conf.get(LOAD_QUANTUM)
    val needed = taskSlots * roundUpToPage(quantum)
    val effectiveFloor = minCache - minCache % PageSize
    if (effectiveFloor < needed) {
      throw new IllegalArgumentException(
        s"${COLUMNAR_VELOX_CACHE_PUSHBACK_MIN_CACHE_SIZE.key} is $minCache bytes, which the " +
          s"cache rounds down to $effectiveFloor, below the $needed bytes that $taskSlots " +
          s"concurrent reads of $quantum bytes need. The cache shrinks to this floor under " +
          s"pressure and would fail queries with 'no cache space'. Raise it, lower " +
          s"${LOAD_QUANTUM.key}, or " +
          s"disable ${COLUMNAR_VELOX_CACHE_PUSHBACK_ENABLED.key}.")
    }

    // Pressure can never push below the floor, so it is memory kept for the life of the executor.
    // A large one is usually a unit mistake, and nothing else about the run would look wrong.
    if (minCache > SuspiciousFloor) {
      logWarning(
        s"${COLUMNAR_VELOX_CACHE_PUSHBACK_MIN_CACHE_SIZE.key} is $minCache bytes. The cache " +
          s"never shrinks below this, so that much stays reserved from Spark for the life of " +
          s"the executor, however tight memory gets. Check the unit if that was not intended.")
    }

    // TODO: reject a step below one page. The native side rounds every target down to a page, so
    // a sub-page step never moves the cap: each round borrows the remainder, gets the same capacity
    // back, and repays it, forever. The same rounding leaves up to a page of any unaligned floor or
    // ceiling borrowed but unusable.

    // Nothing left to adjust, so the feature does nothing while appearing to be on. Defensible --
    // a fixed cache Spark knows about still beats one it does not -- so only worth saying aloud.
    if (minCache >= maxCache) {
      logWarning(
        s"The Velox cache is fixed at $maxCache bytes: the " +
          s"${COLUMNAR_VELOX_CACHE_PUSHBACK_MIN_CACHE_SIZE.key} floor leaves no room above " +
          s"${COLUMNAR_VELOX_MEM_CACHE_SIZE.key}, so no round can move it. Lower the floor to " +
          s"let the cache yield memory under pressure.")
    }
  }

  def stop(): Unit = {
    if (checker != null) {
      checker.stop()
      checker = null
    }
  }
}

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

import org.apache.gluten.config.VeloxConfig._

import org.apache.spark.SparkConf

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class PeriodicMemoryCheckerSuite extends AnyFunSuite {

  private val mb = 1024L * 1024L
  private val cacheSize = 1024 * mb

  /**
   * Stands in for the native allocator plus the eviction the JNI layer does.
   *
   * Mirrors the one invariant that matters: the capacity reported is always max(cap, used), so a
   * target below what is in use only takes effect as far as eviction can reach.
   */
  private class FakeNative(var used: Long = 0L) {
    val targets = new ArrayBuffer[Long]
    private var dynamic: Long = cacheSize

    /** Bytes that cannot be evicted, mimicking pinned entries. */
    var pinned: Long = 0L

    def capacity: Long = math.max(dynamic, used)

    /** The capacity as set, before the clamp against what is in use. */
    def governed: Long = dynamic

    def setCapacity(from: Long, to: Long): Long = {
      targets += to
      assert(to >= 0 && to <= cacheSize, s"capacity out of range: $to")
      if (used > to) {
        // The JNI layer evicts before narrowing; everything above `pinned` goes.
        used = math.max(to, pinned)
      }
      // A reservation that does not cover the capacity is reported back rather than refused, since
      // refusing would block the very move that makes the cache evict its way back down.
      //
      // Clamped to what is still allocated and to the configured size, and rounded down to a page,
      // exactly as the native side does. Storing a target eviction could not reach would leave a
      // bound the caller never learns of, since what comes back is held up at the allocated bytes.
      dynamic = math.min(math.max(to - to % 4096, used), cacheSize)
      capacity
    }
  }

  /**
   * Stands in for Spark's storage pool, which grants the whole request or nothing --
   * `acquireStorageMemory` returns a Boolean, so there is no partial grant to model.
   */
  private class FakeReservation(var available: Long) extends CacheStorageReservation {
    val calls = new ArrayBuffer[String]
    var borrowed: Long = 0L

    override def borrow(bytes: Long): Long = {
      calls += s"borrow($bytes)"
      if (bytes > available) {
        return 0L
      }
      available -= bytes
      borrowed += bytes
      bytes
    }

    override def repay(bytes: Long): Long = {
      calls += s"repay($bytes)"
      available += bytes
      borrowed -= bytes
      bytes
    }
  }

  /** Refuses every request once armed, however much has been repaid to it. */
  private class RefusingReservation extends FakeReservation(Long.MaxValue / 2) {
    var armed: Boolean = false

    override def borrow(bytes: Long): Long = {
      if (!armed) {
        return super.borrow(bytes)
      }
      calls += s"borrow($bytes)"
      0L
    }
  }

  /** A checker that has not booked anything yet, for testing the startup path itself. */
  private def unstartedChecker(
      native: FakeNative,
      reservation: FakeReservation,
      maxStorage: () => Long,
      minCache: Long = 64 * mb,
      step: Long = 8 * mb): PeriodicMemoryChecker =
    new PeriodicMemoryChecker(
      checkIntervalMs = 1000,
      cacheStorageRatio = 0.5,
      stepBytes = step,
      minCacheBytes = minCache,
      maxCacheBytes = cacheSize,
      maxStorageBytes = maxStorage,
      setCapacity = (from, to) => native.setCapacity(from, to),
      reservation = reservation
    )

  /**
   * Builds a checker that has already booked `initial` with Spark and capped the cache there, which
   * is the state `reserveInitial` leaves behind. Anything the fake reservation still has available
   * is what Spark could lend on top of that.
   */
  private def newChecker(
      native: FakeNative,
      reservation: FakeReservation,
      maxStorage: () => Long,
      minCache: Long = 64 * mb,
      step: Long = 8 * mb,
      initial: Long = cacheSize): PeriodicMemoryChecker = {
    val c = unstartedChecker(native, reservation, maxStorage, minCache, step)
    // Model an allocator that was built at `initial`, which is what the configured cache size
    // means to the checker.
    native.setCapacity(native.capacity, initial)
    reservation.available += initial
    c.reserveInitial(initial)
    reservation.calls.clear()
    native.targets.clear()
    c
  }

  // Backing off halves the room above the floor, so a cache under pressure is
  // out of the way within a few rounds.
  test("backs off fast when memory is tight") {
    val checker = newChecker(new FakeNative(), new FakeReservation(0), () => 0L)
    // maxStorage 400MB puts the pressure threshold at 200MB.
    assert(checker.nextCapacity(800 * mb, 400 * mb) === 64 * mb + (800 - 64) / 2 * mb)
    assert(checker.nextCapacity(200 * mb, 400 * mb) === 64 * mb + (200 - 64) / 2 * mb)
  }

  test("never backs off below the floor") {
    val checker = newChecker(new FakeNative(), new FakeReservation(0), () => 0L)
    assert(checker.nextCapacity(64 * mb, 1 * mb) === 64 * mb)
  }

  // Reclaiming is slower than yielding on purpose: a lull should not undo the back-off that kept
  // a query from spilling. A fixed step also means a large cache reclaims no faster than a small
  // one, which is the conservative direction.
  test("grows by a fixed step while memory is not tight") {
    val checker = newChecker(new FakeNative(), new FakeReservation(0), () => 0L)
    // 8GB spare puts the threshold at 4GB, well above the current 512MB.
    assert(checker.nextCapacity(512 * mb, 8192 * mb) === 512 * mb + 8 * mb)
    assert(checker.nextCapacity(64 * mb, 8192 * mb) === 64 * mb + 8 * mb)
  }

  test("never grows past the configured size") {
    val checker = newChecker(new FakeNative(), new FakeReservation(0), () => 0L)
    assert(checker.nextCapacity(cacheSize, 8192L * 1024 * mb) === cacheSize)
  }

  // Creeping the last stretch would cost a round of eviction each time and leave the cache
  // holding memory it is under pressure to give up.
  test("goes the rest of the way once halving would take less than a step") {
    val checker = newChecker(new FakeNative(), new FakeReservation(0), () => 0L, step = 8 * mb)
    // 96MB is 32MB above the floor, so halving is worth doing on its own.
    assert(checker.nextCapacity(96 * mb, 1 * mb) === 80 * mb)
    // 80MB leaves 16MB, exactly two steps: close enough to finish.
    assert(checker.nextCapacity(80 * mb, 1 * mb) === 64 * mb)
    assert(checker.nextCapacity(72 * mb, 1 * mb) === 64 * mb)
    // Already there, so there is nothing to do.
    assert(checker.nextCapacity(64 * mb, 1 * mb) === 64 * mb)
  }

  test("skips the tick when SparkEnv is gone") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    newChecker(native, reservation, () => 0L).onTick()

    assert(native.targets.isEmpty)
    assert(reservation.calls.isEmpty)
  }

  // Backing off never asks Spark for anything, so it cannot be refused.
  test("backing off lowers the capacity first and repays what was freed") {
    val native = new FakeNative()
    val reservation = new FakeReservation(0)
    // 400MB spare puts the threshold at 200MB, far below the starting 1GB.
    val checker = newChecker(native, reservation, () => 400 * mb)

    checker.onTick()

    val expected = 64 * mb + (cacheSize - 64 * mb) / 2
    assert(native.capacity === expected)
    assert(reservation.calls === Seq(s"repay(${cacheSize - expected})"))
  }

  test("growing borrows before raising the capacity") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    // Plenty of room: the threshold sits at 4GB, well above the cache.
    val checker = newChecker(native, reservation, () => 8192 * mb, initial = 512 * mb)

    checker.onTick()

    val target = 512 * mb + 8 * mb
    assert(reservation.calls === Seq(s"borrow(${8 * mb})"))
    assert(native.targets === Seq(target))
    assert(native.capacity === target)
    // The cap never exceeds what was borrowed. A step that is not a whole number of pages would
    // land below the target instead, and the difference would come straight back.
    assert(native.capacity <= checker.reservedBytes)
  }

  // A cache below its floor holds nothing but pinned entries and fails the reads running against
  // it with kNoCacheSpace, which no one catches. Starting in that state is worse than not starting,
  // so the floor is a startup requirement rather than something to converge on later.
  test("refuses to start when Spark cannot fund the cache") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize - 1) // one byte short
    val checker = unstartedChecker(native, reservation, () => 400 * mb)

    val e = intercept[IllegalArgumentException](checker.reserveInitial(cacheSize))
    assert(e.getMessage.contains(cacheSize.toString), s"name the size: ${e.getMessage}")
    assert(checker.reservedBytes === 0, "nothing may stay booked after refusing to start")
    assert(native.targets.isEmpty, "the capacity must not move")
  }

  // The allocator holds the configured size from the moment it is built, so that is what gets
  // booked; the rounds that follow are what bring it down.
  test("books the whole configured cache size at startup") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    val checker = unstartedChecker(native, reservation, () => 400 * mb)

    checker.reserveInitial(cacheSize)

    assert(checker.reservedBytes === cacheSize)
    assert(reservation.calls === Seq(s"borrow($cacheSize)"))
  }

  // I2 as a standing property rather than something that only holds at startup.
  test("never drops below the floor however long the pressure lasts") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    val checker = newChecker(native, reservation, () => 1 * mb, minCache = 64 * mb)

    for (_ <- 1 to 50) {
      checker.onTick()
      assert(native.capacity >= 64 * mb, s"capacity fell to ${native.capacity}")
      assert(checker.reservedBytes >= 64 * mb, s"reservation fell to ${checker.reservedBytes}")
    }
  }

  // An allocation already under way is admitted against the cap it read, so the cache can end up
  // above it. Borrowing to cover that would reward an overshoot with a reservation it can grow
  // from, and the gap closes on its own anyway once eviction catches up.
  test("does not borrow to cover a cache that outran its cap") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    val checker = newChecker(native, reservation, () => 400 * mb, initial = 512 * mb)

    // Usage above the reservation, pinned so eviction cannot undo it.
    native.used = 600 * mb
    native.pinned = 600 * mb
    reservation.calls.clear()

    checker.onTick()

    assert(!reservation.calls.exists(_.startsWith("borrow")), "nothing was borrowed")
    assert(checker.reservedBytes === 512 * mb, "and the reservation did not follow usage up")
  }

  // A shrink eviction could not reach must not leave a cap behind.
  //
  // The cap is stored no lower than what is still allocated, so it cannot fall below what has been
  // reserved. Storing the unreachable target instead would leave a bound this class never learns
  // of -- what comes back is held up at the allocated bytes -- and once pressure lifted at the
  // ceiling, where every target equals the reservation, no round would ever move it again: Spark
  // would keep the whole reservation while the cache went on being squeezed to a target that no
  // longer applied.
  test("a blocked shrink leaves no cap behind once pressure lifts") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    // Starting at the ceiling, which is where a stale cap could never be corrected: every later
    // target equals the reservation, so no round has a move to make.
    var maxStorage = 400 * mb
    val checker = newChecker(native, reservation, () => maxStorage, initial = cacheSize)

    native.used = cacheSize
    native.pinned = cacheSize
    checker.onTick()
    assert(checker.reservedBytes === cacheSize, "nothing could be evicted, so nothing was repaid")

    // Pressure lifts before the entries are released, so no round retries the shrink.
    maxStorage = 8192 * mb
    native.pinned = 0
    for (_ <- 1 to 5) {
      checker.onTick()
    }

    assert(
      native.governed === checker.reservedBytes,
      s"cap ${native.governed} should still match reservation ${checker.reservedBytes}")
  }

  test("keeps shrinking once the pinned entries are released") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    val checker = newChecker(native, reservation, () => 400 * mb, initial = 512 * mb)

    native.used = 600 * mb
    native.pinned = 600 * mb
    checker.onTick()
    // Nothing could be evicted, so nothing was handed back and the cap stayed where usage holds
    // it. Retrying is what eventually moves it, which is the next round's job.
    assert(checker.reservedBytes === 512 * mb, "nothing was repaid yet")

    // The readers finish, so the eviction the rounds have been asking for can finally happen.
    native.pinned = 0
    for (_ <- 1 to 10) {
      checker.onTick()
    }

    assert(checker.reservedBytes < 600 * mb, "the reservation came down once entries could go")
    assert(native.used <= checker.reservedBytes, "and the cache is inside it again")
  }

  // A refusal must leave the cache where it is rather than fail anything.
  test("stays put when Spark has nothing to lend") {
    val native = new FakeNative()
    val reservation = new RefusingReservation
    val checker = newChecker(native, reservation, () => 8192 * mb, initial = 512 * mb)
    reservation.armed = true

    checker.onTick()

    assert(native.targets.isEmpty, "the cap was never moved")
    assert(native.capacity === 512 * mb)
  }

  // Pinned entries make eviction fall short, so only what was really freed is
  // repaid; repaying the requested amount would hand back memory still in use.
  test("repays only what was freed when entries stay pinned") {
    val native = new FakeNative(used = cacheSize)
    native.pinned = 700 * mb
    val reservation = new FakeReservation(0)
    val checker = newChecker(native, reservation, () => 400 * mb)

    checker.onTick()

    // Could only evict down to the pinned bytes, so that is where it settles.
    assert(native.capacity === 700 * mb)
    assert(reservation.calls === Seq(s"repay(${cacheSize - 700 * mb})"))
  }

  test("yields the rest on a later tick once entries are unpinned") {
    val native = new FakeNative(used = cacheSize)
    native.pinned = 700 * mb
    val reservation = new FakeReservation(0)
    val checker = newChecker(native, reservation, () => 400 * mb)
    checker.onTick()
    reservation.calls.clear()

    native.pinned = 0 // readers are done
    checker.onTick()

    assert(native.capacity < 700 * mb)
    assert(reservation.calls.head.startsWith("repay("))
  }

  // A cache can give memory up without this class asking: an eviction driven by another query's
  // allocation never reaches the JVM. Reading the capacity back each round is what stops that
  // difference staying borrowed from Spark for good.
  test("hands back what the cache gave up on its own") {
    val native = new FakeNative(used = cacheSize)
    native.pinned = 700 * mb
    val reservation = new FakeReservation(0)
    val checker = newChecker(native, reservation, () => 400 * mb)

    checker.onTick()
    assert(checker.reservedBytes === native.capacity)

    // Readers finish and something else evicts, behind our back.
    native.pinned = 0
    native.used = 600 * mb
    assert(checker.reservedBytes > native.used, "so the reservation is now above what is held")

    checker.onTick()
    assert(checker.reservedBytes === native.capacity)
  }

  test("the reservation never drifts above the capacity over many rounds") {
    val native = new FakeNative(used = cacheSize)
    native.pinned = 700 * mb
    val reservation = new FakeReservation(0)
    val checker = newChecker(native, reservation, () => 400 * mb)
    checker.onTick()

    native.pinned = 0
    native.used = native.governed
    for (_ <- 1 to 40) {
      checker.onTick()
    }

    assert(checker.reservedBytes === native.capacity)
  }

  // With demand that levels off, the cache settles just under the threshold
  // rather than giving everything up.
  test("settles below the threshold when demand levels off") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    val checker = newChecker(native, reservation, () => 400 * mb) // threshold 200MB

    (1 to 40).foreach(_ => checker.onTick())

    assert(native.capacity > 64 * mb)
    assert(native.capacity <= 200 * mb + 200 * mb * 5 / 100)
  }

  // Execution takes whatever the cache gives up, so Spark's spare storage tracks the cache itself
  // and the pressure never lifts. Reaching the floor is the intended outcome.
  test("walks down to the floor when execution keeps taking more") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    // Greedy execution grows into whatever storage is not reserved, so the spare
    // storage Spark reports ends up being the cache's own capacity.
    val checker = newChecker(native, reservation, () => native.capacity)

    (1 to 40).foreach(_ => checker.onTick())

    // The last stretch is taken in one go, so the cache comes to rest exactly on the floor.
    assert(native.capacity === 64 * mb)
  }

  test("does nothing at all when there is no move worth making") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    // Right at the floor and under pressure: the halving is zero.
    val checker = newChecker(native, reservation, () => 1 * mb, initial = 64 * mb)

    checker.onTick()

    assert(native.targets.isEmpty, "the native side was not touched")
    assert(reservation.calls.isEmpty, "and nothing was borrowed or repaid")
  }

  // There is no later tick to correct a stale reading, so any repayment here would tell Spark
  // memory is free while the cache still holds it. See PeriodicMemoryChecker.stop.
  test("touches nothing on stop") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    val checker = newChecker(native, reservation, () => 400 * mb)
    checker.onTick()
    val reservedBefore = checker.reservedBytes
    val capBefore = native.capacity
    reservation.calls.clear()
    native.targets.clear()

    checker.stop()

    assert(native.targets.isEmpty, "the cache was left as it was")
    assert(native.capacity === capBefore)
    assert(checker.reservedBytes === reservedBefore, "and the reservation stays booked")
    assert(reservation.calls.isEmpty, "so Spark is never told the memory is free")
  }

  test("stop is idempotent and later ticks are ignored") {
    val native = new FakeNative()
    val reservation = new FakeReservation(cacheSize)
    val checker = newChecker(native, reservation, () => 400 * mb)
    checker.stop()
    reservation.calls.clear()
    native.targets.clear()

    checker.stop()
    checker.onTick()

    assert(reservation.calls.isEmpty)
    assert(native.targets.isEmpty)
  }

  test("a failed tick does not kill the loop") {
    val checker = new PeriodicMemoryChecker(
      checkIntervalMs = 1000,
      cacheStorageRatio = 0.5,
      stepBytes = 8 * mb,
      minCacheBytes = 64 * mb,
      maxCacheBytes = cacheSize,
      maxStorageBytes = () => throw new RuntimeException("boom"),
      setCapacity = (_, to) => to,
      reservation = new FakeReservation(0)
    )

    checker.onTick() // must not propagate
  }

  // The startup checks. These only ever fail at startup, so nothing later would catch a mistake
  // in them.

  private def confWith(taskSlots: Int, quantum: Long): SparkConf =
    new SparkConf(false)
      .set("spark.master", "spark://fake:7077")
      .set("spark.executor.cores", taskSlots.toString)
      .set("spark.task.cpus", "1")
      .set(LOAD_QUANTUM.key, quantum.toString)

  test("refuses a floor too small for the reads that will run against it") {
    val conf = confWith(taskSlots = 8, quantum = 8 * mb)
    // Eight concurrent reads of 8MB need 64MB; half that cannot hold them.
    val e = intercept[IllegalArgumentException] {
      PeriodicMemoryChecker.checkConfiguration(conf, minCache = 32 * mb, maxCache = cacheSize)
    }
    assert(e.getMessage.contains(COLUMNAR_VELOX_CACHE_PUSHBACK_MIN_CACHE_SIZE.key))
    // A floor that fits is accepted.
    PeriodicMemoryChecker.checkConfiguration(conf, 64 * mb, cacheSize)
  }

  // A floor at the configured size leaves nothing to adjust. That is a defensible way to run -- a
  // fixed cache Spark has been told about is still better than one it has not -- so it is allowed
  // through with a warning rather than refused.
  test("accepts a capacity that can never move") {
    val conf = confWith(taskSlots = 1, quantum = 8 * mb)
    PeriodicMemoryChecker.checkConfiguration(conf, minCache = 128 * mb, maxCache = 128 * mb)
  }

  // A floor is memory the executor never gives back, so a unit mistake there is silent: nothing
  // fails, the cache simply keeps far more than intended for the whole run.
  test("accepts a suspiciously large floor rather than refusing it") {
    val conf = confWith(taskSlots = 1, quantum = 8 * mb)
    PeriodicMemoryChecker.checkConfiguration(
      conf,
      minCache = 2L * 1024 * mb,
      maxCache = 8L * 1024 * mb)
  }

}

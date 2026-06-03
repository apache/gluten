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
package org.apache.spark.sql.execution

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.util.Random

/**
 * Invariants for [[SchemaJsonInternCache]]: (1) determinism -- equal inputs yield byte-identical /
 * canonical-instance outputs; (2) capacity -- LRU cap = 256, eviction never corrupts later results;
 * (3) concurrency -- contended get-or-compute yields correct results without exception.
 */
class SchemaJsonInternCacheSuite extends SparkFunSuite {

  private def schemaOfWidth(n: Int): StructType =
    StructType((0 until n).map(i => StructField(s"c$i", LongType, nullable = true)))

  // === Invariant 1: determinism ===

  test("encode is deterministic: same StructType => byte-identical output") {
    val intern = new SchemaJsonInternCache
    val s = schemaOfWidth(10)
    val a = intern.encodeBytes(s)
    val b = intern.encodeBytes(s)
    assert(a.sameElements(b), "encodeBytes must be deterministic for equal inputs")
    // intern is a memoizer, not a transformer
    val raw = s.json.getBytes(StandardCharsets.UTF_8)
    assert(a.sameElements(raw), "encodeBytes(s) must equal s.json.getBytes(UTF_8)")
  }

  test("decode is deterministic: same bytes => structurally-equal StructType") {
    val intern = new SchemaJsonInternCache
    val s = StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", StringType),
      StructField("c", LongType, nullable = false)))
    val bytes = s.json.getBytes(StandardCharsets.UTF_8)
    val d1 = intern.decodeStructType(bytes)
    val d2 = intern.decodeStructType(bytes)
    assert(d1 == s)
    assert(d2 == s)
    // canonical-instance contract: equal bytes => same instance (saves repeated parse cost)
    assert(d1.eq(d2), "decodeStructType must return the same canonical instance for equal bytes")
  }

  test("encode canonicality: same StructType returns the same byte array instance") {
    val intern = new SchemaJsonInternCache
    val s = schemaOfWidth(5)
    val a = intern.encodeBytes(s)
    val b = intern.encodeBytes(s)
    assert(a.eq(b), "encodeBytes must return the same canonical byte array for equal inputs")
  }

  // === Invariant 2: capacity ===

  test("cap = 256 entries: eviction past cap does not corrupt later results") {
    val intern = new SchemaJsonInternCache
    val cap = 256
    val total = cap * 4 // 1024 distinct schemas, forces ~75% miss rate
    val schemas = (0 until total).map(i => schemaOfWidth(8 + (i % 16)))
    schemas.foreach(intern.encodeBytes)
    schemas.zipWithIndex.foreach {
      case (s, i) =>
        val cached = intern.encodeBytes(s)
        val raw = s.json.getBytes(StandardCharsets.UTF_8)
        assert(
          cached.sameElements(raw),
          s"entry $i (width=${s.length}) was corrupted across eviction cycles")
    }
  }

  test("decode under cap pressure: >= cap distinct bytes still all decode correctly") {
    val intern = new SchemaJsonInternCache
    val cap = 256
    val distinct = cap * 4
    val pairs = (0 until distinct).map {
      i =>
        val s = schemaOfWidth(8 + (i % 16))
        (s, s.json.getBytes(StandardCharsets.UTF_8))
    }
    // walk twice -- second walk hits a mix of evicted and live entries
    pairs.foreach { case (_, bytes) => intern.decodeStructType(bytes) }
    pairs.foreach {
      case (s, bytes) =>
        val decoded = intern.decodeStructType(bytes)
        assert(decoded == s, s"decoded != expected for width=${s.length}")
    }
  }

  // === Invariant 3: concurrency ===

  test("concurrent get-or-compute: N threads on overlapping keys yields correct results") {
    val intern = new SchemaJsonInternCache
    val threads = 8
    val keysPerThread = 200
    val sharedKeySpace = 64 // overlap forces contention on same cache slots
    val schemas = (0 until sharedKeySpace).map(i => schemaOfWidth(8 + (i % 12)))

    val pool = Executors.newFixedThreadPool(threads)
    val start = new CountDownLatch(1)
    val errors = new AtomicInteger(0)
    val random = new Random(42)

    val futures = (0 until threads).map {
      tid =>
        val rnd = new Random(random.nextLong())
        pool.submit(new Runnable {
          override def run(): Unit = {
            start.await()
            var i = 0
            while (i < keysPerThread) {
              val s = schemas(rnd.nextInt(sharedKeySpace))
              try {
                val enc = intern.encodeBytes(s)
                val raw = s.json.getBytes(StandardCharsets.UTF_8)
                if (!enc.sameElements(raw)) errors.incrementAndGet()

                val dec = intern.decodeStructType(raw)
                if (dec != s) errors.incrementAndGet()
              } catch {
                case _: Throwable => errors.incrementAndGet()
              }
              i += 1
            }
          }
        })
    }
    start.countDown()
    futures.foreach(_.get(60, TimeUnit.SECONDS))
    pool.shutdown()
    assert(pool.awaitTermination(10, TimeUnit.SECONDS), "thread pool did not terminate")
    assert(
      errors.get() == 0,
      s"${errors.get()} concurrent get-or-compute errors out of ${threads * keysPerThread} ops")
  }
}

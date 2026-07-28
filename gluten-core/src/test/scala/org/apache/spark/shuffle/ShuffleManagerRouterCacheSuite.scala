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
package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.test.SharedSparkSession

import java.util.concurrent.{CopyOnWriteArrayList, CyclicBarrier}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Reproduces the two ways [[ShuffleManagerRouter]]'s cache mishandles the executor-side lifecycle,
 * where the router is populated lazily and concurrently by task threads rather than by a single
 * driver coordinator.
 */
class ShuffleManagerRouterCacheSuite extends SharedSparkSession {
  import ShuffleManagerRouterCacheSuite._

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SHUFFLE_MANAGER.key, classOf[GlutenShuffleManager].getName)
      .set(UI_ENABLED, false)

  override protected def beforeEach(): Unit = {
    ShuffleManagerRegistry.get().clear()
    // Register a no-op manager that accepts every dependency, so router lookups resolve
    // without delegating into a real SortShuffleManager.
    ShuffleManagerRegistry
      .get()
      .register(
        new LookupKey {
          override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
        },
        classOf[AcceptAllShuffleManager].getName)
  }

  override protected def afterEach(): Unit = {
    ShuffleManagerRegistry.get().clear()
  }

  test("unregisterShuffle tolerates a shuffleId never registered on this router") {
    // On a multi-executor cluster, Spark broadcasts RemoveShuffle to every executor, so a
    // router that never registered/served this shuffleId still receives unregisterShuffle.
    val gm = spark.sparkContext.env.shuffleManager
    // Force the router to build before exercising it.
    gm.shuffleBlockResolver

    val neverRegistered = 987654321
    // Should behave like SortShuffleManager: return a boolean, not throw.
    assert(!gm.unregisterShuffle(neverRegistered))
  }

  test("concurrent first-touch of a new shuffleId does not fail tasks") {
    // On a multi-core executor, N tasks of the same new shuffleId call getReader/getWriter
    // concurrently. The cache is populated lazily, so they race to register the manager.
    val gm = spark.sparkContext.env.shuffleManager
    gm.shuffleBlockResolver // force build

    val concurrency = 4
    val iterations = 200
    val idGen = new AtomicInteger(100000)
    val errors = new CopyOnWriteArrayList[Throwable]()

    (0 until iterations).foreach {
      _ =>
        val shuffleId = idGen.getAndIncrement()
        val barrier = new CyclicBarrier(concurrency)
        val threads = (0 until concurrency).map {
          _ =>
            val t = new Thread(
              () => {
                try {
                  barrier.await()
                  val dep: ShuffleDependency[Any, Any, Any] = null
                  val handle = new BaseShuffleHandle(shuffleId, dep)
                  gm.getReader(
                    handle,
                    0,
                    1,
                    0,
                    1,
                    null.asInstanceOf[TaskContext],
                    null.asInstanceOf[ShuffleReadMetricsReporter])
                } catch {
                  case th: Throwable => errors.add(th)
                }
              })
            t.start()
            t
        }
        threads.foreach(_.join())
    }

    assert(
      errors.isEmpty,
      s"Expected no errors from concurrent first-touch but got ${errors.size}: " +
        errors.toArray.map(_.toString).toSet.mkString("; "))
  }
}

object ShuffleManagerRouterCacheSuite {

  /** A minimal [[ShuffleManager]] that accepts everything and does no real work. */
  class AcceptAllShuffleManager(conf: SparkConf) extends ShuffleManager {
    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
      new BaseShuffleHandle(shuffleId, dependency)
    }

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
      null.asInstanceOf[ShuffleWriter[K, V]]
    }

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      null.asInstanceOf[ShuffleReader[K, C]]
    }

    override def unregisterShuffle(shuffleId: Int): Boolean = true

    override def shuffleBlockResolver: ShuffleBlockResolver = {
      null.asInstanceOf[ShuffleBlockResolver]
    }

    override def stop(): Unit = {}
  }
}

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

import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.test.SharedSparkSession

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

class GlutenShuffleManagerSuite extends SharedSparkSession {
  import GlutenShuffleManagerSuite._
  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SHUFFLE_MANAGER.key, classOf[GlutenShuffleManager].getName)
      .set(UI_ENABLED, false)

  override protected def beforeEach(): Unit = {
    val registry = ShuffleManagerRegistry.get()
    registry.clear()
  }

  override protected def afterEach(): Unit = {
    val registry = ShuffleManagerRegistry.get()
    registry.clear()
    counter1.clear()
    counter2.clear()
  }

  test("register one") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("stop") == 0)
    gm.stop()
    assert(counter1.count("stop") == 1)
    gm.stop()
    gm.stop()
    assert(counter1.count("stop") == 3)
  }

  test("register two") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)
    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager2].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 0)
    // The statement calls #registerShuffle internally.
    val dep =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep.shuffleId)
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 1)

    assert(counter1.count("stop") == 0)
    assert(counter2.count("stop") == 0)
    gm.stop()
    assert(counter1.count("stop") == 1)
    assert(counter2.count("stop") == 1)
  }

  test("register two - disordered registration") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 0)
    val dep1 =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep1.shuffleId)
    assert(counter1.count("registerShuffle") == 1)
    assert(counter2.count("registerShuffle") == 0)

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager2].getName)

    // The statement calls #registerShuffle internally.
    val dep2 =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep2.shuffleId)
    assert(counter1.count("registerShuffle") == 1)
    assert(counter2.count("registerShuffle") == 1)

    assert(counter1.count("stop") == 0)
    assert(counter2.count("stop") == 0)
    gm.stop()
    assert(counter1.count("stop") == 1)
    assert(counter2.count("stop") == 1)
  }

  test("register two - with empty key") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)
    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = false
      },
      classOf[ShuffleManager2].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 0)
    // The statement calls #registerShuffle internally.
    val dep =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep.shuffleId)
    assert(counter1.count("registerShuffle") == 1)
    assert(counter2.count("registerShuffle") == 0)
  }

  test("register recursively") {
    val registry = ShuffleManagerRegistry.get()

    assertThrows[IllegalArgumentException](
      registry.register(
        new LookupKey {
          override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
        },
        classOf[GlutenShuffleManager].getName))
  }

  test("register recursively - subclass of GlutenShuffleManager") {
    val registry = ShuffleManagerRegistry.get()

    assertThrows[IllegalArgumentException](
      registry.register(
        new LookupKey {
          override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
        },
        classOf[GlutenSubShuffleManager].getName))
  }

  test("register recursively - ShuffleManager interface") {
    val registry = ShuffleManagerRegistry.get()

    assertThrows[IllegalArgumentException](
      registry.register(
        new LookupKey {
          override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
        },
        classOf[ShuffleManager].getName))
  }

  test("register duplicated") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)
    assertThrows[IllegalArgumentException](
      registry.register(
        new LookupKey {
          override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
        },
        classOf[ShuffleManager1].getName))
  }
}

object GlutenShuffleManagerSuite {
  private val counter1 = new InvocationCounter
  private val counter2 = new InvocationCounter

  class ShuffleManager1(conf: SparkConf) extends ShuffleManager {
    private val delegate = new SortShuffleManager(conf)
    private val counter = counter1
    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
      counter.increment("registerShuffle")
      delegate.registerShuffle(shuffleId, dependency)
    }

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
      counter.increment("getWriter")
      delegate.getWriter(handle, mapId, context, metrics)
    }

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      counter.increment("getReader")
      delegate.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      counter.increment("unregisterShuffle")
      delegate.unregisterShuffle(shuffleId)
    }

    override def shuffleBlockResolver: ShuffleBlockResolver = {
      counter.increment("shuffleBlockResolver")
      delegate.shuffleBlockResolver
    }

    override def stop(): Unit = {
      counter.increment("stop")
      delegate.stop()
    }
  }

  class ShuffleManager2(conf: SparkConf, isDriver: Boolean) extends ShuffleManager {
    private val delegate = new SortShuffleManager(conf)
    private val counter = counter2
    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
      counter.increment("registerShuffle")
      delegate.registerShuffle(shuffleId, dependency)
    }

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
      counter.increment("getWriter")
      delegate.getWriter(handle, mapId, context, metrics)
    }

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      counter.increment("getReader")
      delegate.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      counter.increment("unregisterShuffle")
      delegate.unregisterShuffle(shuffleId)
    }

    override def shuffleBlockResolver: ShuffleBlockResolver = {
      counter.increment("shuffleBlockResolver")
      delegate.shuffleBlockResolver
    }

    override def stop(): Unit = {
      counter.increment("stop")
      delegate.stop()
    }
  }

  private class InvocationCounter {
    private val counter: mutable.Map[String, AtomicInteger] = mutable.Map()

    def increment(name: String): Unit = synchronized {
      counter.getOrElseUpdate(name, new AtomicInteger()).incrementAndGet()
    }

    def count(name: String): Int = {
      counter.getOrElse(name, new AtomicInteger()).get()
    }

    def clear(): Unit = {
      counter.clear()
    }
  }

  // A subclass of GlutenShuffleManager is the exact class that recurses when
  // registered: each instance builds its own router that instantiates the
  // registered classes again.
  class GlutenSubShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends GlutenShuffleManager(conf, isDriver)

  private object DummyPartitioner extends Partitioner {
    override def numPartitions: Int = 0
    override def getPartition(key: Any): Int = 0
  }
}

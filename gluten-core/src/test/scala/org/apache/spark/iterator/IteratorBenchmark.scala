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
package org.apache.spark.iterator

import org.apache.gluten.iterator.Iterators
import org.apache.gluten.iterator.Iterators.V1

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.task.TaskResources
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.TimeUnit

object IteratorBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Iterator Nesting") {
      TaskResources.runUnsafe {
        val nPayloads: Int = 50000000 // 50 millions

        def makeScalaIterator: Iterator[Any] = {
          (0 until nPayloads).view.map { _: Int => new Object }.iterator
        }

        def compareIterator(name: String)(
            makeGlutenIterator: Iterators.Version => Iterator[Any]): Unit = {
          val benchmark = new Benchmark(name, nPayloads, output = output)
          benchmark.addCase("Scala Iterator") {
            _ =>
              val count = makeScalaIterator.count(_ => true)
              assert(count == nPayloads)
          }
          benchmark.addCase("Gluten Iterator V1") {
            _ =>
              val count = makeGlutenIterator(V1).count(_ => true)
              assert(count == nPayloads)
          }
          benchmark.run()
        }

        compareIterator("0 Levels Nesting") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .create()
        }
        compareIterator("1 Levels Nesting - read") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .collectReadNanos { _ => }
              .create()
        }
        compareIterator("5 Levels Nesting - read") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .create()
        }
        compareIterator("10 Levels Nesting - read") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .collectReadNanos { _ => }
              .create()
        }
        compareIterator("1 Levels Nesting - recycle") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .recycleIterator {}
              .create()
        }
        compareIterator("5 Levels Nesting - recycle") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .create()
        }
        compareIterator("10 Levels Nesting - recycle") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .create()
        }
      }
    }

    runBenchmark("Iterator Multi Threads") {
      val nPayloads: Int = 50000000 // 50 millions

      def makeScalaIterator: Iterator[Any] = {
        (0 until nPayloads).view.map { _: Int => new Object }.iterator
      }

      def compareMultiThreadsIterator(name: String, threads: Int = 3)(
          makeGlutenIterator: Iterators.Version => Iterator[Any]): Unit = {
        val benchmark = new Benchmark(name, nPayloads, output = output)
        benchmark.addCase("Scala Iterator") {
          _ =>
            val pool = ThreadUtils.newDaemonFixedThreadPool(threads, "ScalaIterator")
            for (_ <- 0 until threads) {
              pool.execute(
                () => {
                  TaskResources.runUnsafe {
                    val count = makeScalaIterator.count(_ => true)
                    assert(count == nPayloads)
                  }
                })
            }
            pool.shutdown()
            pool.awaitTermination(10, TimeUnit.SECONDS)
        }
        benchmark.addCase("Gluten Iterator V1") {
          _ =>
            val pool = ThreadUtils.newDaemonFixedThreadPool(threads, "GlutenIteratorV1")
            for (_ <- 0 until threads) {
              pool.execute(
                () => {
                  TaskResources.runUnsafe {
                    val count = makeGlutenIterator(V1).count(_ => true)
                    assert(count == nPayloads)
                  }
                })
            }
            pool.shutdown()
            pool.awaitTermination(10, TimeUnit.SECONDS)
        }
        benchmark.run()
      }

      compareMultiThreadsIterator("Multi Threads - recycle") {
        version =>
          var count = 0
          Iterators
            .wrap(version, makeScalaIterator)
            .recyclePayload(_ => count += 1)
            .recycleIterator(assert(count == nPayloads))
            .create()
      }
    }
  }
}

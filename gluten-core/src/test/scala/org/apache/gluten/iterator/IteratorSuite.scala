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
package org.apache.gluten.iterator

import org.apache.gluten.iterator.Iterators.{V1, WrapperBuilder}

import org.apache.spark.task.TaskResources

import org.scalatest.funsuite.AnyFunSuite

class IteratorV1Suite extends IteratorSuite {
  override protected def wrap[A](in: Iterator[A]): WrapperBuilder[A] = Iterators.wrap(V1, in)

  test("Sub-millisecond read durations accumulate with carry-over instead of truncating") {
    val reported = scala.collection.mutable.ArrayBuffer.empty[Long]
    val accumulator = new IteratorsV1.NanosToMillisAccumulator(reported += _)
    // Three 0.4ms reads: running totals 0.4ms, 0.8ms, 1.2ms. Converting each call to
    // milliseconds on its own would report 0 for all three; the carry-over reports one
    // whole millisecond only once the third read crosses 1ms.
    accumulator.add(400000L)
    accumulator.add(400000L)
    accumulator.add(400000L)
    assert(reported.toSeq == Seq(1L))
    // 0.2ms remainder + 0.9ms = 1.1ms, so another whole millisecond is reported.
    accumulator.add(900000L)
    assert(reported.toSeq == Seq(1L, 1L))
  }
}

abstract class IteratorSuite extends AnyFunSuite {
  protected def wrap[A](in: Iterator[A]): WrapperBuilder[A]

  test("Trivial wrapping") {
    val strings = Array[String]("one", "two", "three")
    val itr = strings.toIterator
    val wrapped = wrap(itr)
      .create()
    assertResult(strings) {
      wrapped.toArray
    }
  }

  test("Complete iterator") {
    var completeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val wrapped = wrap(itr)
        .recycleIterator {
          completeCount += 1
        }
        .create()
      assertResult(strings) {
        wrapped.toArray
      }
      assert(completeCount == 1)
    }
    assert(completeCount == 1)
  }

  test("Complete intermediate iterator") {
    var completeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val _ = wrap(itr)
        .recycleIterator {
          completeCount += 1
        }
        .create()
      assert(completeCount == 0)
    }
    assert(completeCount == 1)
  }

  test("Close payload") {
    var closeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val wrapped = wrap(itr)
        .recyclePayload { _: String => closeCount += 1 }
        .create()
      assertResult(strings) {
        wrapped.toArray
      }
      assert(closeCount == 3)
    }
    assert(closeCount == 3)
  }

  test("Close intermediate payload") {
    var closeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val wrapped = wrap(itr)
        .recyclePayload { _: String => closeCount += 1 }
        .create()
      assertResult(strings.take(2)) {
        wrapped.take(2).toArray
      }
      assert(closeCount == 1) // the first one is closed after consumed
    }
    assert(closeCount == 2) // the second one is closed on task exit
  }

  test("Protect invocation flow") {
    var hasNextCallCount = 0
    var nextCallCount = 0
    val itr = new Iterator[Any] {
      override def hasNext: Boolean = {
        hasNextCallCount += 1
        true
      }

      override def next(): Any = {
        nextCallCount += 1
        new Object
      }
    }
    val wrapped = wrap(itr)
      .protectInvocationFlow()
      .create()
    wrapped.hasNext
    assert(hasNextCallCount == 1)
    assert(nextCallCount == 0)
    wrapped.hasNext
    assert(hasNextCallCount == 1)
    assert(nextCallCount == 0)
    wrapped.next
    assert(hasNextCallCount == 1)
    assert(nextCallCount == 1)
    wrapped.next
    assert(hasNextCallCount == 2)
    assert(nextCallCount == 2)
  }
}

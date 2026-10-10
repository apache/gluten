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

import java.util.concurrent.TimeUnit

class IteratorV1Suite extends IteratorSuite {
  override protected def wrap[A](in: Iterator[A]): WrapperBuilder[A] = Iterators.wrap(V1, in)
}

abstract class IteratorSuite extends AnyFunSuite {
  protected def wrap[A](in: Iterator[A]): WrapperBuilder[A]

  test("Read time is reported in nanoseconds per read") {
    val reported = scala.collection.mutable.ArrayBuffer.empty[Long]
    // Both hasNext and next sleep at least 1ms, so every reported duration has a hard
    // lower bound that does not depend on the clock's resolution.
    val slow = new Iterator[Int] {
      private var i = 0
      override def hasNext: Boolean = {
        Thread.sleep(1)
        i < 3
      }
      override def next(): Int = {
        Thread.sleep(1)
        i += 1
        i
      }
    }
    val wrapped = wrap(slow)
      .collectReadNanos(reported += _)
      .create()
    var reads = List.empty[Int]
    while (wrapped.hasNext) {
      reads = wrapped.next() :: reads
    }
    assert(reads.reverse == List(1, 2, 3))
    // Four hasNext calls and three next calls, each reported once. A millisecond unit
    // would report about 1 for each; nanoseconds report at least 1,000,000.
    assert(reported.size == 7)
    assert(reported.forall(_ >= TimeUnit.MILLISECONDS.toNanos(1)), reported)
  }

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

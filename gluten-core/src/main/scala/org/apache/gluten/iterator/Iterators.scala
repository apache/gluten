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

import org.apache.spark.TaskContext

import IteratorsV1.WrapperBuilderV1

/**
 * Utility class to provide iterator wrappers for non-trivial use cases. E.g. iterators that manage
 * payload's lifecycle.
 */
object Iterators {
  sealed trait Version
  case object V1 extends Version

  private val DEFAULT_VERSION: Version = V1

  trait WrapperBuilder[A] {
    def recyclePayload(closeCallback: (A) => Unit): WrapperBuilder[A]
    def recycleIterator(completionCallback: => Unit): WrapperBuilder[A]
    def collectLifeMillis(onCollected: Long => Unit): WrapperBuilder[A]
    def collectReadNanos(onAdded: Long => Unit): WrapperBuilder[A]
    def asInterruptible(context: TaskContext): WrapperBuilder[A]
    def protectInvocationFlow(): WrapperBuilder[A]
    def create(): Iterator[A]
  }

  def wrap[A](in: Iterator[A]): WrapperBuilder[A] = {
    wrap(DEFAULT_VERSION, in)
  }

  def wrap[A](version: Version, in: Iterator[A]): WrapperBuilder[A] = {
    version match {
      case V1 =>
        new WrapperBuilderV1[A](in)
    }
  }
}

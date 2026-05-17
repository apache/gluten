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
package org.apache.spark.utils

import org.apache.gluten.events.GlutenPlanFallbackEvent

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}

import scala.collection.mutable.ArrayBuffer

object GlutenSuiteUtils {
  def waitUntilEmpty(ctx: SparkContext): Unit = ctx.listenerBus.waitUntilEmpty()

  // Drain any pending events from previous tests before registering the listener.
  // Spark's LiveListenerBus is async, so events posted but not yet dispatched would
  // still be delivered to a listener added afterwards, contaminating the buffer.
  // After the body finishes, callers should invoke waitUntilEmpty before reading
  // the events buffer.
  def withFallbackEventListener(ctx: SparkContext)(
      body: ArrayBuffer[GlutenPlanFallbackEvent] => Unit): Unit = {
    waitUntilEmpty(ctx)
    val events = new ArrayBuffer[GlutenPlanFallbackEvent]
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: GlutenPlanFallbackEvent => events.append(e)
        case _ =>
      }
    }
    ctx.addSparkListener(listener)
    try body(events)
    finally ctx.removeSparkListener(listener)
  }
}

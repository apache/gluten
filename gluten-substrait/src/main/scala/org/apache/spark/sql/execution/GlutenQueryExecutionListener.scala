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

import org.apache.gluten.events.GlutenPlanFallbackEvent

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{GlutenUIUtils, SparkListenerSQLExecutionEnd}

/** A SparkListener that generates complete Gluten UI data after query execution completes. */
class GlutenQueryExecutionListener(sc: SparkContext) extends SparkListener with Logging {

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd =>
      onSQLExecutionEnd(e)
    case _ =>
  }

  private def onSQLExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    try {
      val qe = event.qe
      if (qe == null) {
        // History Server replay or edge case. Rely on per-stage events already in event log.
        return
      }

      val summary =
        GlutenImplicits.collectQueryExecutionFallbackSummary(qe.sparkSession, qe)

      // Combine plan descriptions and fallback reasons from all segments.
      val planStringBuilder = new StringBuilder()
      planStringBuilder.append("== Physical Plan ==\n")
      summary.physicalPlanDescription.foreach(planStringBuilder.append)
      val combinedFallbackReasons =
        summary.fallbackNodeToReason.foldLeft(Map.empty[String, String])(_ ++ _)

      // Post event to listener bus. The event is serialized to event log, so History Server
      // can replay it. GlutenSQLAppStatusListener receives this event and writes to kvstore.
      val fallbackEvent = GlutenPlanFallbackEvent(
        event.executionId,
        summary.numGlutenNodes,
        combinedFallbackReasons.size,
        planStringBuilder.toString(),
        combinedFallbackReasons
      )
      GlutenUIUtils.postEvent(sc, fallbackEvent)
    } catch {
      case e: Exception =>
        logWarning(
          s"Failed to generate complete fallback data for execution ${event.executionId}",
          e)
    }
  }
}

object GlutenQueryExecutionListener {

  /** Register the listener on the status queue. Should be called once during driver start. */
  def register(sc: SparkContext): Unit = {
    if (GlutenUIUtils.uiEnabled(sc)) {
      sc.listenerBus.addToStatusQueue(new GlutenQueryExecutionListener(sc))
    }
  }
}

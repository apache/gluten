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
package org.apache.spark.sql.kafka010

import org.apache.gluten.execution.MicroBatchScanExecTransformer

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{Dataset, GlutenSQLTestsTrait}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, StreamExecution}
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.streaming.OutputMode

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

/** Reuse Spark's source tests while checking the physical scan after successful answer checks. */
private[kafka010] trait GlutenKafkaSourceTest extends KafkaSourceTest with GlutenSQLTestsTrait {

  private val completedKafkaPlans = new ConcurrentLinkedQueue[SparkPlan]()
  private var kafkaScanListener: Option[SparkListener] = None

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    completedKafkaPlans.clear()
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case end: SparkListenerSQLExecutionEnd if end.executionFailure.isEmpty =>
          end.qe match {
            case execution: IncrementalExecution =>
              completedKafkaPlans.add(execution.executedPlan)
            case _ =>
          }
        case _ =>
      }
    }
    spark.sparkContext.addSparkListener(listener)
    kafkaScanListener = Some(listener)
  }

  override protected def afterEach(): Unit = {
    try {
      // Some inherited tests start queries directly instead of using testStream. Observe every
      // successful micro-batch, including stopped queries and Kafka sink writes. A query execution
      // listener would miss streaming executions without an action name.
      spark.sparkContext.listenerBus.waitUntilEmpty(streamingTimeout.toMillis)
      // Assert on the test thread: Spark logs listener failures without failing the test.
      assertKafkaScans(completedKafkaPlans.asScala.toSeq)
    } finally {
      try {
        kafkaScanListener.foreach(spark.sparkContext.removeSparkListener)
        kafkaScanListener = None
        completedKafkaPlans.clear()
      } finally {
        super.afterEach()
      }
    }
  }

  protected def assertKafkaScan(query: StreamExecution, requireScan: Boolean = false): Boolean = {
    val execution = Option(query).flatMap(q => Option(q.lastExecution))
    assertKafkaScans(execution.toSeq.map(_.executedPlan), requireScan)
  }

  private def assertKafkaScans(
      plans: Seq[SparkPlan],
      requireScan: Boolean = false): Boolean = {
    val nativeScans = plans.flatMap {
      _.collect { case scan: MicroBatchScanExecTransformer => scan }
    }
    val sparkScans = plans.flatMap {
      _.collect {
        case scan: MicroBatchScanExec
            if MicroBatchScanExecTransformer.supportsBatchScan(scan.scan) =>
          scan
      }
    }

    // Empty batches and recovery checks may have no physical scan.
    // Dedicated scan tests require one.
    if (requireScan) {
      assert(
        nativeScans.nonEmpty || sparkScans.nonEmpty,
        "No executed Kafka micro-batch scan found")
    }
    nativeScans.foreach {
      scan =>
        assert(
          !scan.output.exists(_.name == "headers"),
          s"Kafka headers must fall back to Spark, but the scan was native:\n$scan")
    }
    sparkScans.foreach {
      scan =>
        assert(
          scan.output.exists(_.name == "headers"),
          "Kafka micro-batch scan fell back to Spark. Build a Kafka-enabled native backend " +
            "and include the Velox Kafka component before running native Kafka tests. " +
            s"The merged upstream integration must also exist in this checkout.\n$scan"
        )
    }
    true
  }

  override def testStream(
      stream: Dataset[_],
      outputMode: OutputMode,
      extraOptions: Map[String, String],
      sink: MemorySink)(actions: StreamAction*): Unit = {
    // Spark's AssertOnQuery uses the current query, or the last stopped query during recovery.
    // Do not require isActive: answer checks can inspect a completed or stopped execution.
    val checkedActions = actions.flatMap {
      case action: CheckAnswerRows =>
        Seq(action, AssertOnQuery(query => assertKafkaScan(query)))
      case action: CheckAnswerRowsContains =>
        Seq(action, AssertOnQuery(query => assertKafkaScan(query)))
      case action: CheckAnswerRowsByFunc =>
        Seq(action, AssertOnQuery(query => assertKafkaScan(query)))
      case action: CheckNewAnswerRows =>
        Seq(action, AssertOnQuery(query => assertKafkaScan(query)))
      case action => Seq(action)
    }
    super.testStream(stream, outputMode, extraOptions, sink)(checkedActions: _*)
  }
}

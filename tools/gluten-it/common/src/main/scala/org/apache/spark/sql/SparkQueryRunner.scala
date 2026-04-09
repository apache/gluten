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
package org.apache.spark.sql

import org.apache.gluten.integration.Query
import org.apache.gluten.integration.metrics.{MetricMapper, PlanMetric}

import org.apache.spark.{SparkContext, Success, TaskKilled}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerExecutorMetricsUpdate, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

import org.apache.commons.lang3.RandomUtils

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object SparkQueryRunner {
  private val availableExecutorMetrics: Set[String] = Set(
    "JVMHeapMemory",
    "JVMOffHeapMemory",
    "OnHeapExecutionMemory",
    "OffHeapExecutionMemory",
    "OnHeapStorageMemory",
    "OffHeapStorageMemory",
    "OnHeapUnifiedMemory",
    "OffHeapUnifiedMemory",
    "DirectPoolMemory",
    "MappedPoolMemory",
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory"
  )

  def runQuery(
      spark: SparkSession,
      desc: String,
      query: Query,
      explain: Boolean,
      metricMapper: MetricMapper,
      executorMetrics: Seq[String],
      randomKillTasks: Boolean): RunResult = {
    val unrecognizableMetrics = executorMetrics.filter(!availableExecutorMetrics.contains(_))
    if (unrecognizableMetrics.nonEmpty) {
      throw new IllegalArgumentException(
        "Unrecognizable metric names: " + unrecognizableMetrics.mkString("Array(", ", ", ")"))
    }

    val sc = spark.sparkContext
    sc.setJobDescription(desc)

    // Wait until all SparkListener events are processed before we start executing the query, to make sure
    // the metrics we collect are accurate.
    sc.listenerBus.waitUntilEmpty()

    // Executor metrics listener.
    val em = new ExecutorMetrics()
    val metricsListener = new MetricsListener(em)
    sc.addSparkListener(metricsListener)

    // Execution time listener.
    val executionTimeListener = new ExecutionTimeListener()
    sc.addSparkListener(executionTimeListener)

    // kill task listener.
    val killTaskListener: Option[KillTaskListener] = if (randomKillTasks) {
      Some(new KillTaskListener(sc))
    } else {
      None
    }
    killTaskListener.foreach(sc.addSparkListener(_))

    println(s"Executing SQL query from resource path ${query.path}...")
    try {
      val otherTracker = new QueryPlanningTracker
      val df = spark.sql(query.sql)
      val rows = QueryPlanningTracker.withTracker(otherTracker) {
        df.collect()
      }
      if (explain) {
        df.explain(extended = true)
      }
      // TODO: sparkTracker and otherTracker include planning time information,
      //  while they are not accurate enough so we paused on using them for now.
      val sparkTracker = df.queryExecution.tracker
      // This makes sure all the SparkListenerSQLExecutionEnd event is processed before we
      // get the execution time and metrics.
      sc.listenerBus.waitUntilEmpty()
      val totalMillis = executionTimeListener.wallMillis()
      val collectedExecutorMetrics =
        executorMetrics.map(name => (name, em.getMetricValue(name))).toMap
      val collectedSQLMetrics = collectSQLMetrics(query.path, metricMapper, df.queryExecution)
      RunResult(rows, totalMillis, collectedSQLMetrics, collectedExecutorMetrics)
    } finally {
      sc.removeSparkListener(metricsListener)
      sc.removeSparkListener(executionTimeListener)
      killTaskListener.foreach(
        l => {
          sc.removeSparkListener(l)
          println(s"Successful kill rate ${"%.2f%%"
              .format(100 * l.successfulKillRate())} during execution of app: ${sc.applicationId}")
        })
      sc.setJobDescription(null)
    }
  }

  private def collectAllNodes(plan: SparkPlan, nodes: mutable.LinkedHashMap[Int, SparkPlan]): Unit =
    plan match {
      case a: AdaptiveSparkPlanExec =>
        nodes += a.id -> a
        collectAllNodes(a.executedPlan, nodes)
      case q: QueryStageExec =>
        nodes += q.id -> q
        collectAllNodes(q.plan, nodes)
      case r: ReusedExchangeExec =>
        nodes += r.id -> r
        collectAllNodes(r.child, nodes)
      case other =>
        nodes += other.id -> other
        other.children.foreach(c => collectAllNodes(c, nodes))
    }

  private def collectSQLMetrics(
      queryPath: String,
      mapper: MetricMapper,
      qe: QueryExecution): Seq[PlanMetric] = {
    val nodes = mutable.LinkedHashMap[Int, SparkPlan]()
    collectAllNodes(qe.executedPlan, nodes)
    val all = nodes.flatMap {
      case (_, p) =>
        p.metrics.map {
          case keyValue @ (k, m) =>
            val tags = mapper.map(p, k, m)
            PlanMetric(queryPath, p.id, p.nodeName, k, m, tags.toSet)
        }
    }
    all.toSeq
  }
}

case class RunResult(
    rows: Seq[Row],
    executionTimeMillis: Long,
    sqlMetrics: Seq[PlanMetric],
    executorMetrics: Map[String, Long])

class MetricsListener(em: ExecutorMetrics) extends SparkListener {
  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    executorMetricsUpdate.executorUpdates.foreach {
      case (_, peakUpdates) =>
        em.compareAndUpdatePeakValues(peakUpdates)
    }
    super.onExecutorMetricsUpdate(executorMetricsUpdate)
  }
}

class KillTaskListener(val sc: SparkContext) extends SparkListener {
  private val taskCount = new AtomicInteger(0)
  private val killCount = new AtomicInteger(0)

  private val sync = new Object()
  private val stageKillWaitTimeLookup =
    new java.util.concurrent.ConcurrentHashMap[Int, Long]
  private val stageKillMaxWaitTimeLookup =
    new java.util.concurrent.ConcurrentHashMap[Int, Long]

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    taskCount.getAndIncrement()

    val killer = new Thread {
      override def run(): Unit = {

        def wait(): Long = {
          val startMs = System.currentTimeMillis()
          while (true) {
            sync.synchronized {
              val total = Math.min(
                stageKillMaxWaitTimeLookup.computeIfAbsent(taskStart.stageId, _ => Long.MaxValue),
                stageKillWaitTimeLookup
                  .computeIfAbsent(taskStart.stageId, _ => KillTaskListener.INIT_WAIT_TIME_MS)
              )
              val elapsed = System.currentTimeMillis() - startMs
              val remaining = total - elapsed
              if (remaining <= 0L) {
                // 50ms, 100ms, 200ms, 400ms...
                stageKillWaitTimeLookup.put(taskStart.stageId, total * 2)
                sync.notifyAll()
                return elapsed
              }
              sync.wait(remaining)
            }
          }
          throw new IllegalStateException()
        }

        val elapsed = wait()

        // We have 50% chance to kill the task. FIXME make it configurable?
        if (RandomUtils.nextFloat(0.0f, 1.0f) < 0.5f) {
          if (sc.isStopped) {
            return
          }
          println(
            s"Killing task after $elapsed ms: [task ID:  ${taskStart.taskInfo.taskId}, stage ID: ${taskStart.stageId}, attempt number: ${taskStart.taskInfo.attemptNumber}]...")
          sc.killTaskAttempt(taskStart.taskInfo.taskId, interruptThread = true)
        }
      }
    }
    killer.setDaemon(true)
    killer.start()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.reason match {
      case TaskKilled(_, _, _, _) =>
        killCount.getAndIncrement()
        println(
          s"Task successfully killed: ${taskEnd.taskInfo.taskId}, stage ID: ${taskEnd.stageId}, attempt number: ${taskEnd.taskInfo.attemptNumber}]")
      case Success =>
        // once one task from the stage ends, kill all the others immediately
        sync.synchronized {
          stageKillMaxWaitTimeLookup.put(
            taskEnd.stageId,
            (taskEnd.taskInfo.duration * 0.8d).asInstanceOf[Long])
          sync.notifyAll()
        }
        println(
          s"Task ended normally: ${taskEnd.taskInfo.taskId}, stage ID: ${taskEnd.stageId}, attempt number: ${taskEnd.taskInfo.attemptNumber}]")
      case _ =>
    }

  }

  def successfulKillRate(): Float = {
    killCount.get().asInstanceOf[Float] / taskCount.get()
  }
}

object KillTaskListener {
  private val INIT_WAIT_TIME_MS: Long = 50L
}

class ExecutionTimeListener extends SparkListener {
  private val executionStart = new ConcurrentHashMap[Long, Long]().asScala
  private val executionEnd = new ConcurrentHashMap[Long, Long]().asScala

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      executionStart(e.executionId) = e.time
    case e: SparkListenerSQLExecutionEnd =>
      executionEnd(e.executionId) = e.time
    case _ =>
  }

  def wallMillis(): Long = {
    executionEnd.map {
      case (executionId, endTime) =>
        val startTime = executionStart(executionId)
        endTime - startTime
    }.sum
  }
}

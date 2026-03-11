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
package org.apache.gluten.integration.metrics

import org.apache.gluten.integration.action.TableRender
import org.apache.gluten.integration.action.TableRender.Field.Leaf

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.commons.io.output.ByteArrayOutputStream

import java.io.File
import java.nio.charset.Charset

import scala.reflect.ClassTag

case class PlanMetric(
    queryPath: String,
    plan: SparkPlan,
    key: String,
    metric: SQLMetric,
    tags: Set[MetricTag]) {

  def containsTags(tag: MetricTag): Boolean = {
    tags.contains(tag)
  }
}

object PlanMetric {
  def newReporter(`type`: String): Reporter = `type` match {
    case "execution-time" =>
      new ChainedReporter(
        Seq(
          new NodeTimeReporter(10),
          new StepTimeReporter(30)
        ))
    case "join-selectivity" =>
      new ChainedReporter(Seq(new JoinSelectivityReporter(30)))
    case other => throw new IllegalArgumentException(s"Metric reporter type $other not defined")
  }

  sealed trait Reporter {
    def toString(metrics: Seq[PlanMetric]): String
  }

  private class ChainedReporter(reporter: Seq[Reporter]) extends Reporter {
    override def toString(metrics: Seq[PlanMetric]): String = {
      val sb = new StringBuilder()
      reporter.foreach {
        r =>
          sb.append(r.toString(metrics))
          sb.append(System.lineSeparator())
      }
      sb.toString()
    }
  }

  private class StepTimeReporter(topN: Int) extends Reporter {
    private def toNanoTime(m: SQLMetric): Long = m.metricType match {
      case "nsTiming" => m.value
      case "timing" => m.value * 1000000
    }

    override def toString(metrics: Seq[PlanMetric]): String = {
      val sb = new StringBuilder()
      val selfTimes = metrics
        .filter(_.containsTags(MetricTag.IsSelfTime))
      val sorted = selfTimes.sortBy(m => toNanoTime(m.metric))(Ordering.Long.reverse)
      sb.append(s"Top $topN computation steps that took longest time to execute: ")
      sb.append(System.lineSeparator())
      sb.append(System.lineSeparator())
      val tr: TableRender[Seq[String]] =
        TableRender.create(
          Leaf("Query"),
          Leaf("Node ID"),
          Leaf("Node Name"),
          Leaf("Step Time (ns)"))
      for (i <- 0 until (topN.min(sorted.size))) {
        val m = sorted(i)
        val queryPath = new File(m.queryPath).toPath.getFileName.toString
        tr.appendRow(
          Seq(
            queryPath,
            m.plan.id.toString,
            m.plan.nodeName,
            s"[${m.metric.name.getOrElse("")}] ${toNanoTime(m.metric).toString}"))
      }
      val out = new ByteArrayOutputStream()
      tr.print(out)
      sb.append(out.toString(Charset.defaultCharset))
      sb.toString()
    }
  }

  private class NodeTimeReporter(topN: Int) extends Reporter {
    import NodeTimeReporter._
    private def toNanoTime(m: SQLMetric): Long = m.metricType match {
      case "nsTiming" => m.value
      case "timing" => m.value * 1000000
    }

    override def toString(metrics: Seq[PlanMetric]): String = {
      val sb = new StringBuilder()
      val selfTimes = metrics
        .filter(_.containsTags(MetricTag.IsSelfTime))
      val rows: Seq[TableRow] = selfTimes
        .groupBy(m => m.plan.id)
        .toSeq
        .map {
          perPlanId =>
            assert(perPlanId._2.map(_.plan.id).distinct.count(_ => true) == 1)
            assert(perPlanId._2.map(_.queryPath).distinct.count(_ => true) == 1)
            val head = perPlanId._2.head
            TableRow(
              head.queryPath,
              head.plan,
              perPlanId._2.map(m => toNanoTime(m.metric)).sum,
              perPlanId._2.map(m => (m.key, m.metric)))
        }
      val sorted = rows.sortBy(m => m.selfTimeNs)(Ordering.Long.reverse)
      sb.append(s"Top $topN plan nodes that took longest time to execute: ")
      sb.append(System.lineSeparator())
      sb.append(System.lineSeparator())
      val tr: TableRender[Seq[String]] =
        TableRender.create(
          Leaf("Query"),
          Leaf("Node ID"),
          Leaf("Node Name"),
          Leaf("Node Time (ns)"))
      for (i <- 0 until (topN.min(sorted.size))) {
        val row = sorted(i)
        val f = new File(row.queryPath).toPath.getFileName.toString
        tr.appendRow(Seq(f, row.plan.id.toString, row.plan.nodeName, s"${row.selfTimeNs}"))
      }
      val out = new ByteArrayOutputStream()
      tr.print(out)
      sb.append(out.toString(Charset.defaultCharset))
      sb.toString()
    }
  }

  private object NodeTimeReporter {
    private case class TableRow(
        queryPath: String,
        plan: SparkPlan,
        selfTimeNs: Long,
        metrics: Seq[(String, SQLMetric)])
  }

  private class JoinSelectivityReporter(topN: Int) extends Reporter {
    import JoinSelectivityReporter._
    private def toNumRows(m: SQLMetric): Long = m.metricType match {
      case "sum" => m.value
    }

    override def toString(metrics: Seq[PlanMetric]): String = {
      val sb = new StringBuilder()
      sb.append(s"Top $topN join operations that has lowest selectivity: ")
      sb.append(System.lineSeparator())
      sb.append(System.lineSeparator())

      val tr: TableRender[Seq[String]] =
        TableRender.create(
          Leaf("Query"),
          Leaf("Node ID"),
          Leaf("Node Name"),
          Leaf("Input Row Count"),
          Leaf("Output Row Count"),
          Leaf("Selectivity"))
      val probeInputNumRows = metrics
        .filter(_.containsTags(MetricTag.IsJoinProbeInputNumRows))
        .groupBy(m => m.plan.id)
        .toSeq
        .sortBy(_._1)
      val probeOutputNumRows = metrics
        .filter(_.containsTags(MetricTag.IsJoinProbeOutputNumRows))
        .groupBy(m => m.plan.id)
        .toSeq
        .sortBy(_._1)
      assert(probeInputNumRows.size == probeOutputNumRows.size)
      val rows = probeInputNumRows
        .zip(probeOutputNumRows)
        .map {
          case ((id1, inputMetrics), (id2, outputMetrics)) =>
            assert(id1 == id2)
            val queryPath = new File(inputMetrics.head.queryPath).toPath.getFileName.toString
            val inputNumRows = inputMetrics.map(m => toNumRows(m.metric)).sum
            val outputNumRows = outputMetrics.map(m => toNumRows(m.metric)).sum
            val selectivity = outputNumRows.toDouble / inputNumRows.toDouble
            TableRow(
              queryPath,
              id1,
              inputMetrics.head.plan.nodeName,
              inputNumRows,
              outputNumRows,
              selectivity)
        }
        .sortBy(_.selectivity)
      for (i <- 0 until (topN.min(rows.size))) {
        val row = rows(i)
        tr.appendRow(
          Seq(
            row.queryPath,
            row.planId.toString,
            row.planNodeName,
            row.probeInputNumRows.toString,
            row.probeOutputNumRows.toString,
            "%.3f".format(row.selectivity)))
      }
      val out = new ByteArrayOutputStream()
      tr.print(out)
      sb.append(out.toString(Charset.defaultCharset))
      sb.toString()
    }
  }

  private object JoinSelectivityReporter {
    private case class TableRow(
        queryPath: String,
        planId: Long,
        planNodeName: String,
        probeInputNumRows: Long,
        probeOutputNumRows: Long,
        selectivity: Double)
  }
}

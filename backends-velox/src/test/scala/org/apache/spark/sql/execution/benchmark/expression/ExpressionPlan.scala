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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.execution.ValidationResult

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project, Repartition, Window}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.ClassicTypes
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.GlutenImplicits.withSQLConf
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.util.UUID

import scala.collection.mutable.ArrayBuffer

/** Generates and prepares standard plans; owns no input or execution resources. */
private[expression] class ExpressionPlan(
    val spark: SparkSession,
    val scenario: Catalog.CaseDef,
    val context: Data.Context,
    val dataPlan: Data.Plan) {
  lazy val aggregate: Boolean = optimizedPlan.exists(_.isInstanceOf[Aggregate])
  lazy val window: Boolean = optimizedPlan.exists(_.isInstanceOf[Window])
  private val complete = scenario.mode.contains("complete")
  lazy val partitions = if ((!aggregate && !window) || complete) 1 else context.inputPartitions
  private val session = spark.asInstanceOf[ClassicTypes.ClassicSparkSession]
  private[expression] def inPlanningScope[T](body: => T): T = {
    val task = TaskContext.get()
    TaskContext.unset()
    try session.withActive(body)
    finally TaskContext.setTaskContext(task)
  }
  lazy val optimizedPlan: LogicalPlan = inPlanningScope {
    withSQLConf(
      "spark.gluten.enabled" -> "false",
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.groupByOrdinal" -> "false") {
      val name = "benchmark_input_" + UUID.randomUUID().toString.replace("-", "")
      val frame = session.internalCreateDataFrame(
        spark.sparkContext.parallelize(
          Seq.empty[InternalRow],
          if (complete) 1 else context.inputPartitions),
        dataPlan.inputSchema)
      frame.createOrReplaceTempView(name)
      try {
        val input = if (complete) s"(SELECT /*+ COALESCE(1) */ * FROM $name)" else name
        val keys = scenario.groupingKeys.getOrElse(Seq.empty)
        require(keys.size <= 1, "One grouping key supported")
        val select =
          (keys :+ (scenario.sql + " AS __gluten_expression_benchmark_result")).mkString(", ")
        val group = if (keys.isEmpty) "" else keys.mkString(" GROUP BY ", ", ", "")
        session.sql(s"SELECT $select FROM $input$group").queryExecution.optimizedPlan
      } finally session.catalog.dropTempView(name)
    }
  }
  def outputSchema: StructType = optimizedPlan.schema
  def groupingExpressions: Seq[Expression] = optimizedPlan.collectFirst {
    case a: Aggregate => a.groupingExpressions
  }.getOrElse(Seq.empty)

  private def physical(native: Boolean): SparkPlan = SQLConf.withExistingConf(
    session.sessionState.conf) {
    withSQLConf(
      "spark.gluten.enabled" -> native.toString,
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "1",
      "spark.gluten.sql.mergeTwoPhasesAggregate.enabled" -> complete.toString
    ) {
      val source = session.sessionState.executePlan(optimizedPlan).sparkPlan
      if (native) QueryExecution.prepareExecutedPlan(session, source)
      else {
        val columnar = source.transformUp {
          case scan: RDDScanExec => ColumnarToRowExec(InputAdapter(RowToColumnarExec(scan)))
        }
        QueryExecution.prepareExecutedPlan(session, columnar)
      }
    }
  }
  lazy val vanillaPlan = inPlanningScope(physical(false))
  lazy val nativePlan = inPlanningScope(physical(true))
  private def exchanges(plan: SparkPlan): Seq[Exchange] = plan.collect { case e: Exchange => e }
  lazy val vanillaExchange = {
    val nodes = exchanges(vanillaPlan)
    require(nodes.size <= 1, "Only one shuffle boundary is supported")
    val aggregations = vanillaPlan.collect { case a: BaseAggregateExec => a }
    if (aggregate) {
      require(aggregations.size == (if (complete) 1 else 2), "Unexpected aggregate stages")
      require(
        aggregations.forall(
          a =>
            a.aggregateExpressions.size == 1 &&
              a.aggregateExpressions.forall(e => !e.isDistinct && e.filter.isEmpty)),
        "One non-distinct aggregate without FILTER is required"
      )
    }
    nodes.headOption
  }
  lazy val nativeExchange = {
    val nodes = exchanges(nativePlan)
    require(
      nodes.size <= 1 && nodes.size == vanillaExchange.size,
      "Native and JVM shuffle boundaries differ")
    nodes.headOption.map {
      case e: ColumnarShuffleExchangeExec => e
      case other => throw new IllegalArgumentException(s"Native shuffle fell back: $other")
    }
  }
  def validate(): ValidationResult = {
    require(
      optimizedPlan.collect { case w: Window => w }.size <= 1,
      "One window specification supported")
    val reasons = ArrayBuffer.empty[String]
    GlutenExplainUtils.visitFallbackNodes(
      nativePlan,
      (node, reason) =>
        node match {
          // The benchmark supplies the scan's input; it does not execute the scan itself.
          case _: RDDScanExec =>
          case _ => reasons += reason
        }
    )
    if (reasons.isEmpty) ValidationResult.succeeded
    else ValidationResult.failed(reasons.distinct.mkString("; "))
  }
  lazy val outputDeterminism: Seq[Boolean] = comparableOutputs(optimizedPlan)

  private def comparableOutputs(plan: LogicalPlan): Seq[Boolean] = {
    val inputs =
      plan.children.flatMap(child => child.output.map(_.exprId).zip(comparableOutputs(child))).toMap
    def comparable(expression: Expression): Boolean =
      expression.deterministic && expression.references.forall(
        a => inputs.getOrElse(a.exprId, false))
    plan match {
      case p: Project => p.projectList.map(comparable)
      case a: Aggregate =>
        val stableGroups = a.groupingExpressions.forall(comparable)
        a.aggregateExpressions.map(e => stableGroups && comparable(e))
      case w: Window =>
        w.child.output.map(a => inputs.getOrElse(a.exprId, false)) ++
          w.windowExpressions.map(comparable)
      case r: Repartition => r.output.map(a => inputs.getOrElse(a.exprId, false))
      case leaf if leaf.children.isEmpty => leaf.output.map(_ => true)
      case other => throw new IllegalArgumentException(s"Unsupported comparison plan: $other")
    }
  }

}

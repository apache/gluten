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
package org.apache.gluten.extension

import org.apache.gluten.extension.columnar.ColumnarRuleApplier
import org.apache.gluten.extension.columnar.transition.Transitions
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

object GlutenColumnarRule {
  // Utilities to infer columnar rule's caller's property:
  // ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
  private case class DummyRowOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }

  private case class DummyColumnarOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }
}

case class GlutenColumnarRule(
    session: SparkSession,
    applierBuilder: SparkSession => ColumnarRuleApplier)
  extends ColumnarRule
  with Logging
  with LogLevelUtil {

  import GlutenColumnarRule._

  /**
   * Note: Do not implement this API. We basically inject all of Gluten's physical rules through
   * `postColumnarTransitions`.
   *
   * See: https://github.com/oap-project/gluten/pull/4790
   */
  final override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    // To infer caller's property: ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
    if (plan.supportsColumnar) {
      DummyColumnarOutputExec(plan)
    } else {
      DummyRowOutputExec(plan)
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    val (originalPlan, outputsColumnar) = plan match {
      case DummyRowOutputExec(child) =>
        (child, false)
      case RowToColumnarExec(DummyRowOutputExec(child)) =>
        (child, true)
      case DummyColumnarOutputExec(child) =>
        (child, true)
      case ColumnarToRowExec(DummyColumnarOutputExec(child)) =>
        (child, false)
      case _ =>
        throw new IllegalStateException(
          "This should not happen. Please leave an issue at" +
            " https://github.com/apache/gluten.")
    }
    val vanillaPlan = Transitions.insert(originalPlan, outputsColumnar)
    val applier = applierBuilder.apply(session)
    val out = applier.apply(vanillaPlan, outputsColumnar)
    out
  }
}

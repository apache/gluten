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
package org.apache.gluten.expression

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.DataType

/**
 * A native pass-through call that makes the backend count how many rows reached this point of an
 * expression tree. Delta's transformers emit it in place of `IncrementMetric` when the backend
 * supports native counting. `functionName` is one of the backend's fixed counter slots and is
 * unique to one SQL metric within its projection: the backend keys its per-expression statistics by
 * function name, and the metrics updater credits the metric from that entry. Never evaluated on
 * Spark; it only exists in the expression list handed to the backend.
 */
case class IncrementMetricCall(child: Expression, functionName: String)
  extends UnaryExpression
  with Unevaluable
  with Transformable {

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def prettyName: String = functionName

  override protected def withNewChildInternal(newChild: Expression): IncrementMetricCall =
    copy(child = newChild)

  override def getTransformer(
      childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer =
    GenericExpressionTransformer(functionName, childrenTransformers, this)
}

object IncrementMetricCall {

  /** Prefix of every counter slot name. */
  val functionNamePrefix: String = "increment_metric_"

  /**
   * Number of counter slots the backend registers at startup (`increment_metric_0` to
   * `increment_metric_31`). Mirrors `kIncrementMetricFunctionSlots` in the Velox backend. A
   * projection with more distinct metrics than this falls back.
   */
  val maxCounters: Int = 32

  /** Name of the counter slot `slot`, which must be below [[maxCounters]]. */
  def functionName(slot: Int): String = {
    require(slot >= 0 && slot < maxCounters, s"counter slot $slot out of range")
    functionNamePrefix + slot
  }
}

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
package org.apache.gluten.extension.joinagg

import org.apache.gluten.extension.joinagg.JoinAggregateFunctionWrapper.TargetPhase

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, UnaryNode}

/** An aggregate execution phase introduced after Spark's logical optimization. */
case class JoinAggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan,
    phase: TargetPhase)
  extends UnaryNode {

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  def asAggregate: Aggregate = Aggregate(groupingExpressions, aggregateExpressions, child)

  override protected def withNewChildInternal(newChild: LogicalPlan): JoinAggregate =
    copy(child = newChild)
}

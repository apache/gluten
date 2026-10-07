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
package org.apache.gluten.execution

import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.JavaConverters._

/** The join condition of a broadcast hash join, as needed by the native hash table build. */
object BroadcastHashJoinFilter {

  /**
   * Serializes the join condition as a substrait plan: a filter over an input iterator of the
   * attributes the condition references. Returns the plan and the column names of those attributes,
   * or empty arrays without a condition.
   */
  def serialize(condition: Option[Expression]): (Array[Byte], Array[String]) = condition match {
    case Some(cond) =>
      val inputs = cond.references.toSeq
      val context = new SubstraitContext
      val operatorId = context.nextOperatorId("BroadcastHashJoinFilter")
      val read = RelBuilder.makeReadRelForInputIterator(inputs.asJava, context, operatorId)
      val filter = RelBuilder.makeFilterRel(
        read,
        SubstraitUtil.toSubstraitExpression(cond, inputs, context),
        context,
        operatorId)
      val plan = PlanBuilder.makePlan(context, new java.util.ArrayList(Seq(filter).asJava))
      (plan.toProtobuf.toByteArray, inputs.map(ConverterUtils.genColumnNameWithExprId).toArray)
    case None =>
      (Array.emptyByteArray, Array.empty[String])
  }
}

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
package org.apache.gluten.extension.columnar.enumerated.planner.property

import org.apache.gluten.extension.columnar.enumerated.planner.plan.GroupLeafExec
import org.apache.gluten.extension.columnar.enumerated.planner.property.Conv.{Prop, Req}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq, Transition}
import org.apache.gluten.ras.{GroupLeafBuilder, Property, PropertyDef}
import org.apache.gluten.ras.rule.EnforcerRuleFactory

import org.apache.spark.sql.execution._

sealed trait Conv extends Property[SparkPlan] {
  override def definition(): PropertyDef[SparkPlan, _ <: Property[SparkPlan]] = {
    ConvDef
  }
}

object Conv {
  val any: Conv = Req(ConventionReq.any)

  def of(conv: Convention): Prop = Prop(conv)
  def req(req: ConventionReq): Req = Req(req)

  def get(plan: SparkPlan): Prop = {
    Conv.of(Convention.get(plan))
  }

  def findTransition(from: Prop, to: Req): Transition = {
    val out =
      Transition.factory.findTransitionOrThrow(from.prop, to.req)(new IllegalStateException())
    out
  }

  case class Prop(prop: Convention) extends Conv
  case class Req(req: ConventionReq) extends Conv {
    def isAny: Boolean = {
      req.requiredBatchType == ConventionReq.BatchType.Any &&
      req.requiredRowType == ConventionReq.RowType.Any
    }
  }
}

object ConvDef extends PropertyDef[SparkPlan, Conv] {
  // TODO: Should the convention-transparent ops (e.g., aqe shuffle read) support
  //  convention-propagation. Probably need to refactor getChildrenConstraints.
  override def getProperty(plan: SparkPlan): Conv = {
    conventionOf(plan)
  }

  private def conventionOf(plan: SparkPlan): Conv = {
    val out = Conv.get(plan)
    out
  }

  override def getChildrenConstraints(
      plan: SparkPlan,
      constraint: Property[SparkPlan]): Seq[Conv] = {
    val out = ConventionReq.get(plan).map(Conv.req)
    out
  }

  override def any(): Conv = Conv.any

  override def satisfies(
      property: Property[SparkPlan],
      constraint: Property[SparkPlan]): Boolean = {
    // The following enforces strict type checking against `property` and `constraint`
    // to make sure:
    //
    //  1. `property`, which came from user implementation of PropertyDef.getProperty, must be a
    //     `Prop`
    //  2. `constraint` which came from user implementation of PropertyDef.getChildrenConstraints,
    //     must be a `Req`
    //
    // If the user implementation doesn't follow the criteria, cast error will be thrown.
    //
    // This can be a common practice to implement a safe Property for RAS.
    //
    // TODO: Add a similar case to RAS UTs.
    (property, constraint) match {
      case (prop: Prop, req: Req) =>
        if (req.isAny) {
          return true
        }
        val out = Transition.factory.satisfies(prop.prop, req.req)
        out
    }
  }

  override def assignToGroup(
      group: GroupLeafBuilder[SparkPlan],
      constraint: Property[SparkPlan]): GroupLeafBuilder[SparkPlan] = (group, constraint) match {
    case (builder: GroupLeafExec.Builder, req: Req) =>
      builder.withConvReq(req)
  }
}

case class ConvEnforcerRule() extends EnforcerRuleFactory.SubRule[SparkPlan] {
  override def enforce(node: SparkPlan, constraint: Property[SparkPlan]): Iterable[SparkPlan] = {
    val reqConv = constraint.asInstanceOf[Req]
    val conv = Conv.get(node)
    if (ConvDef.satisfies(conv, reqConv)) {
      return List.empty
    }
    val transition = Conv.findTransition(conv, reqConv)
    val after = transition.apply(node)
    List(after)
  }
}

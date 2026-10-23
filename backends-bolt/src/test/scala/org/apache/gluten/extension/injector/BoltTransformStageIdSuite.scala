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
package org.apache.gluten.extension.injector

import org.apache.gluten.backendsapi.bolt.BoltRuleApi
import org.apache.gluten.execution.WholeStageTransformer
import org.apache.gluten.extension.caller.CallerInfo
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.extension.injector.GlutenInjector.LegacyInjector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GenerateTransformStageId, LeafExecNode, SparkPlan, UnionExec}
import org.apache.spark.sql.internal.SQLConf

import org.mockito.Mockito.{mock, when, RETURNS_DEEP_STUBS}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class BoltTransformStageIdSuite extends AnyFunSuite {
  private def postRules(): Seq[Rule[SparkPlan]] = {
    val conf = new SQLConf
    val session = mock(classOf[SparkSession], RETURNS_DEEP_STUBS)
    when(session.sessionState.conf).thenReturn(conf)
    val injector = new Injector(new SparkSessionExtensions)
    // Exercise the production legacy registration.
    // Unrelated Spark rules require native initialization.
    val register = BoltRuleApi.getClass.getDeclaredMethods
      .find(_.getName.endsWith("injectLegacy")).get
    register.setAccessible(true)
    register.invoke(BoltRuleApi, injector.gluten.legacy)
    val field = classOf[LegacyInjector].getDeclaredFields
      .find(_.getName.endsWith("postBuilders")).get
    field.setAccessible(true)
    val builders = field.get(injector.gluten.legacy)
      .asInstanceOf[mutable.Buffer[ColumnarRuleCall => Rule[SparkPlan]]]
    val call = new ColumnarRuleCall(session, mock(classOf[CallerInfo]), outputsColumnar = false)
    builders.map(_(call)).toVector
  }

  private def injectedStageIdRule(): GenerateTransformStageId = {
    val rules = postRules()
    val stages = rules.collect { case r: GenerateTransformStageId => r }
    assert(stages.size == 1)
    val collapse = rules.indexWhere(_.isInstanceOf[ColumnarCollapseTransformStages])
    assert(collapse >= 0)
    assert(rules.indexWhere(_.isInstanceOf[GenerateTransformStageId]) == collapse + 1)
    stages.head
  }

  private case object EmptyPlan extends LeafExecNode {
    override def output: Seq[Attribute] = Nil
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("This metadata-only fixture is never executed")
  }

  private def stage(child: SparkPlan = EmptyPlan): WholeStageTransformer =
    WholeStageTransformer(child)(-1)

  test("Bolt injects stage IDs after collapse and numbers independent native stages") {
    val rule = injectedStageIdRule()
    val left = stage()
    val right = stage()
    val plan = UnionExec(Seq(left, right))
    assert(rule(plan) eq plan)
    assert(left.stageId == 1)
    assert(right.stageId == 2)
    rule(plan)
    assert(left.stageId == 1)
    assert(right.stageId == 2)
  }

  test("Bolt native stage IDs number nested children before their parent") {
    val rule = injectedStageIdRule()
    val child = stage()
    val parent = stage(child)
    rule(parent)
    assert(child.stageId == 1)
    assert(parent.stageId == 2)
  }
}

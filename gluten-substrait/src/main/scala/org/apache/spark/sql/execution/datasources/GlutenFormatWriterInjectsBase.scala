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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution._
import org.apache.gluten.execution.datasource.GlutenFormatWriterInjects
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}

import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, SparkPlan}

trait GlutenFormatWriterInjectsBase extends GlutenFormatWriterInjects {
  private lazy val transform = HeuristicTransform.static()

  /**
   * FileFormatWriter wraps some Project & Sort on the top of the original output spark plan, we
   * need to replace them with Columnar version
   * @param plan
   *   must be a FakeRowAdaptor
   * @return
   */
  override def getWriterWrappedSparkPlan(plan: SparkPlan): SparkPlan = {
    if (plan.isInstanceOf[ColumnarToCarrierRowExecBase]) {
      // here, the FakeRowAdaptor is simply a R2C converter
      return plan
    }

    val transitionsRemoved = RemoveTransitions.apply(plan)
    // FIXME: HeuristicTransform is costly. Re-applying it may cause performance issues.
    val transformed = transform(transitionsRemoved)

    if (!transformed.isInstanceOf[TransformSupport]) {
      throw new IllegalStateException(
        "Cannot transform the SparkPlans wrapped by FileFormatWriter, " +
          "consider disabling native writer to workaround this issue.")
    }

    def injectAdapter(p: SparkPlan): SparkPlan = p match {
      case p: ProjectExecTransformer => p.mapChildren(injectAdapter)
      case s: SortExecTransformer => s.mapChildren(injectAdapter)
      case _ => ColumnarCollapseTransformStages.wrapInputIteratorTransformer(p)
    }

    // why materializeInput = true? this is for CH backend.
    // in this wst, a Sort will be executed by the underlying native engine.
    // In CH, a SortingTransform cannot handle Const Columns,
    // unless it can get its const-ness from input's header
    // and use const_columns_to_remove to skip const columns.
    // Unfortunately, in our case this wst's input is SourceFromJavaIter
    // and cannot provide const-ness.
    val transformedWithAdapter = injectAdapter(transformed)
    val wst = WholeStageTransformer(transformedWithAdapter, materializeInput = true)(
      ColumnarCollapseTransformStages.getTransformStageCounter(transformed).incrementAndGet())
    val wstWithTransitions = BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToCarrierRow(
      InsertTransitions.create(outputsColumnar = true, wst.batchType()).apply(wst))
    wstWithTransitions
  }
}

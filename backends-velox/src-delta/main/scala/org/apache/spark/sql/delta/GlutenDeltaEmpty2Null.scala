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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ProjectExecTransformer, WholeStageTransformer}
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.catalyst.expressions.{Empty2Null, Expression}
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GenerateTransformStageId, ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * Delta's TransactionalWrite.convertEmptyToNullIfNeeded puts a ProjectExec of Empty2Null on the
 * string partition columns on top of a write's plan when the write has constraints. Delta adds it
 * after Gluten's rules have run, so it stays row-based, and on a Velox plan it converts every row
 * of every column from columnar to row and back. Velox runs Empty2Null (as Spark's `empty2null`),
 * so this runs the projection on the Velox batches when Velox supports it.
 */
private[delta] object GlutenDeltaEmpty2Null {

  /**
   * Returns `empty2NullPlan` as a Velox plan, with Delta's Empty2Null projection on top of it, if
   * any, run by Velox when Velox supports it. `toVeloxPlan` makes a plan columnar.
   */
  def toVeloxPlan(
      empty2NullPlan: SparkPlan,
      conf: SQLConf,
      toVeloxPlan: SparkPlan => SparkPlan): SparkPlan = empty2NullPlan match {
    case ProjectExec(projectList, child) if projectList.exists(_.exists(isEmpty2Null)) =>
      val nativePlan = toVeloxPlan(child) match {
        case wst @ WholeStageTransformer(wholeStageChild, _) =>
          val project = ProjectExecTransformer(projectList, wholeStageChild)
          if (project.doValidate().ok()) Some(wst.withNewChildren(Seq(project))) else None
        case veloxChild if Convention.get(veloxChild).batchType == VeloxBatchType =>
          val project = ProjectExecTransformer(projectList, veloxChild)
          if (project.doValidate().ok()) {
            Some(
              GenerateTransformStageId()(
                ColumnarCollapseTransformStages(new GlutenConfig(conf))(project)))
          } else {
            None
          }
        case _ => None
      }
      nativePlan.getOrElse(toVeloxPlan(empty2NullPlan))
    case _ => toVeloxPlan(empty2NullPlan)
  }

  private def isEmpty2Null(expression: Expression): Boolean = expression.isInstanceOf[Empty2Null]
}

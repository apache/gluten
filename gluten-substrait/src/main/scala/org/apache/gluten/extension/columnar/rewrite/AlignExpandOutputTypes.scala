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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan}
import org.apache.spark.sql.types.DataType

/**
 * Spark 3.3 may produce Expand projections whose expression output types do not exactly match the
 * corresponding Expand output attributes. Spark's row path tolerates this, but native Expand
 * conversion requires each projection column to have a consistent type.
 *
 * This rule rewrites each projection column: null literals are replaced with a typed null matching
 * the output type; non-matching expressions are wrapped in a Cast to the output type.
 */
object AlignExpandOutputTypes extends RewriteSingleNode {
  override def isRewritable(plan: SparkPlan): Boolean = {
    plan match {
      case _: ExpandExec => true
      case _ => false
    }
  }

  override def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case expand: ExpandExec =>
      val alignedProjections = ExpandOutputTypeAlignment.alignProjections(
        expand.projections,
        expand.output,
        expand.child.output)
      if (alignedProjections == expand.projections) {
        expand
      } else {
        val newExpand = expand.copy(projections = alignedProjections)
        newExpand.copyTagsFrom(expand)
        newExpand
      }
    case _ => plan
  }
}

private[gluten] object ExpandOutputTypeAlignment {
  def alignProjections(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      inputAttributes: Seq[Attribute]): Seq[Seq[Expression]] = {
    projections.map {
      projection =>
        projection.zipWithIndex.map {
          case (expression, colIdx) if colIdx < output.length =>
            alignExpression(expression, output(colIdx).dataType)
          case (expression, _) =>
            expression
        }
    }
  }

  private def alignExpression(expression: Expression, outputType: DataType): Expression = {
    expression match {
      case Literal(null, _) =>
        Literal.create(null, outputType)
      case _ if expression.dataType != outputType =>
        Cast(expression, outputType)
      case _ =>
        expression
    }
  }
}

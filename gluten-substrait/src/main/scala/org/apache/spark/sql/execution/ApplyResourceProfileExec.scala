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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Used to apply specified resource profile for the whole stage.
 * @param child
 * @param resourceProfile
 *   resource profile specified for child belong stage.
 */
@Experimental
case class ApplyResourceProfileExec(child: SparkPlan, resourceProfile: ResourceProfile)
  extends UnaryExecNode
  with GlutenPlan {

  override def batchType(): Convention.BatchType = {
    Convention.get(child).batchType
  }

  override def rowType(): Convention.RowType = {
    Convention.get(child).rowType
  }

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning
  }

  override def requiredChildDistribution: scala.Seq[Distribution] = {
    child.requiredChildDistribution
  }

  override def outputOrdering: scala.Seq[SortOrder] = {
    child.outputOrdering
  }

  override def requiredChildOrdering: scala.Seq[scala.Seq[SortOrder]] = {
    child.requiredChildOrdering
  }

  override protected def doExecute(): RDD[InternalRow] = {
    log.info(s"Apply $resourceProfile for plan ${child.nodeName}")
    child.execute.withResources(resourceProfile)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    log.info(s"Apply $resourceProfile for columnar plan ${child.nodeName}")
    child.executeColumnar.withResources(resourceProfile)
  }

  override def output: scala.Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): ApplyResourceProfileExec =
    copy(child = newChild)
}

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

import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.Collections

case class OneRowRelationExecTransformer() extends LeafExecNode with TransformSupport {

  override def output: Seq[Attribute] = Nil

  override def outputPartitioning: Partitioning = SinglePartition

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = Seq.empty

  override def supportsNoInputExecution: Boolean = true

  override def metricsUpdater(): MetricsUpdater = MetricsUpdater.Todo

  override def doCanonicalize(): OneRowRelationExecTransformer = copy()

  override protected def doValidateInternal(): ValidationResult = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(nodeName)
    doNativeValidation(context, makeOneRowRel(context, operatorId))
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val operatorId = context.nextOperatorId(nodeName)
    TransformContext(output, makeOneRowRel(context, operatorId))
  }

  private def makeOneRowRel(context: SubstraitContext, operatorId: Long): RelNode = {
    RelBuilder.makeVirtualTableReadRel(
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.singletonList(Collections.emptyList()),
      context,
      operatorId)
  }
}

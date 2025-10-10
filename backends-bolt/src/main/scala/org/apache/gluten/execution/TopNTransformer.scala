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

import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.SparkPlan

import io.substrait.proto.SortField

import scala.collection.JavaConverters._

case class TopNTransformer(
    limit: Long,
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryTransformSupport {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) AllTuples :: Nil else UnspecifiedDistribution :: Nil

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"TopNTransformer (limit=$limit, " +
      s"orderBy=$orderByString, global=$global, output=$outputString)"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override protected def doValidateInternal(): ValidationResult = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode =
      getRelNode(context, operatorId, limit, sortOrder, child.output, null, validation = true)
    doNativeValidation(context, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode =
      getRelNode(
        context,
        operatorId,
        limit,
        sortOrder,
        child.output,
        childCtx.root,
        validation = false)
    TransformContext(child.output, relNode)
  }

  private def getRelNode(
      context: SubstraitContext,
      operatorId: Long,
      count: Long,
      sortOrder: Seq[SortOrder],
      inputAttributes: Seq[Attribute],
      input: RelNode,
      validation: Boolean): RelNode = {
    val sortFieldList = sortOrder.map {
      order =>
        val builder = SortField.newBuilder()
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
          .doTransform(context)
        builder.setExpr(exprNode.toProtobuf)

        builder.setDirectionValue(SortExecTransformer.transformSortDirection(order))
        builder.build()
    }
    if (!validation) {
      RelBuilder.makeTopNRel(input, count, sortFieldList.asJava, context, operatorId)
    } else {
      RelBuilder.makeTopNRel(
        input,
        count,
        sortFieldList.asJava,
        RelBuilder.createExtensionNode(inputAttributes.asJava),
        context,
        operatorId)
    }
  }

  override def metricsUpdater(): MetricsUpdater = MetricsUpdater.Todo // TODO
}

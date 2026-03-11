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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarShuffleExchangeExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicInteger

// FIXME: The operator is simply a wrapper for sort + limit + project (+ exchange if needed).
//  It's better to remove this wrapper then return the piped sub-operators to query planner
//  directly. Otherwise optimizer may not get enough information to optimize.
case class TakeOrderedAndProjectExecTransformer(
    limit: Long,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    offset: Int = 0)
  extends UnaryExecNode
  with ValidatablePlan {
  override def outputPartitioning: Partitioning = SinglePartition
  override def outputOrdering: Seq[SortOrder] = sortOrder
  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType
  override def rowType0(): Convention.RowType = Convention.RowType.None

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"TakeOrderedAndProjectExecTransformer (limit=$limit, " +
      s"orderBy=$orderByString, output=$outputString)"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (offset != 0) {
      return ValidationResult.failed(s"Native TopK does not support offset: $offset")
    }

    var tagged: ValidationResult = null
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
    if (orderingSatisfies) {
      val limitPlan = LimitExecTransformer(child, offset, limit)
      tagged = limitPlan.doValidate()
    } else {
      // Here we are validating sort + limit which is a kind of whole stage transformer,
      // because we would call sort.doTransform in limit.
      // So, we should add adapter to make it work.
      val inputTransformer =
        ColumnarCollapseTransformStages.wrapInputIteratorTransformer(child)
      val sortPlan = SortExecTransformer(sortOrder, false, inputTransformer)
      val sortValidation = sortPlan.doValidate()
      if (!sortValidation.ok()) {
        return sortValidation
      }
      val limitPlan = LimitExecTransformer(sortPlan, offset, limit)
      tagged = limitPlan.doValidate()
    }

    if (tagged.ok()) {
      val projectPlan = ProjectExecTransformer(projectList, child)
      tagged = projectPlan.doValidate()
    }
    tagged
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // No real computation here
    val childRDD = child.executeColumnar()
    val childRDDPartsNum = childRDD.getNumPartitions

    if (childRDDPartsNum == 0) {
      sparkContext.parallelize(Seq.empty, 1)
    } else {
      val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
      def withLocalSort(sparkPlan: SparkPlan): SparkPlan = {
        if (orderingSatisfies) {
          sparkPlan
        } else {
          SortExecTransformer(sortOrder, false, sparkPlan)
        }
      }

      val hasShuffle = childRDDPartsNum == 1
      val limitBeforeShuffleOffset = if (hasShuffle) {
        // Local limit does not need offset
        0
      } else {
        offset
      }
      // The child should have been replaced by ColumnarCollapseTransformStages.
      val limitBeforeShuffle = child match {
        case wholeStage: WholeStageTransformer =>
          // remove this WholeStageTransformer, put the new sort, limit and project
          // into a new whole stage.
          val localSortPlan = withLocalSort(wholeStage.child)
          LimitExecTransformer(localSortPlan, limitBeforeShuffleOffset, limit)
        case other =>
          // if the child it is not WholeStageTransformer, add the adapter first
          // so that, later we can wrap WholeStageTransformer.
          val localSortPlan = withLocalSort(
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(other))
          LimitExecTransformer(localSortPlan, limitBeforeShuffleOffset, limit)
      }
      val transformStageCounter: AtomicInteger =
        ColumnarCollapseTransformStages.getTransformStageCounter(child)
      val finalLimitPlan = if (hasShuffle) {
        limitBeforeShuffle
      } else {
        val limitStagePlan =
          WholeStageTransformer(limitBeforeShuffle)(transformStageCounter.incrementAndGet())
        val cudfTag = child.getTagValue(CudfTag.CudfTag)
        if (cudfTag.isDefined) {
          limitStagePlan.setTagValue(CudfTag.CudfTag, cudfTag.get)
        }
        val shuffleExec = ShuffleExchangeExec(SinglePartition, limitStagePlan)
        val transformedShuffleExec =
          ColumnarShuffleExchangeExec(shuffleExec, limitStagePlan, shuffleExec.child.output)
        val localSortPlan =
          SortExecTransformer(
            sortOrder,
            false,
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(transformedShuffleExec))
        LimitExecTransformer(localSortPlan, offset, limit)
      }

      val projectPlan = if (projectList != child.output) {
        ProjectExecTransformer(projectList, finalLimitPlan)
      } else {
        finalLimitPlan
      }

      val collapsed =
        BackendsApiManager.getSparkPlanExecApiInstance.maybeCollapseTakeOrderedAndProject(
          projectPlan)

      val finalPlan =
        WholeStageTransformer(collapsed)(transformStageCounter.incrementAndGet())
      finalPlan.copyTagsFrom(child)
      finalPlan.executeColumnar()
    }
  }
}

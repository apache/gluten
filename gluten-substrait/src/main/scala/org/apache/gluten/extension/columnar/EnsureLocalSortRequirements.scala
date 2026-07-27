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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule is similar with `EnsureRequirements` but only handle local `SortExec`.
 *
 * The reason is that, during transform SparkPlan to GlutenPlan, some operators do not need local
 * sort anymore, e.g., convert SortAggregate to HashAggregateTransformer, and we remove local sort
 * eagerly. However, it may break the other operator's requirements, e.g., A SortMergeJoin on top of
 * SortAggregate with the same key. So, this rule adds local sort back if necessary.
 */
object EnsureLocalSortRequirements extends Rule[SparkPlan] {
  private lazy val transform: HeuristicTransform = HeuristicTransform.static()

  private def numStaticPartitionCols(writeFiles: WriteFilesExec): Int = {
    // HadoopFs writes include static partition columns in partitionColumns, while Hive writes may
    // only include the partition columns that are present in the write query.
    val resolver = SQLConf.get.resolver
    val staticPartitionNames = writeFiles.staticPartitions.keys
    writeFiles.partitionColumns.takeWhile {
      partitionColumn => staticPartitionNames.exists(resolver(_, partitionColumn.name))
    }.size
  }

  private def requiredChildOrdering(plan: SparkPlan): Seq[Seq[SortOrder]] = {
    plan match {
      // V1Writes assumes that the logical ordering it prepared is preserved in the physical plan,
      // so WriteFilesExec does not expose requiredChildOrdering itself. Gluten may invalidate that
      // ordering when it replaces a SortAggregateExec with a hash aggregate.
      case writeFiles: WriteFilesExec
          if ColumnarWriteFilesExec.OnNoopLeafPath.unapply(writeFiles).isEmpty =>
        Seq(
          SparkShimLoader.getSparkShims.getV1WriteRequiredOrdering(
            writeFiles.child.output,
            writeFiles.partitionColumns,
            writeFiles.bucketSpec,
            writeFiles.options,
            numStaticPartitionCols(writeFiles)))
      case _ => plan.requiredChildOrdering
    }
  }

  private def addLocalSort(
      plan: SparkPlan,
      originalChild: SparkPlan,
      requiredOrdering: Seq[SortOrder]): SparkPlan = {
    // FIXME: HeuristicTransform is costly. Re-applying it may cause performance issues.
    val newChild = SortExec(requiredOrdering, global = false, child = originalChild)
    (plan, originalChild) match {
      case (_, child: GlutenPlan) if child.supportsColumnar =>
        transform.apply(newChild)
      case (parent: GlutenPlan, _) if parent.supportsColumnar =>
        transform.apply(newChild)
      case _ =>
        newChild
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p =>
        val newChildren = p.children.zip(requiredChildOrdering(p)).map {
          case (child, requiredOrdering) =>
            // If child.outputOrdering already satisfies the requiredOrdering,
            // we do not need to sort.
            if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
              child
            } else {
              addLocalSort(p, child, requiredOrdering)
            }
        }
        p.withNewChildren(newChildren)
    }
  }
}

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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec

object StoragePartitionedJoinShim {

  /**
   * Mirrors the `InnerLike` arm of Spark 4.3 `ShuffledJoin.outputPartitioning`: the members of the
   * collection share one key list and agree on their flags (SPARK-59045), and a spurious
   * unknown-keys marker is cleared when marked and unmarked sides meet (SPARK-59050).
   */
  def innerShuffledJoinOutputPartitioning(partitionings: Seq[Partitioning]): Partitioning =
    PartitioningCollection.fromPartitionings(clearUnknownPartitionKeys(partitionings))

  private def clearUnknownPartitionKeys(partitionings: Seq[Partitioning]): Seq[Partitioning] = {
    val markers = partitionings.flatMap(PartitioningCollection.keyedMarkerOf)
    if (markers.isEmpty || markers.forall(_ == markers.head)) {
      partitionings
    } else {
      partitionings.map {
        case partitioning: Partitioning with Expression
            if PartitioningCollection.keyedMarkerOf(partitioning).contains(true) =>
          partitioning.transform {
            case k: KeyedPartitioning if k.mayContainUnknownPartitionKeys =>
              k.copy(mayContainUnknownPartitionKeys = false)
          }.asInstanceOf[Partitioning]
        case p => p
      }
    }
  }

  /**
   * Since SPARK-60044 a `GroupPartitionsExec` fixes its k-way merge when `EnsureRequirements` plans
   * it, and fails at execution if a node that is not `SafeForKWayMerge` is below it by then.
   * Offloading puts such nodes there, so the merge is dropped and the node coalesces by
   * concatenation.
   */
  def dropUnsafeKWayMerge(plan: SparkPlan): SparkPlan = plan.transformUp {
    case gp: GroupPartitionsExec
        if gp.enableSortedMerge && gp.child.exists(!_.isInstanceOf[SafeForKWayMerge]) =>
      val newGp = gp.copy(sortedMergeOrdering = None)
      newGp.copyTagsFrom(gp)
      newGp
  }
}

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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.{ValidatablePlan, WriteFilesExecTransformer}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.extension.columnar.transition.Convention.RowType
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.vectorized.ColumnarBatch

// The class inherits from "BinaryExecNode" instead of "UnaryExecNode" because
// we need to expose a dummy child (as right child) with type "WriteFilesExec" to let Spark
// choose the new write code path (version >= 3.4). The actual plan to write is the left child
// of this operator.
abstract class ColumnarWriteFilesExec protected (
    override val left: SparkPlan,
    override val right: SparkPlan)
  extends BinaryExecNode
  with ValidatablePlan
  with ColumnarWriteFilesExec.ExecuteWriteCompatible {

  val child: SparkPlan = left

  override lazy val references: AttributeSet = AttributeSet.empty

  override def requiredChildConvention(): Seq[ConventionReq] = {
    val req = ConventionReq.ofBatch(
      ConventionReq.BatchType.Is(BackendsApiManager.getSettings.primaryBatchType))
    Seq.tabulate(2)(
      _ => {
        req
      })
  }

  /**
   * Mark the plan node as outputting both row-based and columnar data. Then we could avoid
   * unnecessary transitions from being added on the exit side of the node.
   *
   * This is feasible based on the assumption that the node doesn't actually involve in either row
   * processing or columnar processing. It's true because Spark only calls `doExecuteWrite` of the
   * object.
   *
   * Since https://github.com/apache/gluten/pull/6745.
   */
  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType
  override def rowType(): RowType = {
    RowType.VanillaRowType
  }

  override def output: Seq[Attribute] = Seq.empty

  override protected def doExecute(): RDD[InternalRow] = {
    throw new GlutenException(s"$nodeName does not implement #doExecute")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new GlutenException(s"$nodeName does not implement #doExecuteColumnar")
  }

  /** Fallback to use vanilla Spark write files to generate an empty file for metadata only. */
  protected def writeFilesForEmptyRDD(
      description: WriteJobDescription,
      committer: FileCommitProtocol,
      jobTrackerID: String): RDD[WriterCommitMessage] = {
    val rddWithNonEmptyPartitions = session.sparkContext.parallelize(Seq.empty[InternalRow], 1)
    rddWithNonEmptyPartitions.mapPartitionsInternal {
      iterator =>
        val sparkStageId = TaskContext.get().stageId()
        val sparkPartitionId = TaskContext.get().partitionId()
        val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue

        val ret = SparkShimLoader.getSparkShims.writeFilesExecuteTask(
          description,
          jobTrackerID,
          sparkStageId,
          sparkPartitionId,
          sparkAttemptNumber,
          committer,
          iterator
        )
        Iterator(ret)
    }
  }

  /** We need this to avoid compiler error. */
  override def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    throw new GlutenException(s"$nodeName does not implement #doExecuteWrite")
  }
}

object ColumnarWriteFilesExec {

  def apply(
      child: WriteFilesExecTransformer,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): ColumnarWriteFilesExec = {
    // This is a workaround for FileFormatWriter#write. Vanilla Spark (version >= 3.4) requires for
    // a plan that has at least one node exactly of type `WriteFilesExec` that is a Scala
    // case-class, to decide to choose new `#executeWrite` code path over the legacy `#execute`
    // for write operation.
    //
    // So we add a no-op `WriteFilesExec` child to let Spark pick the new code path.
    //
    // See: FileFormatWriter#write
    // See: V1Writes#getWriteFilesOpt
    val right: SparkPlan =
      WriteFilesExec(
        NoopLeaf(),
        fileFormat,
        partitionColumns,
        bucketSpec,
        options,
        staticPartitions)
    right.foreach(node => node.setTagValue(NoopTag, true))

    BackendsApiManager.getSparkPlanExecApiInstance.createColumnarWriteFilesExec(
      child,
      right,
      fileFormat,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions)
  }

  private val NoopTag =
    TreeNodeTag[Boolean]("org.apache.spark.sql.execution.ColumnarWriteFilesExec.NoopTag")

  // Decides whether a plan node is on the dummy `WriteFilesExec + NoopLeaf` path.
  object OnNoopLeafPath {
    def unapply(plan: SparkPlan): Option[NoopLeaf] = {
      val leafs = traverseDown(plan)
      if (leafs.size > 1) {
        throw new IllegalArgumentException(s"More than one noop leafs were found in plan: $plan")
      }
      leafs.headOption
    }

    private def traverseDown(plan: SparkPlan): Seq[NoopLeaf] = {
      val hasNoopTag = plan.getTagValue(NoopTag).getOrElse(false)
      if (!hasNoopTag) {
        return Nil
      }
      plan match {
        case leaf: NoopLeaf =>
          Seq(leaf)
        case other =>
          other.children.map(traverseDown).reduce(_ ++ _)
      }
    }
  }

  case class NoopLeaf() extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new GlutenException(s"$nodeName does not support doExecute")
    override def output: Seq[Attribute] = Seq.empty
  }

  sealed trait ExecuteWriteCompatible {
    // To be compatible with Spark (version < 3.4)
    protected def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
      throw new GlutenException("Illegal state: The method is not expected to be called")
    }
  }
}

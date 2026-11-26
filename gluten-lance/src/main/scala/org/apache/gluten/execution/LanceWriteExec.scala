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
import org.apache.gluten.connector.write.ColumnarStreamingDataWriterFactory

import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution
import org.apache.spark.sql.connector.write.{BatchWrite, RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType

import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.lance.spark.utils.BlobUtils
import org.lance.spark.write.{LanceBatchWrite, SparkWrite}

import java.util.{List => JList}

import scala.collection.JavaConverters._

/**
 * A Lance batch write offloaded to a native backend. Executors export their native batches as an
 * Arrow C stream that lance-spark writes into uncommitted fragments; lance-spark's
 * [[LanceBatchWrite]] then commits them on the driver, exactly as for its own row writer. A backend
 * provides the writer factory, and says which columns it cannot write.
 *
 * Implementations take the [[LanceBatchWrite]] as a constructor argument, so the node keeps one
 * batch write through the copies the planner makes of it, and the validation, the writer factory
 * and the commit all use it. `SparkWrite.toBatch` builds a new one on every call, and each opens
 * the table to pin its version, so the argument comes from [[LanceWriteExec.batchWriteOf]], which
 * opens the table once per planned write.
 */
trait LanceWriteExec extends ColumnarV2TableWriteExec {

  protected def lanceBatchWrite: LanceBatchWrite = batchWrite.asInstanceOf[LanceBatchWrite]

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory =
    throw new UnsupportedOperationException("Streaming writes to Lance are not offloaded")

  override def doValidateInternal(): ValidationResult = {
    if (!LanceWriteExec.supportsWrite(write)) {
      return ValidationResult.failed(s"Not a Lance write: ${write.getClass.getName}")
    }
    write match {
      case r: RequiresDistributionAndOrdering
          if !r.requiredDistribution().isInstanceOf[UnspecifiedDistribution] =>
        return ValidationResult.failed("Lance table requires a write distribution (sharded)")
      case _ =>
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    LanceWriteExec.blobColumn(lanceBatchWrite.getCommitSchema) match {
      case Some(name) => return ValidationResult.failed(s"Lance blob column $name is not offloaded")
      case None =>
    }
    unsupportedColumn match {
      case Some(reason) => ValidationResult.failed(reason)
      case None => ValidationResult.succeeded
    }
  }

  /** Why the backend cannot write a column of this write, if it cannot. */
  protected def unsupportedColumn: Option[String]
}

object LanceWriteExec {
  private val BATCH_WRITE_TAG: TreeNodeTag[BatchWrite] =
    TreeNodeTag[BatchWrite]("org.apache.gluten.LanceBatchWriteTag")

  def supportsWrite(write: Write): Boolean = write.isInstanceOf[SparkWrite]

  /**
   * The batch write of `write`, the write of the vanilla node `original`, built on first use and
   * kept on that node. The planner offloads a node twice, first to validate the offloaded node and
   * then to replace the vanilla one, so building it in each offload would open the table twice.
   */
  def batchWriteOf(original: SparkPlan, write: Write): BatchWrite =
    original.getTagValue(BATCH_WRITE_TAG).getOrElse {
      val batchWrite = write.toBatch
      original.setTagValue(BATCH_WRITE_TAG, batchWrite)
      batchWrite
    }

  /**
   * The first blob column, if any. lance-spark's own writer reads blob sources and packs blob
   * files, so blob columns are never offloaded.
   */
  def blobColumn(schema: Schema): Option[String] = blobColumn(schema.getFields, "")

  // Lazy, so the check stops at the first blob column.
  private def blobColumn(fields: JList[Field], prefix: String): Option[String] =
    fields.asScala.iterator
      .map {
        field =>
          val name = prefix + field.getName
          if (isBlob(field)) Some(name) else blobColumn(field.getChildren, s"$name.")
      }
      .collectFirst { case Some(name) => name }

  /** Blob v1 columns carry lance-spark's encoding marker, blob v2 columns an Arrow extension. */
  private def isBlob(field: Field): Boolean =
    BlobUtils.isBlobArrowField(field) || Option(field.getMetadata).exists(
      metadata =>
        BlobUtils.ARROW_EXTENSION_BLOB_V2 == metadata.get(BlobUtils.ARROW_EXTENSION_NAME_KEY))
}

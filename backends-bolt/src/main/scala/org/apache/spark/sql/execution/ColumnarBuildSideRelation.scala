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
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{ColumnarBatchSerializerJniWrapper, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, BindReferences, BoundReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import org.apache.arrow.c.ArrowSchema

import scala.collection.JavaConverters.asScalaIteratorConverter

object ColumnarBuildSideRelation {
  // Keep constructor with BroadcastMode for compatibility
  def apply(
      output: Seq[Attribute],
      batches: Array[Array[Byte]],
      mode: BroadcastMode): ColumnarBuildSideRelation = {
    val boundMode = mode match {
      case HashedRelationBroadcastMode(keys, isNullAware) =>
        // Bind each key to the build-side output so simple cols become BoundReference
        val boundKeys: Seq[Expression] =
          keys.map(k => BindReferences.bindReference(k, AttributeSeq(output)))
        HashedRelationBroadcastMode(boundKeys, isNullAware)
      case m =>
        m // IdentityBroadcastMode, etc.
    }
    new ColumnarBuildSideRelation(output, batches, BroadcastModeUtils.toSafe(boundMode))
  }
}

case class ColumnarBuildSideRelation(
    output: Seq[Attribute],
    batches: Array[Array[Byte]],
    safeBroadcastMode: SafeBroadcastMode)
  extends BuildSideRelation {

  // Rebuild the real BroadcastMode on demand; never serialize it.
  @transient override lazy val mode: BroadcastMode =
    BroadcastModeUtils.fromSafe(safeBroadcastMode, output)

  // If we stored expression bytes, deserialize once and cache locally (not serialized).
  @transient private lazy val exprKeysFromBytes: Option[Seq[Expression]] = safeBroadcastMode match {
    case HashExprSafeBroadcastMode(bytes, _) =>
      Some(BroadcastModeUtils.deserializeExpressions(bytes))
    case _ => None
  }

  private def transformProjection: UnsafeProjection = safeBroadcastMode match {
    case IdentitySafeBroadcastMode =>
      UnsafeProjection.create(output, output)
    case HashSafeBroadcastMode(ords, _) =>
      val bound = ords.map(i => BoundReference(i, output(i).dataType, output(i).nullable))
      UnsafeProjection.create(bound)
    case HashExprSafeBroadcastMode(_, _) =>
      exprKeysFromBytes match {
        case Some(keys) => UnsafeProjection.create(keys)
        case None =>
          throw new IllegalStateException(
            "Failed to deserialize expressions for HashExprSafeBroadcastMode"
          )
      }
  }

  override def deserialized: Iterator[ColumnarBatch] = {
    val runtime =
      Runtimes.contextInstance(BackendsApiManager.getBackendName, "BuildSideRelation#deserialized")
    val jniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializeHandle: Long = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = jniWrapper
        .init(cSchema.memoryAddress())
      cSchema.close()
      handle
    }

    Iterators
      .wrap(new Iterator[ColumnarBatch] {
        var batchId = 0

        override def hasNext: Boolean = {
          batchId < batches.length
        }

        override def next: ColumnarBatch = {
          val handle =
            jniWrapper
              .deserialize(serializeHandle, batches(batchId))
          batchId += 1
          ColumnarBatches.create(handle)
        }
      })
      .protectInvocationFlow()
      .recycleIterator {
        jniWrapper.close(serializeHandle)
      }
      .recyclePayload(ColumnarBatches.forceClose) // FIXME why force close?
      .create()
  }

  override def asReadOnlyCopy(): ColumnarBuildSideRelation = this

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key.
   *
   * NOTE:
   *   - This method was called in Spark Driver, should manage resources carefully.
   *   - The "key" must be already been bound reference.
   */
  override def transform(key: Expression): Array[InternalRow] = TaskResources.runUnsafe {
    val runtime =
      Runtimes.contextInstance(BackendsApiManager.getBackendName, "BuildSideRelation#transform")
    // This transformation happens in Spark driver, thus resources can not be managed automatically.
    val serializerJniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializeHandle = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = serializerJniWrapper.init(cSchema.memoryAddress())
      cSchema.close()
      handle
    }

    var closed = false

    val proj = UnsafeProjection.create(Seq(key))

    // Convert columnar to Row.
    val jniWrapper = NativeColumnarToRowJniWrapper.create(runtime)
    val c2rId = jniWrapper.nativeColumnarToRowInit()
    var batchId = 0
    val iterator = if (batches.length > 0) {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
        override def hasNext: Boolean = {
          val itHasNext = batchId < batches.length
          if (!itHasNext && !closed) {
            jniWrapper.nativeClose(c2rId)
            serializerJniWrapper.close(serializeHandle)
            closed = true
          }
          itHasNext
        }

        override def next(): Iterator[InternalRow] = {
          val batchBytes = batches(batchId)
          batchId += 1
          val batchHandle =
            serializerJniWrapper.deserialize(serializeHandle, batchBytes)
          val batch = ColumnarBatches.create(batchHandle)
          if (batch.numRows == 0) {
            batch.close()
            Iterator.empty
          } else if (output.isEmpty) {
            val rows = ColumnarBatches.emptyRowIterator(batch.numRows()).asScala
            batch.close()
            rows
          } else {
            val cols = batch.numCols()
            val rows = batch.numRows()
            var info: NativeColumnarToRowInfo = null

            new Iterator[InternalRow] {
              var rowId = 0
              var baseLength = 0
              val row = new UnsafeRow(cols)
              var closed = false

              override def hasNext: Boolean = {
                val hasNext = rowId < rows
                if (!hasNext && !closed) {
                  batch.close()
                  closed = true
                }
                hasNext
              }

              override def next: UnsafeRow = {
                if (rowId >= rows) throw new NoSuchElementException
                if (rowId == 0 || rowId == baseLength + info.lengths.length) {
                  baseLength = if (info == null) {
                    baseLength
                  } else {
                    baseLength + info.lengths.length
                  }
                  info = jniWrapper.nativeColumnarToRowConvert(c2rId, batchHandle, rowId)
                }
                val (offset, length) =
                  (info.offsets(rowId - baseLength), info.lengths(rowId - baseLength))
                row.pointTo(null, info.memoryAddress + offset, length.toInt)
                rowId += 1
                row
              }
            }.map(transformProjection).map(proj).map(_.copy())
          }
        }
      }
      res.flatten
    } else {
      Iterator.empty
    }
    iterator.toArray
  }
}

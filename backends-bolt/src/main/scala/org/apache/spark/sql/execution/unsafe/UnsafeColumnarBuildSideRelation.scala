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
package org.apache.spark.sql.execution.unsafe

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{ColumnarBatchSerializerJniWrapper, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, BindReferences, BoundReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{BroadcastModeUtils, HashExprSafeBroadcastMode, HashSafeBroadcastMode, IdentitySafeBroadcastMode, SafeBroadcastMode}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.arrow.c.ArrowSchema

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.JavaConverters.asScalaIteratorConverter

object UnsafeColumnarBuildSideRelation {
  // Keep constructors with BroadcastMode for compatibility
  def apply(
      output: Seq[Attribute],
      batches: UnsafeBytesBufferArray,
      mode: BroadcastMode): UnsafeColumnarBuildSideRelation = {
    val boundMode = mode match {
      case HashedRelationBroadcastMode(keys, isNullAware) =>
        // Bind each key to the build-side output so simple cols become BoundReference
        val boundKeys: Seq[Expression] =
          keys.map(k => BindReferences.bindReference(k, AttributeSeq(output)))
        HashedRelationBroadcastMode(boundKeys, isNullAware)
      case m =>
        m // IdentityBroadcastMode, etc.
    }
    new UnsafeColumnarBuildSideRelation(output, batches, BroadcastModeUtils.toSafe(boundMode))
  }
  def apply(
      output: Seq[Attribute],
      bytesBufferArray: Array[Array[Byte]],
      mode: BroadcastMode): UnsafeColumnarBuildSideRelation = {
    val boundMode = mode match {
      case HashedRelationBroadcastMode(keys, isNullAware) =>
        // Bind each key to the build-side output so simple cols become BoundReference
        val boundKeys: Seq[Expression] =
          keys.map(k => BindReferences.bindReference(k, AttributeSeq(output)))
        HashedRelationBroadcastMode(boundKeys, isNullAware)
      case m =>
        m // IdentityBroadcastMode, etc.
    }
    new UnsafeColumnarBuildSideRelation(
      output,
      bytesBufferArray,
      BroadcastModeUtils.toSafe(boundMode)
    )
  }
}

/**
 * A broadcast relation that is built using off-heap memory. It will avoid the on-heap memory OOM.
 *
 * @param output
 *   output attributes of the relation.
 * @param batches
 *   off-heap memory that stores the broadcast data.
 * @param mode
 *   the broadcast mode.
 */
@Experimental
case class UnsafeColumnarBuildSideRelation(
    private var output: Seq[Attribute],
    private var batches: UnsafeBytesBufferArray,
    var safeBroadcastMode: SafeBroadcastMode)
  extends BuildSideRelation
  with Externalizable
  with Logging
  with KryoSerializable {

  // Rebuild the real BroadcastMode on demand; never serialize it.
  @transient override lazy val mode: BroadcastMode =
    BroadcastModeUtils.fromSafe(safeBroadcastMode, output)

  // If we stored expression bytes, deserialize once and cache locally (not serialized).
  @transient private lazy val exprKeysFromBytes: Option[Seq[Expression]] = safeBroadcastMode match {
    case HashExprSafeBroadcastMode(bytes, _) =>
      Some(BroadcastModeUtils.deserializeExpressions(bytes))
    case _ => None
  }

  /** needed for serialization. */
  def this() = {
    this(null, null.asInstanceOf[UnsafeBytesBufferArray], null)
  }

  def this(
      output: Seq[Attribute],
      bytesBufferArray: Array[Array[Byte]],
      safeMode: SafeBroadcastMode
  ) = {
    this(
      output,
      UnsafeBytesBufferArray(
        bytesBufferArray.length,
        bytesBufferArray.map(_.length),
        bytesBufferArray.map(_.length.toLong).sum
      ),
      safeMode
    )
    val batchesSize = bytesBufferArray.length
    for (i <- 0 until batchesSize) {
      // copy the bytes to off-heap memory.
      batches.putBytesBuffer(i, bytesBufferArray(i))
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeObject(output)
    out.writeObject(safeBroadcastMode)
    out.writeInt(batches.arraySize)
    out.writeObject(batches.bytesBufferLengths)
    out.writeLong(batches.totalBytes)
    for (i <- 0 until batches.arraySize) {
      val bytes = batches.getBytesBuffer(i)
      out.write(bytes)
    }
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    kryo.writeObject(out, output.toList)
    kryo.writeClassAndObject(out, safeBroadcastMode)
    out.writeInt(batches.arraySize)
    kryo.writeObject(out, batches.bytesBufferLengths)
    out.writeLong(batches.totalBytes)
    for (i <- 0 until batches.arraySize) {
      val bytes = batches.getBytesBuffer(i)
      out.write(bytes)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    output = in.readObject().asInstanceOf[Seq[Attribute]]
    safeBroadcastMode = in.readObject().asInstanceOf[SafeBroadcastMode]
    val totalArraySize = in.readInt()
    val bytesBufferLengths = in.readObject().asInstanceOf[Array[Int]]
    val totalBytes = in.readLong()

    // scalastyle:off
    /**
     * We use off-heap memory to reduce on-heap pressure Similar to
     * https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashedRelation.scala#L389-L410
     */
    // scalastyle:on

    batches = UnsafeBytesBufferArray(totalArraySize, bytesBufferLengths, totalBytes)

    for (i <- 0 until totalArraySize) {
      val length = bytesBufferLengths(i)
      val tmpBuffer = new Array[Byte](length)
      in.readFully(tmpBuffer)
      batches.putBytesBuffer(i, tmpBuffer)
    }
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    output = kryo.readObject(in, classOf[List[_]]).asInstanceOf[Seq[Attribute]]
    safeBroadcastMode = kryo.readClassAndObject(in).asInstanceOf[SafeBroadcastMode]
    val totalArraySize = in.readInt()
    val bytesBufferLengths = kryo.readObject(in, classOf[Array[Int]])
    val totalBytes = in.readLong()

    batches = UnsafeBytesBufferArray(totalArraySize, bytesBufferLengths, totalBytes)

    for (i <- 0 until totalArraySize) {
      val length = bytesBufferLengths(i)
      val tmpBuffer = new Array[Byte](length)
      in.read(tmpBuffer)
      batches.putBytesBuffer(i, tmpBuffer)
    }
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
      Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "UnsafeBuildSideRelation#deserialize")
    val jniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializerHandle: Long = {
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
          batchId < batches.arraySize
        }

        override def next: ColumnarBatch = {
          val (offset, length) =
            batches.getBytesBufferOffsetAndLength(batchId)
          batchId += 1
          val handle =
            jniWrapper.deserializeDirect(serializerHandle, offset, length)
          ColumnarBatches.create(handle)
        }
      })
      .protectInvocationFlow()
      .recycleIterator {
        jniWrapper.close(serializerHandle)
      }
      .recyclePayload(ColumnarBatches.forceClose) // FIXME why force close?
      .create()
  }

  override def asReadOnlyCopy(): UnsafeColumnarBuildSideRelation = this

  override def transform(key: Expression): Array[InternalRow] = TaskResources.runUnsafe {
    val runtime =
      Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "UnsafeColumnarBuildSideRelation#transform")
    // This transformation happens in Spark driver, thus resources can not be managed automatically.
    val serializerJniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializerHandle = {
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
    val iterator = if (batches.arraySize > 0) {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
        override def hasNext: Boolean = {
          val itHasNext = batchId < batches.arraySize
          if (!itHasNext && !closed) {
            jniWrapper.nativeClose(c2rId)
            serializerJniWrapper.close(serializerHandle)
            closed = true
          }
          itHasNext
        }

        override def next(): Iterator[InternalRow] = {
          val (offset, length) = batches.getBytesBufferOffsetAndLength(batchId)
          batchId += 1
          val batchHandle =
            serializerJniWrapper.deserializeDirect(serializerHandle, offset, length)
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
                row.pointTo(null, info.memoryAddress + offset, length)
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

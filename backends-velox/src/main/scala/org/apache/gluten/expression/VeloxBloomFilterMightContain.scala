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
package org.apache.gluten.expression

import org.apache.gluten.utils.VeloxBloomFilter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BloomFilterMightContain, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.types.DataType

import io.netty.util.internal.PlatformDependent
import sun.nio.ch.DirectBuffer

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.{Buffer, ByteBuffer}

/**
 * Velox's bloom-filter implementation uses different algorithms internally comparing to vanilla
 * Spark so produces different intermediate aggregate data. Thus we use different filter function /
 * agg function types for Velox's version to distinguish from vanilla Spark's implementation.
 */
case class VeloxBloomFilterMightContain(
    bloomFilterExpression: Expression,
    valueExpression: Expression)
  extends BinaryExpression {
  import VeloxBloomFilterMightContain._

  private val delegate = BloomFilterMightContain(bloomFilterExpression, valueExpression)

  override def prettyName: String = "velox_might_contain"

  override def left: Expression = bloomFilterExpression

  override def right: Expression = valueExpression

  override def nullable: Boolean = delegate.nullable

  override def dataType: DataType = delegate.dataType

  override def checkInputDataTypes(): TypeCheckResult = delegate.checkInputDataTypes()

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): VeloxBloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression, valueExpression = newValueExpression)

  private lazy val bloomFilterData: Array[Byte] =
    bloomFilterExpression.eval().asInstanceOf[Array[Byte]]

  @transient private lazy val bloomFilterBuffer: SerializableDirectByteBuffer = {
    if (bloomFilterData == null) {
      null
    } else {
      new SerializableDirectByteBuffer(
        ByteBuffer
          .allocateDirect(bloomFilterData.length)
          .put(bloomFilterData)
          .rewind())
    }
  }

  override def eval(input: InternalRow): Any = {
    if (bloomFilterBuffer == null) {
      return null
    }
    val value = valueExpression.eval(input)
    if (value == null) {
      return null
    }
    VeloxBloomFilter.mightContainLongOnSerializedBloom(
      bloomFilterBuffer.get(),
      value.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (bloomFilterBuffer == null) {
      return ev.copy(isNull = TrueLiteral, value = JavaCode.defaultLiteral(dataType))
    }
    val bloomBuf = ctx.addReferenceObj(
      "bloomFilterBuffer",
      bloomFilterBuffer
    ) // This field keeps the direct buffer data alive.
    val valueEval = valueExpression.genCode(ctx)
    val code =
      code"""
      ${valueEval.code}
      boolean ${ev.isNull} = ${valueEval.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = ${classOf[VeloxBloomFilter].getName}.mightContainLongOnSerializedBloom(
        (Long) ${classOf[PlatformDependent].getName}.directBufferAddress($bloomBuf.get()),
        (Long)${valueEval.value});
      }"""
    ev.copy(code = code)
  }
}

object VeloxBloomFilterMightContain {

  /**
   * A serializable container for a Java direct byte buffer.
   *
   * Note: Keep this public so generated code can access.
   */
  class SerializableDirectByteBuffer(buffer: Buffer) extends Serializable {
    require(buffer.isInstanceOf[DirectBuffer])
    require(buffer.position() == 0)

    @transient private var byteBuffer: ByteBuffer = buffer.asInstanceOf[ByteBuffer]

    def get(): ByteBuffer = {
      byteBuffer
    }

    @throws[IOException]
    private def writeObject(out: ObjectOutputStream): Unit = {
      val data: Array[Byte] = new Array[Byte](byteBuffer.remaining)
      byteBuffer.duplicate.get(data) // use duplicate to avoid affecting position
      out.writeInt(data.length)
      out.write(data)
    }

    @throws[IOException]
    private def readObject(in: ObjectInputStream): Unit = {
      val length: Int = in.readInt
      val data: Array[Byte] = new Array[Byte](length)
      in.readFully(data)
      byteBuffer = ByteBuffer.allocateDirect(length).put(data)
    }
  }
}

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
package org.apache.gluten.expression.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{HyperLogLogPlusPlus, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.HyperLogLogPlusPlusHelper
import org.apache.spark.sql.types._

// HLL in Bolt's intermediate type is binary, which is different from spark HLL.
// We add this wrapper to align the intermediate type for HLL functions.
case class HLLAdapter(
    child: Expression,
    relativeSDExpr: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[GenericInternalRow]
  with BinaryLike[Expression] {

  def this(child: Expression, relativeSDExpr: Expression) = {
    this(
      child = child,
      relativeSDExpr = relativeSDExpr,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  private lazy val relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSDExpr)

  private lazy val hllppHelper = new HyperLogLogPlusPlusHelper(relativeSD)

  private lazy val aggBufferDataType: Array[DataType] = {
    Seq.tabulate(hllppHelper.numWords)(i => LongType).toArray
  }

  private lazy val projection = UnsafeProjection.create(aggBufferDataType)

  private lazy val row = new UnsafeRow(hllppHelper.numWords)

  override def prettyName: String = "bolt_approx_count_distinct"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def defaultResult: Option[Literal] = Option(Literal.create(0L, dataType))

  override def createAggregationBuffer(): GenericInternalRow = {
    val res = new GenericInternalRow(hllppHelper.numWords)
    for (i <- 0 until hllppHelper.numWords) {
      res.update(i, 0L)
    }
    res
  }

  override def eval(buffer: GenericInternalRow): Any = {
    hllppHelper.query(buffer, 0)
  }

  override def update(buffer: GenericInternalRow, input: InternalRow): GenericInternalRow = {
    val v = child.eval(input)
    if (v != null) {
      hllppHelper.update(buffer, 0, v, child.dataType)
    }
    buffer
  }

  override def merge(buffer: GenericInternalRow, other: GenericInternalRow): GenericInternalRow = {
    hllppHelper.merge(buffer1 = buffer, buffer2 = other, offset1 = 0, offset2 = 0)
    buffer
  }

  override def serialize(obj: GenericInternalRow): Array[Byte] = {
    projection.apply(obj).getBytes()
  }

  override def deserialize(bytes: Array[Byte]): GenericInternalRow = {
    val data = createAggregationBuffer()
    row.pointTo(bytes, bytes.length)
    for (i <- 0 until hllppHelper.numWords) {
      data.update(i, row.getLong(i))
    }
    data
  }

  override def left: Expression = child

  override def right: Expression = relativeSDExpr

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): HLLAdapter =
    this.copy(child = newLeft, relativeSDExpr = newRight)
}

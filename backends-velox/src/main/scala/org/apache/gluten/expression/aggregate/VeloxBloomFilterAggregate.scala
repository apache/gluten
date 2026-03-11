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

import org.apache.gluten.utils.VeloxBloomFilter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.BloomFilter

import java.io.Serializable

/**
 * Velox's bloom-filter implementation uses different algorithms internally comparing to vanilla
 * Spark so produces different intermediate aggregate data. Thus we use different filter function /
 * agg function types for Velox's version to distinguish from vanilla Spark's implementation.
 */
case class VeloxBloomFilterAggregate(
    child: Expression,
    estimatedNumItemsExpression: Expression,
    numBitsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[BloomFilter]
  with TernaryLike[Expression] {

  private val delegate = BloomFilterAggregate(
    child,
    estimatedNumItemsExpression,
    numBitsExpression,
    mutableAggBufferOffset,
    inputAggBufferOffset).asInstanceOf[TypedImperativeAggregate[BloomFilter]]

  override def prettyName: String = "velox_bloom_filter_agg"

  // Mark as lazy so that `estimatedNumItems` is not evaluated during tree transformation.
  private lazy val estimatedNumItems: Long =
    Math.min(
      estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue,
      SQLConf.get
        .getConfString("spark.sql.optimizer.runtime.bloomFilter.maxNumItems", "4000000")
        .toLong
    )

  // Mark as lazy so that `updater` is not evaluated during tree transformation.
  private lazy val updater: BloomFilterUpdater = child.dataType match {
    case LongType => LongUpdater
    case IntegerType => IntUpdater
    case ShortType => ShortUpdater
    case ByteType => ByteUpdater
    case _: StringType => BinaryUpdater
  }

  override def first: Expression = child

  override def second: Expression = estimatedNumItemsExpression

  override def third: Expression = numBitsExpression

  override def checkInputDataTypes(): TypeCheckResult = delegate.checkInputDataTypes()

  override def nullable: Boolean = delegate.nullable

  override def dataType: DataType = delegate.dataType

  override protected def withNewChildrenInternal(
      newChild: Expression,
      newEstimatedNumItemsExpression: Expression,
      newNumBitsExpression: Expression): VeloxBloomFilterAggregate = {
    copy(
      child = newChild,
      estimatedNumItemsExpression = newEstimatedNumItemsExpression,
      numBitsExpression = newNumBitsExpression)
  }

  override def createAggregationBuffer(): BloomFilter = {
    if (!TaskResources.inSparkTask()) {
      throw new UnsupportedOperationException("velox_bloom_filter_agg is not evaluable on Driver")
    }
    VeloxBloomFilter.empty(Math.toIntExact(estimatedNumItems))
  }

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter = {
    assert(buffer.isInstanceOf[VeloxBloomFilter])
    val value = child.eval(input)
    // Ignore null values.
    if (value == null) {
      return buffer
    }
    updater.update(buffer, value)
    buffer
  }

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter = {
    assert(buffer.isInstanceOf[VeloxBloomFilter])
    assert(input.isInstanceOf[VeloxBloomFilter])
    buffer.asInstanceOf[VeloxBloomFilter].mergeInPlace(input)
  }

  override def eval(buffer: BloomFilter): Any = {
    assert(buffer.isInstanceOf[VeloxBloomFilter])
    serialize(buffer)
  }

  override def serialize(buffer: BloomFilter): Array[Byte] = {
    assert(buffer.isInstanceOf[VeloxBloomFilter])
    buffer.asInstanceOf[VeloxBloomFilter].serialize()
  }

  override def deserialize(bytes: Array[Byte]): BloomFilter = {
    VeloxBloomFilter.readFrom(bytes)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): VeloxBloomFilterAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): VeloxBloomFilterAggregate =
    copy(inputAggBufferOffset = newOffset)

}

// see https://github.com/apache/spark/pull/42414
private trait BloomFilterUpdater {
  def update(bf: BloomFilter, v: Any): Boolean
}

private object LongUpdater extends BloomFilterUpdater with Serializable {
  override def update(bf: BloomFilter, v: Any): Boolean =
    bf.putLong(v.asInstanceOf[Long])
}

private object IntUpdater extends BloomFilterUpdater with Serializable {
  override def update(bf: BloomFilter, v: Any): Boolean =
    bf.putLong(v.asInstanceOf[Int])
}

private object ShortUpdater extends BloomFilterUpdater with Serializable {
  override def update(bf: BloomFilter, v: Any): Boolean =
    bf.putLong(v.asInstanceOf[Short])
}

private object ByteUpdater extends BloomFilterUpdater with Serializable {
  override def update(bf: BloomFilter, v: Any): Boolean =
    bf.putLong(v.asInstanceOf[Byte])
}

private object BinaryUpdater extends BloomFilterUpdater with Serializable {
  override def update(bf: BloomFilter, v: Any): Boolean =
    bf.putBinary(v.asInstanceOf[UTF8String].getBytes)
}

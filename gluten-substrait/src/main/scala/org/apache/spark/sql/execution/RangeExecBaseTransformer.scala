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
import org.apache.gluten.execution.ValidatablePlan
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Range => LogicalRange}

/** Base class for [[RangeExec]] transformation that can be implemented by supported backends. */
abstract class ColumnarRangeBaseExec extends LeafExecNode with ValidatablePlan {

  def range: LogicalRange

  val start: Long = range.start
  val end: Long = range.end
  val step: Long = range.step
  val numElements: BigInt = range.numElements
  val numSlices: Int = range.numSlices.getOrElse(session.leafNodeDefaultParallelism)
  val isEmptyRange: Boolean = start == end || (start < end ^ 0 < step)

  override def output: Seq[Attribute] = range.output

  override def simpleString(maxFields: Int): String = {
    s"ColumnarRange $start, $end, $step, $numSlices, $numElements, " +
      s"${output.mkString("[", ", ", "]")}"
  }

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}

/**
 * Companion object for ColumnarRangeBaseExec, provides factory methods to create instance from
 * existing RangeExec plan.
 */
object ColumnarRangeBaseExec {
  def from(rangeExec: RangeExec): ColumnarRangeBaseExec = {
    BackendsApiManager.getSparkPlanExecApiInstance
      .genColumnarRangeExec(rangeExec)
  }
}

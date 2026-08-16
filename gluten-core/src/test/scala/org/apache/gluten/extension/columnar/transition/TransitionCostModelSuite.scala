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
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.extension.columnar.cost.LongCostModel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import org.scalatest.funsuite.AnyFunSuite

class TransitionCostModelSuite extends AnyFunSuite {
  import TransitionCostModelSuite._

  test("costComparator does not overflow on distant node name hash codes") {
    // The tiebreaker used to subtract the two hash codes. "RowToVeloxColumnar" hashes to
    // 2056048280 and "CHColumnarToCarrierRow" to -2037667767, so the true difference is
    // 4093716047, past Int.MaxValue, and the subtraction wrapped to -201251249. That inverted
    // sign hands an equal-cost tie to the wrong path in FloydWarshallGraph#build. 46 of the 110
    // ordered pairs of Gluten transition node names invert this way.
    val comparator = costModel.costComparator()
    val higherHash = costOf(plan => RowToVeloxColumnar(plan))
    val lowerHash = costOf(plan => CHColumnarToCarrierRow(plan))

    assert("RowToVeloxColumnar".hashCode - "CHColumnarToCarrierRow".hashCode < 0)
    assert(comparator.compare(higherHash, lowerHash) > 0)
    assert(comparator.compare(lowerHash, higherHash) < 0)
  }

  test("costComparator prefers the cheaper cost before consulting node names") {
    // Aaaa sorts and hashes below Zzzz, so a comparator that ignored the base cost would order
    // these the other way around.
    val comparator = costModel.costComparator()
    val cheap = costOf(plan => Zzzz(plan))
    val expensive = costOf(plan => Aaaa(Aaaa(plan)))
    assert(comparator.compare(cheap, expensive) < 0)
    assert(comparator.compare(expensive, cheap) > 0)
  }

  test("costComparator treats the same transition as equal to itself") {
    val comparator = costModel.costComparator()
    assert(comparator.compare(costOf(plan => Zzzz(plan)), costOf(plan => Zzzz(plan))) == 0)
  }
}

object TransitionCostModelSuite {
  private def costModel: FloydWarshallGraph.CostModel[Transition] =
    TransitionGraph.asTransitionCostModel(TestCostModel)

  /** The cost of a transition, whose node names are those of the nodes it wraps the input in. */
  private def costOf(f: SparkPlan => SparkPlan): FloydWarshallGraph.Cost =
    costModel.costOf((plan: SparkPlan) => f(plan))

  /** Charges one per node, so a deeper plan costs more. */
  private object TestCostModel extends LongCostModel {
    override def selfLongCostOf(node: SparkPlan): Long = 1L
  }

  /** Node names below are the class names: Spark only strips a trailing "Exec". */
  private case class RowToVeloxColumnar(child: SparkPlan) extends Wrapper
  private case class CHColumnarToCarrierRow(child: SparkPlan) extends Wrapper
  private case class Zzzz(child: SparkPlan) extends Wrapper
  private case class Aaaa(child: SparkPlan) extends Wrapper

  private trait Wrapper extends UnaryExecNode {
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = Nil
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      throw new UnsupportedOperationException()
  }
}

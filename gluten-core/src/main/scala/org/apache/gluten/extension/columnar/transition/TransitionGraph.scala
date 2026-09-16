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

import org.apache.gluten.extension.columnar.cost.{GlutenCost, GlutenCostModel}
import org.apache.gluten.extension.columnar.transition.Convention.BatchType

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.SparkReflectionUtil

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

object TransitionGraph {
  trait Vertex {
    private val initialized: AtomicBoolean = new AtomicBoolean(false)

    final def ensureRegistered(): Unit = {
      if (!initialized.compareAndSet(false, true)) {
        // Already registered.
        return
      }
      register()
    }

    final private def register(): Unit = BatchType.synchronized {
      Transition.factory.update(graph => graph.addVertex(this))
      register0()
    }

    protected[this] def register0(): Unit

    override def toString: String = SparkReflectionUtil.getSimpleClassName(this.getClass)
  }

  type Builder = FloydWarshallGraph.Builder[TransitionGraph.Vertex, Transition]

  private[transition] def builder(): Builder = {
    FloydWarshallGraph.builder()
  }

  private[transition] def asTransitionCostModel(
      base: GlutenCostModel): FloydWarshallGraph.CostModel[Transition] = {
    new TransitionCostModel(base)
  }

  implicit class TransitionGraphOps(val graph: TransitionGraph) {
    import TransitionGraphOps._
    def hasTransition(from: TransitionGraph.Vertex, to: TransitionGraph.Vertex): Boolean = {
      graph.hasPath(from, to)
    }

    def transitionOf(from: TransitionGraph.Vertex, to: TransitionGraph.Vertex): Transition = {
      val path = graph.pathOf(from, to)
      val out = path.edges().reduceOption((l, r) => chain(l, r)).getOrElse(Transition.empty)
      out
    }

    def transitionOfOption(
        from: TransitionGraph.Vertex,
        to: TransitionGraph.Vertex): Option[Transition] = {
      if (!hasTransition(from, to)) {
        return None
      }
      Some(transitionOf(from, to))
    }
  }

  private case class ChainedTransition(first: Transition, second: Transition) extends Transition {
    override def apply0(plan: SparkPlan): SparkPlan = {
      second(first(plan))
    }
  }

  private object TransitionGraphOps {
    private def chain(first: Transition, second: Transition): Transition = {
      if (first.isEmpty && second.isEmpty) {
        return Transition.empty
      }
      ChainedTransition(first, second)
    }
  }

  /** Reuse Gluten cost to represent transition cost. */
  private case class TransitionCost(value: GlutenCost, nodeNames: Seq[String])
    extends FloydWarshallGraph.Cost

  /**
   * The transition cost model relies on the registered Gluten cost model internally to evaluate
   * cost of transitions.
   *
   * Note the transition graph is built once for all subsequent Spark sessions created on the same
   * driver, so any access to Spark dynamic SQL config in Gluten cost model will not take effect for
   * the transition cost evaluation. Hence, it's not recommended to access Spark dynamic
   * configurations in Gluten cost model as well.
   */
  private class TransitionCostModel(base: GlutenCostModel)
    extends FloydWarshallGraph.CostModel[Transition] {

    override def zero(): TransitionCost = TransitionCost(base.makeZeroCost(), Nil)
    override def costOf(transition: Transition): TransitionCost = {
      costOf0(transition)
    }
    override def sum(
        one: FloydWarshallGraph.Cost,
        other: FloydWarshallGraph.Cost): FloydWarshallGraph.Cost = (one, other) match {
      case (TransitionCost(c1, p1), TransitionCost(c2, p2)) =>
        TransitionCost(base.sum(c1, c2), p1 ++ p2)
    }
    override def costComparator(): Ordering[FloydWarshallGraph.Cost] = {
      (x: FloydWarshallGraph.Cost, y: FloydWarshallGraph.Cost) =>
        (x, y) match {
          case (TransitionCost(v1, nodeNames1), TransitionCost(v2, nodeNames2)) =>
            val diff = base.costComparator().compare(v1, v2)
            if (diff != 0) {
              diff
            } else {
              // Break the tie on the node names, so that as long as the names distinguish the two
              // paths the same one wins on every JVM run. Integer.compare rather than a
              // subtraction: hash codes far enough apart overflow Int, and a wrapped sign hands the
              // tie to the other path of the same cost. Two distinct name sequences can still share
              // a hash code, and mkString joins without a separator, so a tie remains possible; the
              // winner then comes down to map iteration order, as it did before.
              Integer.compare(nodeNames1.mkString.hashCode, nodeNames2.mkString.hashCode)
            }
        }
    }

    private def costOf0(transition: Transition): TransitionCost = {
      val leaf = DummySparkPlan()
      val transited = transition.apply(leaf)

      /**
       * The calculation considers C2C's cost as half of C2R / R2C's cost. So query planner prefers
       * C2C than C2R / R2C.
       */
      def costOfPlan(plan: SparkPlan): GlutenCost = base.costOf(plan)
      def nodeNamesOfPlan(plan: SparkPlan): Seq[String] = {
        plan.map(_.nodeName).reverse
      }

      val leafCost = costOfPlan(leaf)
      val accumulatedCost = costOfPlan(transited)
      val costDiff = base.diff(accumulatedCost, leafCost)

      val leafNodeNames = nodeNamesOfPlan(leaf)
      val accumulatedNodeNames = nodeNamesOfPlan(transited)
      require(
        accumulatedNodeNames.startsWith(leafNodeNames),
        s"Transition should only add unary nodes on the input plan or leave it unchanged. " +
          s"Before: $leaf, after: $transited"
      )
      val nodeNamesDiff = mutable.ListBuffer[String]()
      nodeNamesDiff ++= accumulatedNodeNames
      leafNodeNames.foreach(n => assert(nodeNamesDiff.remove(0) == n))
      assert(
        nodeNamesDiff.size == accumulatedNodeNames.size - leafNodeNames.size,
        s"Dummy leaf node not found in the transited plan: $transited")

      TransitionCost(costDiff, nodeNamesDiff.toSeq)
    }
  }
}

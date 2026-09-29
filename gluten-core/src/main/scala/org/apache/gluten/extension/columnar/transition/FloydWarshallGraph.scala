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

import scala.collection.mutable

/**
 * Floyd-Warshall algorithm for finding e.g., cheapest transition between query plan nodes.
 *
 * https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm
 */
trait FloydWarshallGraph[V <: AnyRef, E <: AnyRef] {
  import FloydWarshallGraph._
  def hasPath(from: V, to: V): Boolean
  def pathOf(from: V, to: V): Path[E]
}

object FloydWarshallGraph {
  trait Cost

  trait CostModel[E <: AnyRef] {
    def zero(): Cost
    def sum(one: Cost, other: Cost): Cost
    def costOf(edge: E): Cost
    def costComparator(): Ordering[Cost]
  }

  trait Path[E <: AnyRef] {
    def edges(): Seq[E]
    def cost(costModel: CostModel[E]): Cost
  }

  def builder[V <: AnyRef, E <: AnyRef](): Builder[V, E] = {
    Builder.create()
  }

  private object Path {
    def apply[E <: AnyRef](edges: Seq[E]): Path[E] = Impl(edges)
    private case class Impl[E <: AnyRef](override val edges: Seq[E]) extends Path[E] {
      override def cost(costModel: CostModel[E]): Cost = {
        edges
          .map(costModel.costOf)
          .reduceOption((c1, c2) => costModel.sum(c1, c2))
          .getOrElse(costModel.zero())
      }
    }
  }

  private class Impl[V <: AnyRef, E <: AnyRef](pathTable: Map[V, Map[V, Path[E]]])
    extends FloydWarshallGraph[V, E] {
    override def hasPath(from: V, to: V): Boolean = {
      if (!pathTable.contains(from)) {
        return false
      }
      val vec = pathTable(from)
      if (!vec.contains(to)) {
        return false
      }
      true
    }

    override def pathOf(from: V, to: V): Path[E] = {
      assert(hasPath(from, to))
      val path = pathTable(from)(to)
      path
    }
  }

  trait Builder[V <: AnyRef, E <: AnyRef] {
    def addVertex(v: V): Builder[V, E]
    def addEdge(from: V, to: V, edge: E): Builder[V, E]
    def build(costModel: CostModel[E]): FloydWarshallGraph[V, E]
  }

  private object Builder {
    private class Impl[V <: AnyRef, E <: AnyRef]() extends Builder[V, E] {
      private val pathTable: mutable.Map[V, mutable.Map[V, Path[E]]] = mutable.Map()

      override def addVertex(v: V): Builder[V, E] = {
        assert(!pathTable.contains(v), s"Vertex $v already exists in graph")
        pathTable.getOrElseUpdate(v, mutable.Map()).getOrElseUpdate(v, Path(Nil))
        this
      }

      override def addEdge(from: V, to: V, edge: E): Builder[V, E] = {
        assert(from != to, s"Input vertices $from and $to should be different")
        assert(pathTable.contains(from), s"Vertex $from not exists in graph")
        assert(pathTable.contains(to), s"Vertex $to not exists in graph")
        assert(!hasPath(from, to), s"Path from $from to $to already exists in graph")
        pathTable(from) += to -> Path(Seq(edge))
        this
      }

      override def build(costModel: CostModel[E]): FloydWarshallGraph[V, E] = {
        val vertices = pathTable.keys
        for (k <- vertices) {
          for (i <- vertices) {
            for (j <- vertices) {
              if (hasPath(i, k) && hasPath(k, j)) {
                val pathIk = pathTable(i)(k)
                val pathKj = pathTable(k)(j)
                val newPath = Path(pathIk.edges() ++ pathKj.edges())
                if (!hasPath(i, j)) {
                  pathTable(i) += j -> newPath
                } else {
                  val path = pathTable(i)(j)
                  if (
                    costModel
                      .costComparator()
                      .compare(newPath.cost(costModel), path.cost(costModel)) < 0
                  ) {
                    pathTable(i) += j -> newPath
                  }
                }
              }
            }
          }
        }
        new FloydWarshallGraph.Impl(pathTable.map { case (k, m) => (k, m.toMap) }.toMap)
      }

      private def hasPath(from: V, to: V): Boolean = {
        if (!pathTable.contains(from)) {
          return false
        }
        val vec = pathTable(from)
        if (!vec.contains(to)) {
          return false
        }
        true
      }
    }

    def create[V <: AnyRef, E <: AnyRef](): Builder[V, E] = {
      new Impl()
    }
  }
}

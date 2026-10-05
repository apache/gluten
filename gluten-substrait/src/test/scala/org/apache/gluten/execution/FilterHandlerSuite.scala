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

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression}
import org.apache.spark.sql.types.BooleanType

import org.scalatest.funsuite.AnyFunSuiteLike

class FilterHandlerSuite extends AnyFunSuiteLike {
  private def conditions(n: Int): Seq[Expression] =
    (0 until n).map(i => AttributeReference(s"c$i", BooleanType)())

  private def depth(e: Expression): Int =
    if (e.children.isEmpty) 0 else 1 + e.children.map(depth).max

  // The conditions under an And tree, from left to right.
  private def leaves(e: Expression): Seq[Expression] = e match {
    case And(left, right) => leaves(left) ++ leaves(right)
    case other => Seq(other)
  }

  test("combineConjuncts builds a balanced And tree in the original order") {
    assert(FilterHandler.combineConjuncts(Seq.empty).isEmpty)
    val Seq(a) = conditions(1)
    assert(FilterHandler.combineConjuncts(Seq(a)).contains(a))

    Seq(2, 3, 8, 1000).foreach {
      n =>
        val input = conditions(n)
        val combined = FilterHandler.combineConjuncts(input).get
        assert(leaves(combined) === input)
        // Same order as the left-deep chain that reduceLeftOption(And) built before.
        assert(leaves(combined) === leaves(input.reduceLeft(And)))
        assert(depth(combined) === math.ceil(math.log(n) / math.log(2)).toInt)
    }
  }

  test("combineConjuncts handles many conjuncts") {
    val input = conditions(100000)
    val combined = FilterHandler.combineConjuncts(input).get
    assert(depth(combined) === 17)
    assert(leaves(combined) === input)
  }
}

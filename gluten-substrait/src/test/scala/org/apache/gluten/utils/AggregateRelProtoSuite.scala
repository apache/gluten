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
package org.apache.gluten.utils

import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.rel.RelBuilder

import org.scalatest.funsuite.AnyFunSuite

/**
 * Locks the AggregateRel producer contract after the Substrait 0.98 migration. 0.98 moved the
 * grouping expressions out of the per-grouping `Grouping.grouping_expressions` (field 1) into a
 * rel-level pool `AggregateRel.grouping_expressions` (field 5) that each grouping set references by
 * index through `Grouping.expression_references` (field 2). Gluten only ever emits a single
 * grouping set with a flat list of grouping expressions (GROUPING SETS / CUBE / ROLLUP are expanded
 * into an ExpandRel upstream), so the producer populates the pool in declaration order and has the
 * single grouping reference every entry as `[0, 1, ..., n - 1]`. This suite pins that mapping.
 */
class AggregateRelProtoSuite extends AnyFunSuite {

  test("makeAggregateRel emits a rel-level pool referenced by the single grouping") {
    val context = new SubstraitContext
    val groupings: java.util.List[ExpressionNode] =
      java.util.Arrays.asList[ExpressionNode](
        ExpressionBuilder.makeSelection(0),
        ExpressionBuilder.makeSelection(1))
    val rel = RelBuilder.makeAggregateRel(
      null,
      groupings,
      java.util.Collections.emptyList[AggregateFunctionNode](),
      java.util.Collections.emptyList[ExpressionNode](),
      null,
      context,
      0L)
    val aggRel = rel.toProtobuf.getAggregate

    // The flat grouping list becomes the rel-level pool, in declaration order.
    def poolField(i: Int): Int =
      aggRel.getGroupingExpressions(i).getSelection.getDirectReference.getStructField.getField
    assert(aggRel.getGroupingExpressionsCount === 2)
    assert(poolField(0) === 0)
    assert(poolField(1) === 1)

    // A single grouping references every pool entry by index.
    assert(aggRel.getGroupingsCount === 1)
    assert(aggRel.getGroupings(0).getExpressionReferencesCount === 2)
    assert(aggRel.getGroupings(0).getExpressionReferences(0) === 0)
    assert(aggRel.getGroupings(0).getExpressionReferences(1) === 1)
  }

  test("makeAggregateRel with no grouping keys emits one empty grouping (global aggregation)") {
    val context = new SubstraitContext
    val rel = RelBuilder.makeAggregateRel(
      null,
      java.util.Collections.emptyList[ExpressionNode](),
      java.util.Collections.emptyList[AggregateFunctionNode](),
      java.util.Collections.emptyList[ExpressionNode](),
      null,
      context,
      0L
    )
    val aggRel = rel.toProtobuf.getAggregate

    assert(aggRel.getGroupingExpressionsCount === 0)
    assert(aggRel.getGroupingsCount === 1)
    assert(aggRel.getGroupings(0).getExpressionReferencesCount === 0)
  }
}

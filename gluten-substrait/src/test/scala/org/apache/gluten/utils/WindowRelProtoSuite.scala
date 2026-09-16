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

import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.sql.catalyst.expressions.{Attribute, CurrentRow, UnboundedFollowing, UnboundedPreceding}

import io.substrait.proto.{Expression, SortField}
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Arrays, Collections}

/**
 * Round-trip coverage for the Substrait 0.98 windowing migration (`WindowRel` ->
 * `ConsistentPartitionWindowRel`, `WindowType` -> `BoundsType`, and the
 * `unbounded_preceding`/`unbounded_following` -> single `unbounded` collapse). These assertions pin
 * the silent risks of the migration: the frame-type enum value remap and the unbounded-bound
 * collapse, neither of which a plain compile would catch.
 */
class WindowRelProtoSuite extends AnyFunSuite {

  private def windowFunctionProto(
      upperBound: org.apache.spark.sql.catalyst.expressions.Expression,
      lowerBound: org.apache.spark.sql.catalyst.expressions.Expression,
      frameType: String,
      columnName: String)
      : io.substrait.proto.ConsistentPartitionWindowRel.WindowRelFunction = {
    val windowFunction = ExpressionBuilder.makeWindowFunction(
      Integer.valueOf(0),
      Collections.emptyList[ExpressionNode](),
      columnName,
      TypeBuilder.makeI32(false),
      upperBound,
      lowerBound,
      frameType,
      Collections.emptyList[Attribute]()
    )
    val rel = RelBuilder.makeWindowRel(
      null,
      Arrays.asList(windowFunction),
      Collections.emptyList[ExpressionNode](),
      Collections.emptyList[SortField](),
      new SubstraitContext(),
      0L)
    val proto = rel.toProtobuf
    assert(proto.hasWindow, "Rel oneof should carry a ConsistentPartitionWindowRel")
    val window = proto.getWindow
    assert(window.getWindowFunctionsCount === 1)
    window.getWindowFunctions(0)
  }

  test("ROWS frame maps to BOUNDS_TYPE_ROWS (value remap) and keeps the column_name graft") {
    val fn = windowFunctionProto(CurrentRow, UnboundedPreceding, "ROWS", "rows_col")
    // Fork WindowType.ROWS was 0; 0.98 BoundsType.BOUNDS_TYPE_ROWS is 1 (UNSPECIFIED took slot 0).
    assert(fn.getBoundsType === Expression.WindowFunction.BoundsType.BOUNDS_TYPE_ROWS)
    assert(fn.getBoundsType.getNumber === 1)
    // column_name is a Gluten graft on WindowRelFunction (0.98 has none).
    assert(fn.getColumnName === "rows_col")
    // UNBOUNDED PRECEDING collapses to the single `unbounded` bound; CURRENT ROW is unchanged.
    assert(fn.getLowerBound.hasUnbounded)
    assert(fn.getUpperBound.hasCurrentRow)
  }

  test("RANGE frame maps to BOUNDS_TYPE_RANGE (value remap)") {
    val fn = windowFunctionProto(CurrentRow, UnboundedPreceding, "RANGE", "range_col")
    // Fork WindowType.RANGE was 1; 0.98 BoundsType.BOUNDS_TYPE_RANGE is 2.
    assert(fn.getBoundsType === Expression.WindowFunction.BoundsType.BOUNDS_TYPE_RANGE)
    assert(fn.getBoundsType.getNumber === 2)
  }

  test("UNBOUNDED FOLLOWING collapses to the single unbounded bound") {
    val fn = windowFunctionProto(UnboundedFollowing, UnboundedPreceding, "ROWS", "both_unbounded")
    // Both the lower (UNBOUNDED PRECEDING) and upper (UNBOUNDED FOLLOWING) bounds collapse to
    // `unbounded`; direction is inferred from position by the consumers.
    assert(fn.getLowerBound.hasUnbounded)
    assert(fn.getUpperBound.hasUnbounded)
  }
}

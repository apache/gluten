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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.types.BooleanType

import io.substrait.proto.NestedLoopJoinRel.{JoinType => NLJ}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Locks the Spark -> Substrait `NestedLoopJoinRel.JoinType` mapping that the cartesian /
 * broadcast-nested-loop-join producers rely on. Substrait 0.98 splits the (previously overloaded)
 * `CrossRel` into a pure-cartesian `CrossRel` plus a `NestedLoopJoinRel` whose `JoinType` enum
 * reorders the anti/semi/single values relative to Gluten's old fork. This suite pins the invariant
 * that the reorder is semantically inert for Gluten: the producer only ever emits the enum values
 * whose proto number is identical across the fork and 0.98 (INNER=1, OUTER=2, LEFT=3, LEFT_SEMI=5).
 */
class SubstraitUtilJoinTypeSuite extends AnyFunSuite {

  private def convert(joinType: JoinType): NLJ =
    SubstraitUtil.toNestedLoopJoinSubstrait(joinType)

  private val existsAttr = AttributeReference("exists", BooleanType, nullable = false)()

  test("toNestedLoopJoinSubstrait maps Spark join types to the expected enum constants") {
    assert(convert(Inner) === NLJ.JOIN_TYPE_INNER)
    assert(convert(Cross) === NLJ.JOIN_TYPE_INNER)
    // RightOuter is exchanged to LeftOuter by the producer (build side is always the right side).
    assert(convert(LeftOuter) === NLJ.JOIN_TYPE_LEFT)
    assert(convert(RightOuter) === NLJ.JOIN_TYPE_LEFT)
    assert(convert(LeftSemi) === NLJ.JOIN_TYPE_LEFT_SEMI)
    assert(convert(ExistenceJoin(existsAttr)) === NLJ.JOIN_TYPE_LEFT_SEMI)
    assert(convert(FullOuter) === NLJ.JOIN_TYPE_OUTER)
  }

  test("unsupported join types fall back to UNRECOGNIZED") {
    // LeftAnti is not supported by the nested-loop-join producers; it must not silently map to a
    // valid enum value (which would produce a wrong plan instead of a Spark fallback).
    assert(convert(LeftAnti) === NLJ.UNRECOGNIZED)
  }

  test("emitted enum values keep their stable 0.98 proto numbers") {
    // These four values are identical between Gluten's old CrossRel fork and 0.98's
    // NestedLoopJoinRel, so re-pointing the producer at the new message is semantically neutral.
    assert(NLJ.JOIN_TYPE_INNER.getNumber === 1)
    assert(NLJ.JOIN_TYPE_OUTER.getNumber === 2)
    assert(NLJ.JOIN_TYPE_LEFT.getNumber === 3)
    assert(NLJ.JOIN_TYPE_LEFT_SEMI.getNumber === 5)
  }

  test("ordinal equals proto number for every declared JoinType value") {
    // StorageJoinBuilder passes toNestedLoopJoinSubstrait(...).ordinal() as a raw int over JNI and
    // the native side casts it back by proto number. That is only correct while the enum is
    // declared densely (0..N with no gaps). Lock it so a future reorder that introduces a gap
    // fails here rather than silently corrupting the join type on the native side.
    NLJ
      .values()
      .filter(_ != NLJ.UNRECOGNIZED)
      .foreach(v => assert(v.ordinal() === v.getNumber, s"ordinal/number mismatch for $v"))
  }
}

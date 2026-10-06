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
import org.apache.gluten.substrait.rel.RelBuilder

import org.scalatest.funsuite.AnyFunSuite

/**
 * Locks the FetchRel producer contract after the Substrait 0.98 migration. 0.98 removed the scalar
 * `int64 offset = 3` / `int64 count = 4` fields in favor of `Expression offset_expr = 5` /
 * `Expression count_expr = 6`. Gluten's only FetchRel producer feeds literal `Long`s (from Spark
 * Limit/Offset), so the producer now wraps each into an i64-literal `Expression`. This suite pins
 * that the values land in the new expression carriers as i64 literals.
 */
class FetchRelProtoSuite extends AnyFunSuite {

  test("makeFetchRel emits offset/count as i64 literal expressions") {
    val context = new SubstraitContext
    val rel = RelBuilder.makeFetchRel(null, 5L, 10L, context, 0L)
    val fetchRel = rel.toProtobuf.getFetch

    assert(fetchRel.hasOffsetExpr)
    assert(fetchRel.hasCountExpr)
    assert(fetchRel.getOffsetExpr.getLiteral.getI64 === 5L)
    assert(fetchRel.getCountExpr.getLiteral.getI64 === 10L)
  }
}

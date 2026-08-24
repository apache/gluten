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
package org.apache.gluten.substrait.rel

import org.apache.gluten.substrait.SubstraitContext

import io.substrait.proto.{FetchMode, SortField, TopNRel}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

/**
 * Locks the TopNRel producer contract after adopting upstream Substrait's `TopNRel`. Gluten's
 * vendored copy previously carried a local `int64 n = 3` field with `sorts = 4`. Upstream instead
 * models the limit as `Expression count = 5`, alongside `Expression offset = 4` and
 * `FetchMode mode = 6`, and puts `sorts` on tag 3. Gluten's only TopNRel producer (Spark
 * `TakeOrderedAndProject`) supplies a literal `Long` limit with no offset and no ties, so the
 * producer wraps the limit into an i64-literal `Expression`, sets `FETCH_MODE_ROWS_ONLY` explicitly
 * (`FETCH_MODE_UNSPECIFIED` is merely the proto3 default and not a valid producer choice), and
 * leaves `offset` unset. These assertions pin that contract, none of which a plain compile catches.
 */
class TopNRelProtoSuite extends AnyFunSuite {

  test("makeTopNRel emits count as an i64 literal expression with ROWS_ONLY mode and no offset") {
    val context = new SubstraitContext
    val sorts = Collections.singletonList(SortField.getDefaultInstance)
    val rel = RelBuilder.makeTopNRel(null, 10L, sorts, context, 0L)
    val topNRel = rel.toProtobuf.getTopN

    // The limit travels as an i64-literal `Expression`, not a scalar field.
    assert(topNRel.hasCount)
    assert(topNRel.getCount.getLiteral.getI64 === 10L)
    // Fetch mode must be set explicitly rather than left at the proto3 default.
    assert(topNRel.getMode === FetchMode.FETCH_MODE_ROWS_ONLY)
    // Gluten never emits an offset (an unset offset is treated as 0).
    assert(!topNRel.hasOffset)
    assert(topNRel.getSortsCount === 1)
    // RelCommon carries the direct output mapping, as it does for every other rel node.
    assert(topNRel.hasCommon)
    assert(topNRel.getCommon.hasDirect)
  }

  test("TopNRel field numbers match upstream Substrait") {
    // A round trip through the generated class cannot detect a renumber, because producer and
    // consumer share one schema. Assert on the descriptor so the wire tags are actually pinned:
    // the native backend and any other Substrait consumer decode by field number, not by name.
    val descriptor = TopNRel.getDescriptor
    assert(descriptor.findFieldByName("sorts").getNumber === 3)
    assert(descriptor.findFieldByName("offset").getNumber === 4)
    assert(descriptor.findFieldByName("count").getNumber === 5)
    assert(descriptor.findFieldByName("mode").getNumber === 6)
    assert(descriptor.findFieldByName("advanced_extension").getNumber === 7)
    // The Gluten-local `int64 n` field is gone; tag 3 now carries `sorts`.
    assert(descriptor.findFieldByName("n") === null)
  }
}

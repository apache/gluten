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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.bolt.BoltSparkPlanExecApi

import org.apache.spark.sql.catalyst.expressions.{Add, BoundReference, Literal, Subtract}
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

class BoltBroadcastCanonicalizationSuite extends AnyFunSuite {
  private val api = new BoltSparkPlanExecApi
  private val rank = BoundReference(0, IntegerType, nullable = false)

  test("broadcast exchange canonicalization distinguishes opposite expression keys") {
    val plus = HashedRelationBroadcastMode(Seq(Add(rank, Literal(1))))
    val minus = HashedRelationBroadcastMode(Seq(Subtract(rank, Literal(1))))
    assert(api.doCanonicalizeForBroadcastMode(plus) != api.doCanonicalizeForBroadcastMode(minus))
    assert(api.doCanonicalizeForBroadcastMode(plus) == plus.canonicalized)
    assert(api.doCanonicalizeForBroadcastMode(minus) == minus.canonicalized)
  }

  test("broadcast exchange canonicalization retains reuse for equivalent expression keys") {
    val first = HashedRelationBroadcastMode(Seq(Add(rank, Literal(1))))
    val equivalent = HashedRelationBroadcastMode(Seq(Add(Literal(1), rank)))
    assert(first.canonicalized == equivalent.canonicalized)
    assert(api.doCanonicalizeForBroadcastMode(first) ==
      api.doCanonicalizeForBroadcastMode(equivalent))
  }

  test("broadcast exchange canonicalization preserves null-aware and identity modes") {
    val regular = HashedRelationBroadcastMode(Seq(rank), isNullAware = false)
    val nullAware = HashedRelationBroadcastMode(Seq(rank), isNullAware = true)
    assert(api.doCanonicalizeForBroadcastMode(regular) !=
      api.doCanonicalizeForBroadcastMode(nullAware))
    assert(api.doCanonicalizeForBroadcastMode(IdentityBroadcastMode) == IdentityBroadcastMode)
  }
}

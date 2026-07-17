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
package org.apache.spark.sql

import org.apache.spark.SparkFunSuite

/**
 * Tests for the ExprId normalization used by the Gluten plan stability suites, in particular that
 * string constants containing an ExprId-like fragment (e.g. Brand#12 in TPCH q19, or
 * "scholaramalgamalg #14" in TPCDS q53) are not treated as ExprIds. See GLUTEN-12375.
 */
class GlutenNormalizeIdsSuite extends SparkFunSuite {
  import GlutenPlanStabilityTestTrait._

  test("ExprIds are normalized in encounter order") {
    val plan = "Project [l_partkey#785, l_quantity#789L AS qty#801, l_partkey#785]"
    assert(glutenNormalizeIds(plan) === "Project [l_partkey#1, l_quantity#2 AS qty#3, l_partkey#1]")
  }

  test("plan ids and _pre_ names are normalized independently of ExprIds") {
    val plan = "Exchange hashpartitioning(a#100, 200), [plan_id=1889]\n" +
      "ReusedExchange [id=#1889]\n" +
      "Project [split(c#101, ,, -1) AS _pre_7#102]"
    val expected = "Exchange hashpartitioning(a#1, 200), [plan_id=1]\n" +
      "ReusedExchange [id=#2]\n" +
      "Project [split(c#2, ,, -1) AS _pre_1#3]"
    assert(glutenNormalizeIds(plan) === expected)
  }

  test("getHashLiterals extracts distinct hash-containing literals, longest first") {
    val query = "select * from t where a = 'Brand#1' or a = 'Brand#12' " +
      "or a = 'Brand#1' or b = 'no hash here' or c = 'scholaramalgamalg #14'"
    assert(getHashLiterals(query) === Seq("scholaramalgamalg #14", "Brand#12", "Brand#1"))
  }

  test("string literals with ExprId-like fragments are not normalized") {
    val query = "select * from part where p_brand = 'Brand#12'"
    val literals = getHashLiterals(query)
    assert(literals === Seq("Brand#12"))

    val plan = "Filter (p_brand#785 = Brand#12)"
    assert(glutenNormalizeIds(plan, literals) === "Filter (p_brand#1 = Brand#12)")
  }

  test("literals padded by CHAR-type columns keep their value") {
    // CHAR(50) columns pad literals with trailing spaces in explain output; the literal from the
    // query text is still a prefix of the padded value.
    val query = "select * from item where i_brand = 'scholaramalgamalg #14'"
    val plan = "In(i_brand, [exportiunivamalg #9   ,scholaramalgamalg #14  ]), i_brand#42"
    val normalized = glutenNormalizeIds(plan, getHashLiterals(query))
    assert(
      normalized === "In(i_brand, [exportiunivamalg #1   ,scholaramalgamalg #14  ]), i_brand#2")
  }

  test("normalization is stable across different ExprId allocations") {
    // The same plan printed in two JVM sessions: one where the ExprId counter is small enough to
    // collide with the literal Brand#12 (an isolated suite run), and one where it is not (the
    // whole module running in a single JVM).
    val query = "select * from part where p_brand = 'Brand#12'"
    val literals = getHashLiterals(query)
    def plan(brandId: Int, sizeId: Int): String =
      s"Filter ((p_brand#$brandId = Brand#12) AND (p_size#$sizeId > 1))"

    // Without protection, the literal token "#12" merges with the ExprId of p_brand#12 and the
    // two sessions normalize differently.
    assert(glutenNormalizeIds(plan(12, 13)) !== glutenNormalizeIds(plan(785, 786)))

    val isolated = glutenNormalizeIds(plan(12, 13), literals)
    val sharedJvm = glutenNormalizeIds(plan(785, 786), literals)
    assert(isolated === sharedJvm)
    assert(isolated === "Filter ((p_brand#1 = Brand#12) AND (p_size#2 > 1))")
  }
}

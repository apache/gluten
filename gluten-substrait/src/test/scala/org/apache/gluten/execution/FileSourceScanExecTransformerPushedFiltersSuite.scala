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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.sources.{EqualTo, In}

import org.scalatest.funsuite.AnyFunSuite

class FileSourceScanExecTransformerPushedFiltersSuite extends AnyFunSuite {

  import FileSourceScanExecTransformerBase._

  test("renderPushedFilters marks every handled filter") {
    assert(
      renderPushedFilters(
        Seq("IsNotNull(id)", "LessThan(id,5)"),
        markAsHandled = true) ===
        "[*IsNotNull(id), *LessThan(id,5)]")
  }

  test("renderPushedFilters leaves unhandled filters unmarked") {
    assert(
      renderPushedFilters(
        Seq("IsNotNull(id)", "LessThan(id,5)"),
        markAsHandled = false) ===
        "[IsNotNull(id), LessThan(id,5)]")
  }

  test("renderPushedFilters treats each filter string as opaque") {
    val filters = Seq(
      In("id", Array[Any](1, 2, 3)),
      EqualTo("closeParen", ")"),
      EqualTo("closeBracket", "]"),
      EqualTo("comma", "a, b")).map(_.toString)
    assert(
      renderPushedFilters(filters, markAsHandled = true) ===
        "[*In(id, [1,2,3]), *EqualTo(closeParen,)), *EqualTo(closeBracket,]), " +
        "*EqualTo(comma,a, b)]")
  }

  test("renderPushedFilters renders an empty sequence") {
    assert(renderPushedFilters(Seq.empty, markAsHandled = true) === "[]")
  }

  test("allScanFiltersHandled rejects any unsupported scan filter") {
    val filters = Seq(Literal(1), Literal(2))
    assert(allScanFiltersHandled(Seq.empty, _ => false))
    assert(allScanFiltersHandled(filters, _ => true))
    assert(!allScanFiltersHandled(filters, _ != filters.last))
  }
}

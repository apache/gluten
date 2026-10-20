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
package org.apache.spark.sql.delta.constraints

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.delta.constraints.Constraints.{Check, NotNull}
import org.apache.spark.sql.delta.schema.DeltaInvariantViolationException
import org.apache.spark.sql.types.LongType

/** Checks of GlutenDeltaInvariantChecker that need no native library: the plan and row paths. */
class GlutenDeltaInvariantCheckerSuite extends SparkFunSuite {

  private val output = Seq("a", "b", "c").map(AttributeReference(_, LongType)())

  test("only top-level NOT NULL constraints on output columns are checked natively") {
    assert(GlutenDeltaInvariantChecker.create(output, Nil).isEmpty)
    assert(GlutenDeltaInvariantChecker.create(output, Seq(NotNull(Seq("a")))).isDefined)
    // Delta resolves column names case-insensitively.
    assert(GlutenDeltaInvariantChecker.create(output, Seq(NotNull(Seq("B")))).isDefined)

    // Any other constraint keeps Delta's DeltaInvariantCheckerExec for the whole write.
    val check = Check("positive", Literal(true))
    assert(GlutenDeltaInvariantChecker.create(output, Seq(NotNull(Seq("a")), check)).isEmpty)
    assert(GlutenDeltaInvariantChecker.create(output, Seq(NotNull(Seq("a", "x")))).isEmpty)
    assert(GlutenDeltaInvariantChecker.create(output, Seq(NotNull(Seq("missing")))).isEmpty)
  }

  test("row path checks every constraint") {
    val checker =
      GlutenDeltaInvariantChecker.create(output, Seq(NotNull(Seq("a")), NotNull(Seq("c")))).get
    val valid = Seq(InternalRow(1L, null, 3L), InternalRow(4L, 5L, 6L))
    assert(checker.wrap(valid.iterator).toSeq == valid)

    val e = intercept[DeltaInvariantViolationException] {
      checker.wrap(Iterator(InternalRow(1L, 2L, 3L), InternalRow(4L, 5L, null))).toList
    }
    assert(e.getMessage.contains("NOT NULL constraint violated for column: c"))
  }
}

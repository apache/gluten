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

import org.apache.gluten.exception.GlutenException

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.delta.constraints.Constraints.{Check, NotNull}
import org.apache.spark.sql.delta.schema.DeltaInvariantViolationException
import org.apache.spark.sql.types.LongType

import org.apache.commons.lang3.exception.ExceptionUtils

/** Checks of GlutenDeltaInvariantChecker that need no native library. */
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

  test("finds a NOT NULL violation that native code turned into text") {
    // The message quotes this column name as `a.b`.
    val violation = DeltaInvariantViolationException(NotNull(Seq("a.b")))
    // Native code keeps the text cpp/core/jni/JniCommon.h builds from the Java exception.
    val taskFailure = new SparkException(
      "[TASK_WRITE_FAILED] Task failed while writing rows",
      new GlutenException(
        "Operator::getOutput failed: Error during calling Java code from native code: " +
          ExceptionUtils.getStackTrace(violation))
    )
    // The driver gets the task's exception as the cause of the job's failure, and its stack
    // trace in the message, which is all it has when it cannot deserialize the exception.
    val withCause = new SparkException("Job aborted due to stage failure", taskFailure)
    val textOnly = new SparkException(
      "Job aborted due to stage failure: " + ExceptionUtils.getStackTrace(taskFailure))

    for (failure <- Seq(withCause, textOnly)) {
      val found = GlutenDeltaInvariantChecker.findViolation(failure).get
      assert(found.getMessage == violation.getMessage)
      assert(found.getSuppressed.toSeq == Seq(failure))
    }
    val otherFailure = new SparkException("Job aborted", new GlutenException("Out of memory"))
    assert(GlutenDeltaInvariantChecker.findViolation(otherFailure).isEmpty)
  }
}

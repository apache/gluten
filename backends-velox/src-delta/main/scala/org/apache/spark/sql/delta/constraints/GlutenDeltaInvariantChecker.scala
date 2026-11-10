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

import org.apache.gluten.columnarbatch.VeloxColumnarBatches

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, SchemaUtils}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.JavaConverters._

/**
 * Checks top-level NOT NULL constraints on Velox batches, for [[GlutenDeltaInvariantCheckerExec]].
 * Nulls are found natively, but a violation is thrown on the JVM as Delta's own
 * [[DeltaInvariantViolationException]]: an exception raised inside Velox would reach Spark only as
 * a GlutenException.
 *
 * @param columnOrdinals
 *   the ordinal in the checked plan's output of the column each constraint checks
 * @param constraints
 *   the constraints, parallel to `columnOrdinals`
 */
private[delta] class GlutenDeltaInvariantChecker private (
    columnOrdinals: Array[Int],
    constraints: Array[NotNull])
  extends Serializable {

  def check(batch: ColumnarBatch): Unit = {
    val failedConstraintIndex = VeloxColumnarBatches.firstNullColumnIndex(batch, columnOrdinals)
    if (failedConstraintIndex >= 0) {
      throw DeltaInvariantViolationException(constraints(failedConstraintIndex))
    }
  }

  /**
   * Returns the violation this checker threw, if it caused `failure`, the failure of the write.
   *
   * Native code that pulls the checked batches, such as the Velox writer's sort on a partitioned
   * write, keeps only the text of a Java exception. The write then fails with a GlutenException
   * whose message holds the violation's text, which is what this looks for. The returned violation
   * carries `failure` as a suppressed exception.
   *
   * TODO: Remove once native code keeps Java exceptions
   * (https://github.com/apache/gluten/pull/13561).
   */
  def findViolation(failure: Throwable): Option[DeltaInvariantViolationException] = {
    val messages =
      ExceptionUtils.getThrowableList(failure).asScala.flatMap(t => Option(t.getMessage))
    constraints.iterator
      .map(DeltaInvariantViolationException(_))
      .find(violation => messages.exists(_.contains(violation.toString)))
      .map {
        violation =>
          violation.addSuppressed(failure)
          violation
      }
  }

  override def toString: String = constraints.map(_.column.head).mkString("NOT NULL [", ", ", "]")
}

private[delta] object GlutenDeltaInvariantChecker {

  /**
   * Returns a checker for `constraints` if all of them are top-level NOT NULL constraints on
   * columns of `output`, or None if any of them needs Delta's row-based DeltaInvariantCheckerExec.
   */
  def create(
      output: Seq[Attribute],
      constraints: Seq[Constraint]): Option[GlutenDeltaInvariantChecker] = {
    if (constraints.isEmpty) {
      return None
    }

    val topLevelNotNullConstraints = constraints.collect {
      case constraint: NotNull if constraint.column.length == 1 => constraint
    }
    if (topLevelNotNullConstraints.size != constraints.size) {
      return None
    }

    val columnOrdinals = topLevelNotNullConstraints.map {
      constraint =>
        output.indexWhere {
          attribute => SchemaUtils.DELTA_COL_RESOLVER(attribute.name, constraint.column.head)
        }
    }
    if (columnOrdinals.contains(-1)) {
      return None
    }
    Some(new GlutenDeltaInvariantChecker(
      columnOrdinals.toArray,
      topLevelNotNullConstraints.toArray))
  }
}

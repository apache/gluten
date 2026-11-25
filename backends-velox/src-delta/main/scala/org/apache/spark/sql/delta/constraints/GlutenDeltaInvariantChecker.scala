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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.delta.constraints.Constraints.{Check, NotNull}
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, SchemaUtils}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.JavaConverters._

/**
 * Checks Delta constraints on Velox batches, for [[GlutenDeltaInvariantCheckerExec]]. A violation
 * is thrown on the JVM as Delta's own exception: an exception raised inside Velox would reach Spark
 * only as a GlutenException.
 */
abstract private[delta] class GlutenDeltaInvariantChecker extends Serializable {

  /** Checks `batch`, and throws the violation of the first row that breaks a constraint. */
  def check(batch: ColumnarBatch): Unit
}

/**
 * Checks top-level NOT NULL constraints. Nulls are found natively.
 *
 * @param columnOrdinals
 *   the ordinal in the checked plan's output of the column each constraint checks
 * @param constraints
 *   the constraints, parallel to `columnOrdinals`
 */
private[delta] class GlutenDeltaNotNullChecker(
    columnOrdinals: Array[Int],
    constraints: Array[NotNull])
  extends GlutenDeltaInvariantChecker {

  override def check(batch: ColumnarBatch): Unit = {
    val failedConstraintIndex = VeloxColumnarBatches.firstNullColumnIndex(batch, columnOrdinals)
    if (failedConstraintIndex >= 0) {
      throw DeltaInvariantViolationException(constraints(failedConstraintIndex))
    }
  }

  override def toString: String = constraints.map(_.column.head).mkString("NOT NULL [", ", ", "]")
}

/**
 * Checks constraints of any kind, such as CHECK constraints, generated columns and nested NOT NULL
 * constraints, with Delta's own checks. Only the columns that the checks reference are exported
 * from the Velox batch to the JVM, and the checks run on those rows as in Delta's
 * DeltaInvariantCheckerExec, so they raise the same errors.
 *
 * @param columnOrdinals
 *   the ordinals in the checked plan's output of the columns the checks reference
 * @param boundChecks
 *   Delta's CheckDeltaInvariant expressions, bound to the columns at `columnOrdinals`
 */
private[delta] class GlutenDeltaReferencedColumnsChecker(
    columnOrdinals: Array[Int],
    boundChecks: Seq[Expression])
  extends GlutenDeltaInvariantChecker {

  // Created once per deserialized checker, that is, once per task.
  @transient private lazy val assertions = UnsafeProjection.create(boundChecks)

  // The message of a CHECK violation prints the values of the columns the CHECK reads. A struct,
  // array or map value prints as its object, which DeltaInvariantCheckerExec reads from an
  // UnsafeRow. Such checks run on unsafe copies of the rows, so that the message is the same.
  private val printsComplexValues: Boolean = boundChecks.exists {
    case check: CheckDeltaInvariant =>
      check.constraint.isInstanceOf[Check] &&
      check.columnExtractors.toSeq.exists { case (_, extractor) => isComplex(extractor.dataType) }
    case _ => false
  }

  @transient private var unsafeCopy: UnsafeProjection = _

  override def check(batch: ColumnarBatch): Unit = {
    val numRows = batch.numRows()
    if (numRows == 0) {
      return
    }
    if (columnOrdinals.isEmpty) {
      // The checks read no column, e.g. `CHECK (current_date() > '2000-01-01')`.
      for (_ <- 0 until numRows) assertions(InternalRow.empty)
      return
    }
    val selected =
      ColumnarBatches.select(BackendsApiManager.getBackendName, batch, columnOrdinals)
    if (ColumnarBatches.isLightBatch(selected)) {
      // `load` closes the selected batch, and the loaded one is closed here.
      val loaded = ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), selected)
      try {
        assertRows(loaded)
      } finally {
        loaded.close()
      }
    } else {
      // A batch on the JVM: the selected batch shares its column vectors.
      assertRows(selected)
    }
  }

  private def assertRows(batch: ColumnarBatch): Unit = {
    if (printsComplexValues && unsafeCopy == null) {
      unsafeCopy =
        UnsafeProjection.create((0 until batch.numCols()).map(batch.column(_).dataType).toArray)
    }
    var rowId = 0
    while (rowId < batch.numRows()) {
      val row = batch.getRow(rowId)
      assertions(if (printsComplexValues) unsafeCopy(row) else row)
      rowId += 1
    }
  }

  private def isComplex(dataType: DataType): Boolean = dataType match {
    case _: StructType | _: ArrayType | _: MapType => true
    case _ => false
  }

  override def toString: String = boundChecks.mkString("[", ", ", "]")
}

private[delta] object GlutenDeltaInvariantChecker {

  /**
   * Returns a native checker for `constraints` if all of them are top-level NOT NULL constraints on
   * columns of `output`, or None otherwise.
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
    Some(new GlutenDeltaNotNullChecker(columnOrdinals.toArray, topLevelNotNullConstraints.toArray))
  }

  /**
   * Returns a checker that runs Delta's `boundChecks` on the `columns` of `output` that they are
   * bound to.
   */
  def forColumns(
      output: Seq[Attribute],
      columns: Seq[Attribute],
      boundChecks: Seq[Expression]): GlutenDeltaInvariantChecker = {
    val columnOrdinals = columns.map(column => output.indexWhere(_.exprId == column.exprId))
    assert(!columnOrdinals.contains(-1), s"Columns $columns are not all in $output")
    new GlutenDeltaReferencedColumnsChecker(columnOrdinals.toArray, boundChecks)
  }

  /**
   * Returns the NOT NULL violation whose text is in the messages of `failure`, the failure of a
   * write, if there is one.
   *
   * Native code that pulls the checked batches, such as the Velox writer's sort on a partitioned
   * write, keeps only the text of a Java exception. The write then fails with a GlutenException
   * whose message holds the violation's text. The returned violation is rebuilt from the column
   * name in that text and carries `failure` as a suppressed exception.
   *
   * TODO: Remove once native code keeps Java exceptions
   * (https://github.com/apache/gluten/pull/13561).
   */
  def findViolation(failure: Throwable): Option[DeltaInvariantViolationException] = {
    ExceptionUtils
      .getThrowableList(failure)
      .asScala
      .iterator
      .flatMap(throwable => Option(throwable.getMessage))
      .map(violatedColumn)
      .collectFirst { case Some(column) => column }
      .map {
        column =>
          val violation = notNullViolation(column)
          violation.addSuppressed(failure)
          violation
      }
  }

  private def notNullViolation(column: String): DeltaInvariantViolationException =
    new DeltaInvariantViolationException("DELTA_NOT_NULL_CONSTRAINT_VIOLATED", Array(column))

  // The text of a NOT NULL violation before and after its column name.
  private lazy val notNullText: Option[(String, String)] = {
    val column = "glutenColumnName"
    val text = notNullViolation(column).toString
    val columnStart = text.indexOf(column)
    if (columnStart < 0) {
      None
    } else {
      Some((text.substring(0, columnStart), text.substring(columnStart + column.length)))
    }
  }

  /** The column name in the text of a NOT NULL violation in `message`, if there is one. */
  private def violatedColumn(message: String): Option[String] = notNullText.flatMap {
    case (before, after) =>
      val start = message.indexOf(before)
      val columnEnd = if (start < 0) -1 else message.indexOf(after, start + before.length)
      if (columnEnd < 0) None else Some(message.substring(start + before.length, columnEnd))
  }
}

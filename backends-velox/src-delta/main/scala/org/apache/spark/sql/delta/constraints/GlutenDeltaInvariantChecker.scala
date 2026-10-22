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
import org.apache.gluten.execution.{PlaceholderRow, TerminalRow}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.V1WritesUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Native write-time invariant checker for constraints that can be validated without converting
 * Velox batches back to Spark rows.
 *
 * Violations are detected natively, by scanning the nulls of the constrained columns of each Velox
 * batch (no per-row expression evaluation, no extra operator in the write plan), but thrown on the
 * JVM side, preserving Delta's typed [[DeltaInvariantViolationException]] error contract.
 * Offloading Delta's CheckDeltaInvariant expression to Velox instead would lose that contract:
 * exceptions raised inside Velox surface only as GlutenException with a message string. CHECK
 * constraints and nested NOT NULL constraints fall back to Delta's row-based
 * DeltaInvariantCheckerExec.
 *
 * Shared by every Delta version, so that they all enforce the constraints the same way.
 *
 * @param columnOrdinals
 *   the ordinal in the write plan's output of the column each constraint checks
 * @param constraints
 *   the constraints, parallel to `columnOrdinals`
 */
private[delta] class GlutenDeltaInvariantChecker private (
    columnOrdinals: Array[Int],
    constraints: Array[NotNull])
  extends Serializable {

  def wrap(rows: Iterator[InternalRow]): Iterator[InternalRow] = {
    rows.map {
      row =>
        check(row)
        row
    }
  }

  private def check(row: InternalRow): Unit = row match {
    case _: PlaceholderRow =>
    case terminal: TerminalRow => check(terminal.batch())
    case other => checkRow(other)
  }

  private def check(batch: ColumnarBatch): Unit = {
    val failedConstraintIndex = VeloxColumnarBatches.firstNullColumnIndex(batch, columnOrdinals)
    if (failedConstraintIndex >= 0) {
      throw DeltaInvariantViolationException(constraints(failedConstraintIndex))
    }
  }

  private def checkRow(row: InternalRow): Unit = {
    var i = 0
    while (i < columnOrdinals.length) {
      if (row.isNullAt(columnOrdinals(i))) {
        throw DeltaInvariantViolationException(constraints(i))
      }
      i += 1
    }
  }
}

private[delta] object GlutenDeltaInvariantChecker {

  /**
   * Returns the checker GlutenDeltaFileFormatWriter applies to the write of `plan`, or None if the
   * write must keep Delta's row-based DeltaInvariantCheckerExec.
   *
   * @param plan
   *   the plan to write, after Delta's own conversion of empty partition strings to nulls
   */
  def forWrite(
      plan: SparkPlan,
      partitionColumns: Seq[Attribute],
      constraints: Seq[Constraint],
      conf: SQLConf): Option[GlutenDeltaInvariantChecker] = {
    // The checker runs in the writer's own tasks, which a WriteFilesExec plan does not use.
    if (V1WritesUtils.getWriteFilesOpt(plan).isDefined) {
      return None
    }
    // The checker sees the rows after the writer has turned empty partition strings into nulls.
    // Delta checks them before that, and converts them itself first only when this conf is on.
    if (
      !conf.getConf(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL) &&
      partitionColumns.exists(_.dataType == StringType)
    ) {
      return None
    }
    create(plan.output, constraints)
  }

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

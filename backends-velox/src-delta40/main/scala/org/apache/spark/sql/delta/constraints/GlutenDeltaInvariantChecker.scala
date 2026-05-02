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
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Native write-time invariant checker for constraints that can be validated without converting
 * Velox batches back to Spark rows.
 */
private[delta] case class GlutenDeltaInvariantChecker private (
    notNullConstraints: Seq[(Int, NotNull)])
  extends Serializable {

  @transient private lazy val columnOrdinals: Array[Int] =
    notNullConstraints.map(_._1).toArray

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
      throw DeltaInvariantViolationException(notNullConstraints(failedConstraintIndex)._2)
    }
  }

  private def checkRow(row: InternalRow): Unit = {
    var i = 0
    while (i < notNullConstraints.size) {
      val (ordinal, constraint) = notNullConstraints(i)
      if (row.isNullAt(ordinal)) {
        throw DeltaInvariantViolationException(constraint)
      }
      i += 1
    }
  }
}

private[delta] object GlutenDeltaInvariantChecker {
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

    val checks = topLevelNotNullConstraints.map {
      constraint =>
        val columnName = constraint.column.head
        val ordinal = output.indexWhere {
          attribute => SchemaUtils.DELTA_COL_RESOLVER(attribute.name, columnName)
        }
        if (ordinal < 0) {
          return None
        }
        ordinal -> constraint
    }
    Some(GlutenDeltaInvariantChecker(checks))
  }
}

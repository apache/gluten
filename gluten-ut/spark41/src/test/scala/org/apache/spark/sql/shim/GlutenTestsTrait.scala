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
package org.apache.spark.sql.shim

import org.apache.spark.sql
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Expression}

/**
 * A Spark 4.1 compatible test trait extending [[sql.GlutenTestsTrait]] to customize expression
 * evaluation logic.
 *
 * Key difference: Calls `replace()` after `ResolveTimeZone` to replace RuntimeReplaceable
 * expressions with their concrete implementations before evaluation to avoid not resolved
 * expressions in the expression tree.
 */
trait GlutenTestsTrait extends sql.GlutenTestsTrait {

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {

    if (canConvertToDataFrame(inputRow)) {
      val resolver = ResolveTimeZone
      val expr = replace(resolver.resolveTimeZones(expression))
      assert(expr.resolved)

      glutenCheckExpression(expr, expected, inputRow)
    } else {
      logWarning(
        "Skipping evaluation - Nonempty inputRow cannot be converted to DataFrame " +
          "due to complex/unsupported types.\n")
    }
  }

}

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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Finds expressions whose ANSI mode differs from the session's.
 *
 * Spark resolves ANSI mode per expression during analysis and stores it in the expression
 * (`failOnError`, `ansiEnabled` or `EvalMode`). Normally that is the session's mode, but a view
 * runs its body with the mode it was created with (SPARK-33141, SPARK-53348), so one query can mix
 * both modes.
 *
 * Velox's Spark functions have no per-call mode. Functions with a fixed mode or a name per mode
 * (`checked_add`, `try_cast`, `get`, `try_make_timestamp`, ...) are picked from the expression and
 * are always correct. The functions listed here read `spark.ansi_enabled` from the query config
 * instead, so they would run in the session's mode even when the expression captured the other one.
 * Such expressions are not offloaded and fall back to Spark.
 */
object AnsiModeMismatch {

  /** True if `expr` captured an ANSI mode that differs from the session's. */
  def apply(expr: Expression): Boolean = {
    capturedAnsiMode(expr).exists(_ != SQLConf.get.ansiEnabled)
  }

  /**
   * The ANSI mode captured in `expr`, for the expressions whose Velox function reads the session
   * config. None for everything else.
   */
  private def capturedAnsiMode(expr: Expression): Option[Boolean] = expr match {
    case e: Remainder => Some(e.evalMode == EvalMode.ANSI)
    case e: Pmod => Some(e.evalMode == EvalMode.ANSI)
    case e: UnaryMinus => Some(e.failOnError)
    case e: Abs => Some(e.failOnError)
    // Only integral round can overflow, on a negative scale.
    case e: Round if isIntegral(e.child.dataType) => Some(e.ansiEnabled)
    case e: BRound if isIntegral(e.child.dataType) => Some(e.ansiEnabled)
    // Spark throws on ceil / floor overflow of a double since 4.3 (SPARK-58052). Earlier versions
    // saturate in both modes, which Velox only does with ANSI mode disabled.
    case e: Ceil if e.child.dataType == DoubleType => Some(failOnErrorOrFalse(e))
    case e: Floor if e.child.dataType == DoubleType => Some(failOnErrorOrFalse(e))
    case e: Conv => Some(e.ansiEnabled)
    case e: Elt => Some(e.failOnError)
    case e: MakeDate => Some(e.failOnError)
    // make_timestamp with failOnError disabled maps to try_make_timestamp, which has a fixed mode.
    case e: MakeTimestamp if e.failOnError => Some(true)
    case e: UnixTimestamp => Some(e.failOnError)
    case e: ToUnixTimestamp => Some(e.failOnError)
    case e: GetTimestamp => Some(e.failOnError)
    case e: NextDay => Some(e.failOnError)
    // A map lookup with failOnError disabled maps to get_map_value, which has a fixed mode.
    case e: ElementAt if e.left.dataType.isInstanceOf[MapType] && !e.failOnError => None
    case e: ElementAt => Some(e.failOnError)
    case e: Sum => Some(sumEvalMode(e) == EvalMode.ANSI)
    case e: Average => Some(e.evalMode == EvalMode.ANSI)
    case _ => None
  }

  private def isIntegral(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }

  // `Sum` holds `evalMode` directly up to Spark 4.0 and inside `evalContext` since 4.1.
  private def sumEvalMode(sum: Sum): EvalMode.Value = {
    try {
      sum.getClass.getMethod("evalMode").invoke(sum).asInstanceOf[EvalMode.Value]
    } catch {
      case _: NoSuchMethodException =>
        val context = sum.getClass.getMethod("evalContext").invoke(sum)
        context.getClass.getMethod("evalMode").invoke(context).asInstanceOf[EvalMode.Value]
    }
  }

  // `Ceil` and `Floor` have `failOnError` only since Spark 4.3.
  private def failOnErrorOrFalse(expr: Expression): Boolean = {
    try {
      expr.getClass.getMethod("failOnError").invoke(expr).asInstanceOf[Boolean]
    } catch {
      case _: NoSuchMethodException => false
    }
  }
}

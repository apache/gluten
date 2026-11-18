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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenExpressionOffloadTracker
import org.apache.spark.sql.shim.GlutenTestsTrait

class GlutenTryEvalSuite
  extends TryEvalSuite
  with GlutenExpressionOffloadTracker
  with GlutenTestsTrait {
  override protected def panoramaMeta(expression: Expression): Map[String, String] =
    expression match {
      case _: Add => Map("operator" -> "Add")
      case _: Subtract => Map("operator" -> "Subtract")
      case _: Multiply => Map("operator" -> "Multiply")
      case _: Divide => Map("operator" -> "Divide")
      case _: IntegralDivide => Map("operator" -> "IntegralDivide")
      case _: Remainder => Map("operator" -> "Remainder")
      case _: Pmod => Map("operator" -> "Pmod")
      case _: Abs => Map("operator" -> "Abs")
      case _: UnaryMinus => Map("operator" -> "UnaryMinus")
      case _ => Map.empty
    }
  override protected def offloadCategory: String = "arithmetic"
}

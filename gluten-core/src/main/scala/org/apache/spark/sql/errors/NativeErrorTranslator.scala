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
package org.apache.spark.sql.errors

import org.apache.spark.{QueryContext, SparkArithmeticException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

/**
 * Translates native errors that carry a Spark error condition into the matching Spark exception.
 *
 * Velox reports the condition as the error code "SPARK:<condition>". The native error message
 * holds it in the line "Error Code: SPARK:<condition>", for example "Error Code:
 * SPARK:DIVIDE_BY_ZERO".
 */
object NativeErrorTranslator extends Logging {

  private val SparkErrorCodePrefix = "SPARK:"

  // Only the first "Error Code:" line is used. It comes before the "Reason:" line, which may hold
  // user data, so a value in the reason cannot pose as the error code.
  private val ErrorCodeLine = "(?m)^Error Code: (\\S*)\\s*$".r

  // Bounds the walk over the cause chain, which may contain a cycle.
  private val MaxCauseDepth = 16

  /** Returns the Spark error condition in a native error message, if any. */
  def sparkCondition(message: String): Option[String] = {
    Option(message)
      .flatMap(ErrorCodeLine.findFirstMatchIn(_))
      .map(_.group(1))
      .filter(code => code.startsWith(SparkErrorCodePrefix))
      .map(_.substring(SparkErrorCodePrefix.length))
      .filter(_.nonEmpty)
  }

  /**
   * Returns the Spark exception for a native error, with the native error as its cause. Returns
   * null if the error has no Spark error condition or the condition is not supported yet.
   */
  def translate(error: Throwable): RuntimeException = {
    val condition = Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .take(MaxCauseDepth)
      .map(t => sparkCondition(t.getMessage))
      .collectFirst { case Some(c) => c }
    condition match {
      case Some(c) =>
        toSparkException(c) match {
          case Some(sparkError) =>
            sparkError.initCause(error)
            logInfo(
              s"Translated native error with Spark error condition $c to " +
                s"${sparkError.getClass.getName}.")
            sparkError
          case None =>
            logWarning(
              s"Native error has Spark error condition $c, which is not supported yet. " +
                "Keeping the native error.")
            null
        }
      case None => null
    }
  }

  private def toSparkException(condition: String): Option[RuntimeException] = condition match {
    case "DIVIDE_BY_ZERO" =>
      Some(
        new SparkArithmeticException(
          "DIVIDE_BY_ZERO",
          Map("config" -> QueryExecutionErrors.toSQLConf(SQLConf.ANSI_ENABLED.key)),
          Array.empty[QueryContext],
          ""))
    case _ => None
  }
}

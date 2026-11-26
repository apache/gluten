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

import org.apache.gluten.exception.GlutenException

import org.apache.spark.{QueryContext, SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SparkVersionUtil

/**
 * Translates native errors into the matching Spark exceptions.
 *
 * Native code sets the Spark error condition, for example "DIVIDE_BY_ZERO", as the error code of
 * the GlutenException that the JNI boundary raises for a native error. Errors whose condition is
 * not supported yet, or that have no error code, fall back to translation based on the error
 * message. The message-based translation can be removed for an error once native code attaches its
 * condition.
 */
object NativeErrorTranslator extends Logging {

  // Velox's message when a char or varchar value is longer than its declared length.
  private val CharVarcharLengthExceeded = "Exceeds allowed length limitation: (\\d+)".r

  /**
   * Returns the Spark exception for a native error, with the native error as its cause. Returns
   * null if the error cannot be translated.
   */
  def translate(error: Throwable): RuntimeException =
    Option(error).flatMap(e => translateByErrorCode(e).orElse(translateByMessage(e))).orNull

  // Only the GlutenException raised at the JNI boundary carries the error code. Any other
  // exception, including one with a native error as its cause, is translated by its message.
  private def translateByErrorCode(error: Throwable): Option[RuntimeException] = error match {
    case e: GlutenException if e.getErrorCode != null =>
      val errorCode = e.getErrorCode
      val translated = toSparkException(errorCode)
      translated match {
        case Some(sparkError) =>
          sparkError.initCause(e)
          logInfo(
            s"Translated native error with error code $errorCode to " +
              s"${sparkError.getClass.getName}.")
        case None =>
          logWarning(
            s"Native error has error code $errorCode, which is not supported yet. " +
              "Translating it by its message.")
      }
      translated
    case _ => None
  }

  // The native error has no Spark query context. QueryExecutionErrors.divideByZeroError(null)
  // would report a null context entry, which PySpark fails to read, so the context is left empty.
  private def toSparkException(errorCode: String): Option[RuntimeException] = errorCode match {
    case SparkErrorCodes.DivideByZero =>
      Some(
        new SparkArithmeticException(
          SparkErrorCodes.DivideByZero,
          Map("config" -> QueryExecutionErrors.toSQLConf(SQLConf.ANSI_ENABLED.key)),
          Array.empty[QueryContext],
          ""))
    case _ => None
  }

  private def translateByMessage(error: Throwable): Option[RuntimeException] = {
    val translated = firstNonNullMessage(error).flatMap {
      message =>
        toSchemaException(message, error).orElse(toCharVarcharLengthException(message, error))
    }
    translated.foreach {
      e => logInfo(s"Translated native error by its message to ${e.getClass.getName}.")
    }
    translated
  }

  // Translates a Velox type conversion error into a SchemaColumnConvertNotSupportedException.
  private def toSchemaException(message: String, error: Throwable): Option[RuntimeException] = {
    if (
      message.contains("not allowed for requested type") || message.contains("Not a valid type for")
    ) {
      val schemaError = new SchemaColumnConvertNotSupportedException("unknown", message, "unknown")
      schemaError.initCause(error)
      Some(schemaError)
    } else {
      None
    }
  }

  // Translates a Velox char/varchar length check failure into the exception Spark throws for a
  // value longer than its declared length.
  private def toCharVarcharLengthException(
      message: String,
      error: Throwable): Option[RuntimeException] = {
    CharVarcharLengthExceeded.findFirstMatchIn(message).map {
      m =>
        val limit = m.group(1)
        if (!SparkVersionUtil.gteSpark35) {
          // Spark 3.4 has no EXCEED_LIMIT_LENGTH error class and throws a plain RuntimeException.
          new RuntimeException(s"Exceeds char/varchar type length limitation: $limit", error)
        } else {
          // Builds the exception QueryExecutionErrors.exceedMaxLimit builds, with the native error
          // as its cause. SparkRuntimeException's cause can only be set through its constructor.
          new SparkRuntimeException(
            "EXCEED_LIMIT_LENGTH",
            Map("limit" -> limit),
            error,
            Array.empty[QueryContext],
            "")
        }
    }
  }

  private def firstNonNullMessage(error: Throwable): Option[String] =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.getMessage).find(_ != null)
}

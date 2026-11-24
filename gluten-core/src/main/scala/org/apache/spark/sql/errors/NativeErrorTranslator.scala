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

import org.apache.spark.internal.Logging

/**
 * Translates native errors that carry a Spark error code into the matching Spark exception.
 *
 * Native code sets the error code, for example "SPARK:DIVIDE_BY_ZERO", on the GlutenException that
 * the JNI boundary raises for a native error. Any other exception, including one with a native
 * error as its cause, keeps the existing handling.
 */
object NativeErrorTranslator extends Logging {

  /**
   * Returns the Spark exception for a native error, with the native error as its cause. Returns
   * null if the error has no error code or its error code is not supported.
   */
  def translate(error: Throwable): RuntimeException = error match {
    case e: GlutenException if e.getErrorCode != null =>
      val errorCode = e.getErrorCode
      toSparkException(errorCode) match {
        case Some(sparkError) =>
          sparkError.initCause(e)
          logInfo(
            s"Translated native error with error code $errorCode to " +
              s"${sparkError.getClass.getName}.")
          sparkError
        case None =>
          if (errorCode.startsWith(NativeErrorCodes.SparkErrorCodePrefix)) {
            logWarning(
              s"Native error has error code $errorCode, which is not supported yet. " +
                "Keeping the native error.")
          }
          null
      }
    case _ => null
  }

  private def toSparkException(errorCode: String): Option[RuntimeException] = errorCode match {
    case NativeErrorCodes.DivideByZero => Some(QueryExecutionErrors.divideByZeroError(null))
    case _ => None
  }
}

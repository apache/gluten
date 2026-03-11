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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.TransformerState
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.test.TestStats

import org.apache.spark.sql.catalyst.analysis.UnresolvedException

/**
 * Base interface for a Gluten query plan that is also open to validation calls.
 *
 * Since https://github.com/apache/incubator-gluten/pull/2185.
 */
trait ValidatablePlan extends GlutenPlan with LogLevelUtil {
  protected def glutenConf: GlutenConfig = GlutenConfig.get

  protected lazy val enableNativeValidation = glutenConf.enableNativeValidation

  protected lazy val validationFailFast = glutenConf.validationFailFast

  // Wraps a validation function f that can also throw a GlutenNotSupportException.
  // Returns ValidationResult.failed if f throws a GlutenNotSupportException,
  // otherwise returns the result of f.
  protected def failValidationWithException(f: => ValidationResult)(
      finallyBlock: => Unit = ()): ValidationResult = {
    def makeFailed(e: Exception): ValidationResult = {
      val message = s"Validation failed with exception from: $nodeName, reason: ${e.getMessage}"
      if (glutenConf.printStackOnValidationFailure) {
        logOnLevel(glutenConf.validationLogLevel, message, e)
      }
      ValidationResult.failed(message)
    }
    try {
      f
    } catch {
      case e: GlutenNotSupportException =>
        logDebug(s"Just a warning. This exception perhaps needs to be fixed.", e)
        makeFailed(e)
      case e: UnsupportedOperationException =>
        makeFailed(e)
      case t: Throwable =>
        throw t
    } finally {
      finallyBlock
    }
  }

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  final def doValidate(): ValidationResult = {
    val schemaValidationResult =
      try {
        BackendsApiManager.getValidatorApiInstance
          .doSchemaValidate(schema)
          .map {
            reason =>
              ValidationResult.failed(s"Found schema check failure for $schema, due to: $reason")
          }
          .getOrElse(ValidationResult.succeeded)
      } catch {
        case u: UnresolvedException =>
          val message =
            s"Failed to retrieve schema, due to: ${u.getMessage}." +
              s" If you are using a hash expression with a map key," +
              s" consider enabling the spark.sql.legacy.allowHashOnMapType " +
              s"setting to resolve this issue."
          ValidationResult.failed(message)
        case e: IllegalArgumentException =>
          // SchemaValidation throws IllegalArgumentException in Join validation
          // when join type is unsupported. For example,
          // LeftSingle join in BroadcastHashJoinExecTransformer.
          ValidationResult.failed(
            s"Failed to retrieve schema for ${this.nodeName}, due to: ${e.getMessage}")
      }

    if (!schemaValidationResult.ok()) {
      TestStats.addFallBackClassName(this.getClass.toString)
      if (validationFailFast) {
        return schemaValidationResult
      }
    }
    val validationResult = failValidationWithException {
      TransformerState.enterValidation
      doValidateInternal()
    } {
      TransformerState.finishValidation
    }
    if (!validationResult.ok()) {
      TestStats.addFallBackClassName(this.getClass.toString)
    }
    if (validationFailFast) validationResult
    else ValidationResult.merge(schemaValidationResult, validationResult)
  }

  protected def doValidateInternal(): ValidationResult = ValidationResult.succeeded

  private def logValidationMessage(msg: => String, e: Throwable): Unit = {
    if (glutenConf.printStackOnValidationFailure) {
      logOnLevel(glutenConf.validationLogLevel, msg, e)
    } else {
      logOnLevel(glutenConf.validationLogLevel, msg)
    }
  }
}

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
package org.apache.gluten.utils

import org.apache.spark.SparkException

object ExceptionUtils {

  // Spark's QueryExecutionErrors.invalidBucketFile is private[sql] and cannot be reached from
  // this package, so build the same exception here.
  // https://issues.apache.org/jira/browse/SPARK-40400
  def invalidBucketFile(path: String): Throwable =
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Map("path" -> path),
      cause = null)

  /**
   * Utility to check the exception for the specified type.
   *
   * @param throwable
   *   Exception to check
   * @param causeType
   *   Class of the cause to look for
   * @tparam T
   *   Type of the cause
   * @return
   *   True if the cause is found; false otherwise
   */
  def hasCause[T <: Throwable](throwable: Throwable, causeType: Class[T]): Boolean = {
    var current = throwable
    while (current != null) {
      if (causeType.isInstance(current)) {
        return true
      }
      current = current.getCause
    }
    false
  }
}

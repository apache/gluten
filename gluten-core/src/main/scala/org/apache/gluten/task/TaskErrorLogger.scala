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
package org.apache.gluten.task

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.internal.Logging

/** Utility object for logging task errors in a consistent manner. */
object TaskErrorLogger extends Logging {

  /**
   * Logs task failure errors with appropriate filtering.
   *
   * @param context
   *   The TaskContext of the failed task
   * @param error
   *   The error that caused the task failure
   */
  def logTaskFailure(context: TaskContext, error: Throwable): Unit = {
    error match {
      case e: TaskKilledException if e.reason == "another attempt succeeded" =>
        // This is an expected scenario in speculative execution, no need to log as error
        logDebug(s"Task ${context.taskAttemptId()} was killed because another attempt succeeded")
      case _ =>
        // Log genuine errors for debugging
        logError(s"Task ${context.taskAttemptId()} failed with error: ", error)
    }
  }
}

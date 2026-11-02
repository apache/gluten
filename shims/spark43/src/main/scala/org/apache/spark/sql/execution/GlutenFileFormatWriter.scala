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
package org.apache.spark.sql.execution

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{FileFormatWriter, WriteJobDescription, WriteTaskResult}

object GlutenFileFormatWriter {
  def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {
    FileFormatWriter.executeTask(
      description,
      jobTrackerID,
      sparkStageId,
      sparkPartitionId,
      sparkAttemptNumber,
      committer,
      iterator,
      None
    )
  }

  // Wrapper for throwing standardized write error using QueryExecutionErrors
  def wrapWriteError(cause: Throwable, writePath: String): Nothing = {
    throw QueryExecutionErrors.taskFailedWhileWritingRowsError(writePath, cause)
  }
}

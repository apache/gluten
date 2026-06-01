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
package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex

object GlutenDMLWithDeletionVectorsHelper extends DeltaCommand {
  def findTouchedFiles(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      hasDVsEnabled: Boolean,
      deltaLog: DeltaLog,
      targetDf: DataFrame,
      fileIndex: TahoeBatchFileIndex,
      condition: Expression,
      opName: String): Seq[TouchedFileWithDV] = {
    require(
      DMLWithDeletionVectorsHelper.SUPPORTED_DML_COMMANDS.contains(opName),
      s"Expecting opName to be one of " +
        s"${DMLWithDeletionVectorsHelper.SUPPORTED_DML_COMMANDS.mkString(", ")}, " +
        s"but got '$opName'."
    )

    recordDeltaOperation(deltaLog, opType = s"$opName.findTouchedFiles.gluten") {
      val candidateFiles = fileIndex.addFiles
      val matchedRowIndexSets =
        DeletionVectorBitmapGenerator.buildRowIndexSetsForFilesMatchingCondition(
          sparkSession,
          txn,
          hasDVsEnabled,
          targetDf,
          candidateFiles,
          condition)

      val nameToAddFileMap = generateCandidateFileMap(txn.deltaLog.dataPath, candidateFiles)
      DMLWithDeletionVectorsHelper.findFilesWithMatchingRows(
        txn,
        nameToAddFileMap,
        matchedRowIndexSets)
    }
  }
}

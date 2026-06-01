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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils, NumRecordsStats, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.commands.MergeIntoCommandBase.totalBytesAndDistinctPartitionValues
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf

object GlutenDeleteCommand {
  def apply(delegate: DeleteCommand): GlutenDeleteCommand =
    new GlutenDeleteCommand(
      delegate.deltaLog,
      delegate.catalogTable,
      delegate.target,
      delegate.condition)

  def shouldOffload(delegate: DeleteCommand, sparkSession: SparkSession): Boolean = {
    val persistentDeletionVectorsEnabled =
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS)
    if (!persistentDeletionVectorsEnabled) {
      return false
    }

    delegate.condition.exists {
      deleteCondition =>
        val snapshot = delegate.deltaLog.update()
        val (_, dataPredicates) = DeltaTableUtils.splitMetadataAndDataPredicates(
          deleteCondition,
          snapshot.metadata.partitionColumns,
          sparkSession)
        dataPredicates.nonEmpty && DeletionVectorUtils.deletionVectorsWritable(snapshot)
    }
  }
}

@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class GlutenDeleteCommand(
    override val deltaLog: DeltaLog,
    override val catalogTable: Option[CatalogTable],
    override val target: LogicalPlan,
    override val condition: Option[Expression])
  extends DeleteCommand(deltaLog, catalogTable, target, condition) {

  override def performDelete(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      txn: OptimisticTransaction): (Seq[Action], DeleteMetric) = {
    val (cond, metadataPredicates, otherPredicates) =
      condition match {
        case Some(deleteCondition) =>
          val (metadata, data) = DeltaTableUtils.splitMetadataAndDataPredicates(
            deleteCondition,
            txn.metadata.partitionColumns,
            sparkSession)
          (deleteCondition, metadata, data)
        case None =>
          return super.performDelete(sparkSession, deltaLog, txn)
      }

    val shouldWriteDVs = otherPredicates.nonEmpty &&
      shouldWritePersistentDeletionVectors(sparkSession, txn)
    if (!shouldWriteDVs) {
      return super.performDelete(sparkSession, deltaLog, txn)
    }

    var numRemovedFiles: Long = 0
    val numAddedFiles: Long = 0
    var scanTimeMs: Long = 0
    val rewriteTimeMs: Long = 0
    val numAddedBytes: Long = 0
    val changeFileBytes: Long = 0
    val numRemovedBytes: Long = 0
    val numFilesBeforeSkipping: Long = txn.snapshot.numOfFiles
    val numBytesBeforeSkipping: Long = txn.snapshot.sizeInBytes
    var numFilesAfterSkipping: Long = 0
    var numBytesAfterSkipping: Long = 0
    var numPartitionsAfterSkipping: Option[Long] = None
    val numPartitionsRemovedFrom: Option[Long] = None
    val numPartitionsAddedTo: Option[Long] = None
    var numDeletedRows: Option[Long] = None
    val numCopiedRows: Option[Long] = None
    var numDeletionVectorsAdded: Long = 0
    var numDeletionVectorsRemoved: Long = 0
    var numDeletionVectorsUpdated: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = txn.snapshot.numOfFiles
    val candidateFiles = txn.filterFiles(
      metadataPredicates ++ otherPredicates,
      keepNumRecords = true)

    numFilesAfterSkipping = candidateFiles.size
    val (numCandidateBytes, numCandidatePartitions) =
      totalBytesAndDistinctPartitionValues(candidateFiles)
    numBytesAfterSkipping = numCandidateBytes
    if (txn.metadata.partitionColumns.nonEmpty) {
      numPartitionsAfterSkipping = Some(numCandidatePartitions)
    }

    val fileIndex = new TahoeBatchFileIndex(
      sparkSession,
      "delete",
      candidateFiles,
      deltaLog,
      deltaLog.dataPath,
      txn.snapshot)
    val targetDf = DMLWithDeletionVectorsHelper.createTargetDfForScanningForMatches(
      sparkSession,
      target,
      fileIndex)
    val mustReadDeletionVectors = DeletionVectorUtils.deletionVectorsReadable(txn.snapshot)
    val touchedFiles = GlutenDMLWithDeletionVectorsHelper.findTouchedFiles(
      sparkSession,
      txn,
      mustReadDeletionVectors,
      deltaLog,
      targetDf,
      fileIndex,
      cond,
      opName = "DELETE")

    scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
    val deleteActions =
      if (touchedFiles.nonEmpty) {
        val (actions, metricMap) = DMLWithDeletionVectorsHelper.processUnmodifiedData(
          sparkSession,
          touchedFiles,
          txn.snapshot)
        metrics("numDeletedRows").set(metricMap("numModifiedRows"))
        numDeletedRows = Some(metricMap("numModifiedRows"))
        numDeletionVectorsAdded = metricMap("numDeletionVectorsAdded")
        numDeletionVectorsRemoved = metricMap("numDeletionVectorsRemoved")
        numDeletionVectorsUpdated = metricMap("numDeletionVectorsUpdated")
        numRemovedFiles = metricMap("numRemovedFiles")
        actions
      } else {
        Nil
      }

    metrics("numRemovedFiles").set(numRemovedFiles)
    metrics("numAddedFiles").set(numAddedFiles)
    val executionTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
    metrics("executionTimeMs").set(executionTimeMs)
    metrics("scanTimeMs").set(scanTimeMs)
    metrics("rewriteTimeMs").set(rewriteTimeMs)
    metrics("numAddedChangeFiles").set(0L)
    metrics("changeFileBytes").set(changeFileBytes)
    metrics("numAddedBytes").set(numAddedBytes)
    metrics("numRemovedBytes").set(numRemovedBytes)
    metrics("numFilesBeforeSkipping").set(numFilesBeforeSkipping)
    metrics("numBytesBeforeSkipping").set(numBytesBeforeSkipping)
    metrics("numFilesAfterSkipping").set(numFilesAfterSkipping)
    metrics("numBytesAfterSkipping").set(numBytesAfterSkipping)
    metrics("numDeletionVectorsAdded").set(numDeletionVectorsAdded)
    metrics("numDeletionVectorsRemoved").set(numDeletionVectorsRemoved)
    metrics("numDeletionVectorsUpdated").set(numDeletionVectorsUpdated)
    numPartitionsAfterSkipping.foreach(metrics("numPartitionsAfterSkipping").set)
    numPartitionsAddedTo.foreach(metrics("numPartitionsAddedTo").set)
    numPartitionsRemovedFrom.foreach(metrics("numPartitionsRemovedFrom").set)
    numCopiedRows.foreach(metrics("numCopiedRows").set)
    txn.registerSQLMetrics(sparkSession, metrics)
    sendDriverMetrics(sparkSession, metrics)

    val numRecordsStats = NumRecordsStats.fromActions(deleteActions)
    val deleteMetric = DeleteMetric(
      condition = condition.map(_.sql).getOrElse("true"),
      numFilesTotal,
      numFilesAfterSkipping,
      numAddedFiles,
      numRemovedFiles,
      numAddedFiles,
      numAddedChangeFiles = 0L,
      numFilesBeforeSkipping,
      numBytesBeforeSkipping,
      numFilesAfterSkipping,
      numBytesAfterSkipping,
      numPartitionsAfterSkipping,
      numPartitionsAddedTo,
      numPartitionsRemovedFrom,
      numCopiedRows,
      numDeletedRows,
      numAddedBytes,
      numRemovedBytes,
      changeFileBytes = changeFileBytes,
      scanTimeMs,
      rewriteTimeMs,
      numDeletionVectorsAdded,
      numDeletionVectorsRemoved,
      numDeletionVectorsUpdated,
      numLogicalRecordsAdded = numRecordsStats.numLogicalRecordsAdded,
      numLogicalRecordsRemoved = numRecordsStats.numLogicalRecordsRemoved
    )

    val actionsToCommit = if (deleteActions.nonEmpty) {
      createSetTransaction(sparkSession, deltaLog).toSeq ++ deleteActions
    } else {
      Seq.empty
    }
    (actionsToCommit, deleteMetric)
  }
}

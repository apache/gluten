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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.gluten.config.VeloxDeltaConfig
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.delta.{DeletionVectorsTableFeature, NoMapping, Snapshot}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.spark.sql.delta.commands.{DeleteCommand, DeltaCommand, DeltaReorgTableCommand, DeltaReorgTableMode, OptimizeTableCommand, UpdateCommand}
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.tablefeatures.DropFeature
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand

case class OffloadDeltaCommand() extends OffloadSingleNode with DeltaCommand {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (!VeloxDeltaConfig.get.enableNativeWrite) {
      return plan
    }
    plan match {
      case ExecutedCommandExec(uc: UpdateCommand) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(uc))
      case ExecutedCommandExec(dc: DeleteCommand) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(dc))
      case ExecutedCommandExec(optimize: OptimizeTableCommand) if shouldOffloadOptimize(optimize) =>
        ExecutedCommandExec(GlutenDeltaRunnableCommand(optimize))
      case ExecutedCommandExec(reorg: DeltaReorgTableCommand) if shouldOffloadReorg(reorg) =>
        ExecutedCommandExec(GlutenDeltaRunnableCommand(reorg))
      case alter: AlterTableExec if shouldOffloadDropDeletionVectors(alter) =>
        GlutenDeltaLeafV2CommandExec(alter)
      case ExecutedCommandExec(s @ SaveIntoDataSourceCommand(_, _: DeltaDataSource, _, _)) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(s))
      case ctas: AtomicCreateTableAsSelectExec if ctas.catalog.isInstanceOf[DeltaCatalog] =>
        GlutenDeltaLeafV2CommandExec(ctas)
      case rtas: AtomicReplaceTableAsSelectExec if rtas.catalog.isInstanceOf[DeltaCatalog] =>
        GlutenDeltaLeafV2CommandExec(rtas)
      case other => other
    }
  }

  // Currently only plain OPTIMIZE bin-packing is supported for command offload. OPTIMIZE
  // variants with layout-specific semantics, such as ZORDER, OPTIMIZE FULL, or liquid clustering,
  // continue to use Delta's original command path. REORG is offloaded as DeltaReorgTableCommand,
  // which runs its OptimizeTableCommand directly.
  private def shouldOffloadOptimize(optimize: OptimizeTableCommand): Boolean = {
    optimize.zOrderBy.isEmpty &&
    optimize.optimizeContext.reorg.isEmpty &&
    !optimize.optimizeContext.isFull &&
    !isClusteredOptimize(optimize)
  }

  private def isClusteredOptimize(optimize: OptimizeTableCommand): Boolean = {
    val snapshot = getDeltaTable(optimize.child, "OPTIMIZE").update()
    ClusteredTableUtils.isSupported(snapshot.protocol)
  }

  // REORG TABLE ... APPLY (PURGE) rewrites the files that have deletion vectors through the same
  // OptimizeExecutor bin jobs as plain OPTIMIZE. REORG UPGRADE UNIFORM keeps Delta's path.
  private def shouldOffloadReorg(reorg: DeltaReorgTableCommand): Boolean = {
    reorg.reorgTableSpec.reorgTableMode == DeltaReorgTableMode.PURGE &&
    canPurgeNatively(getDeltaTable(reorg.target, "REORG").update())
  }

  // ALTER TABLE ... DROP FEATURE deletionVectors purges the deletion vectors with REORG PURGE,
  // which Delta runs inside the ALTER TABLE command. Delta 3.3 cannot drop the feature.
  private def shouldOffloadDropDeletionVectors(alter: AlterTableExec): Boolean = {
    val dropsDeletionVectors = alter.changes.nonEmpty && alter.changes.forall {
      case drop: DropFeature =>
        drop.featureName.equalsIgnoreCase(DeletionVectorsTableFeature.name)
      case _ => false
    }
    dropsDeletionVectors && alter.catalog.isInstanceOf[DeltaCatalog] && {
      alter.catalog.loadTable(alter.ident) match {
        case table: DeltaTableV2 => canPurgeNatively(table.update())
        case _ => false
      }
    }
  }

  // Clustered tables recluster the rewritten files. Column-mapped tables need Parquet field IDs,
  // which the Velox writer does not write.
  private def canPurgeNatively(snapshot: Snapshot): Boolean = {
    !ClusteredTableUtils.isSupported(snapshot.protocol) &&
    snapshot.metadata.columnMappingMode == NoMapping
  }
}

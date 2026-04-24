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
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{DeleteCommand, UpdateCommand}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.internal.SQLConf

case class OffloadDeltaCommand() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (!VeloxDeltaConfig.get.enableNativeWrite) {
      return plan
    }
    plan match {
      case ExecutedCommandExec(_: UpdateCommand) if shouldFallbackAnsiWrite =>
        FallbackTags.add(plan, "fallback Delta UPDATE in ANSI mode")
        plan
      case ExecutedCommandExec(uc: UpdateCommand)
          if shouldFallbackDeletionVectorDml &&
            deletionVectorsReadable(uc.tahoeFileIndex.deltaLog.update()) =>
        FallbackTags.add(
          plan,
          "fallback Delta UPDATE with deletion vectors when metadata row index is disabled")
        plan
      case ExecutedCommandExec(uc: UpdateCommand) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(uc))
      case ExecutedCommandExec(_: DeleteCommand) if shouldFallbackAnsiWrite =>
        FallbackTags.add(plan, "fallback Delta DELETE in ANSI mode")
        plan
      case ExecutedCommandExec(dc: DeleteCommand)
          if shouldFallbackDeletionVectorDml &&
            deletionVectorsReadable(dc.deltaLog.update()) =>
        FallbackTags.add(
          plan,
          "fallback Delta DELETE with deletion vectors when metadata row index is disabled")
        plan
      case ExecutedCommandExec(dc: DeleteCommand) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(dc))
      case ExecutedCommandExec(_: SaveIntoDataSourceCommand) if shouldFallbackAnsiWrite =>
        FallbackTags.add(plan, "fallback Delta save command in ANSI mode")
        plan
      case ExecutedCommandExec(s @ SaveIntoDataSourceCommand(_, _: DeltaDataSource, _, _)) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(s))
      case ctas: AtomicCreateTableAsSelectExec
          if shouldFallbackAnsiWrite && ctas.catalog.isInstanceOf[DeltaCatalog] =>
        FallbackTags.add(ctas, "fallback Delta CTAS in ANSI mode")
        ctas
      case ctas: AtomicCreateTableAsSelectExec if ctas.catalog.isInstanceOf[DeltaCatalog] =>
        GlutenDeltaLeafV2CommandExec(ctas)
      case rtas: AtomicReplaceTableAsSelectExec
          if shouldFallbackAnsiWrite && rtas.catalog.isInstanceOf[DeltaCatalog] =>
        FallbackTags.add(rtas, "fallback Delta RTAS in ANSI mode")
        rtas
      case rtas: AtomicReplaceTableAsSelectExec if rtas.catalog.isInstanceOf[DeltaCatalog] =>
        GlutenDeltaLeafV2CommandExec(rtas)
      case other => other
    }
  }

  private def shouldFallbackAnsiWrite: Boolean = {
    val conf = SQLConf.get
    conf.ansiEnabled ||
    conf.getConfString(SQLConf.STORE_ASSIGNMENT_POLICY.key, "").equalsIgnoreCase("ANSI")
  }

  private def shouldFallbackDeletionVectorDml: Boolean = {
    !SQLConf.get.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)
  }
}

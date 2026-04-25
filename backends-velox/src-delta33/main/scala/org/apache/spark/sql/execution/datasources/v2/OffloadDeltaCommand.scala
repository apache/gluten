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

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{DeleteCommand, UpdateCommand}
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand

case class OffloadDeltaCommand() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (!VeloxDeltaConfig.get.enableNativeWrite) {
      return plan
    }
    plan match {
      case ExecutedCommandExec(uc: UpdateCommand) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(uc))
      case ExecutedCommandExec(dc: DeleteCommand) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(dc))
      case ExecutedCommandExec(s @ SaveIntoDataSourceCommand(_, _: DeltaDataSource, _, _)) =>
        ExecutedCommandExec(GlutenDeltaLeafRunnableCommand(s))
      case ctas: AtomicCreateTableAsSelectExec if ctas.catalog.isInstanceOf[DeltaCatalog] =>
        GlutenDeltaLeafV2CommandExec(ctas)
      case rtas: AtomicReplaceTableAsSelectExec if rtas.catalog.isInstanceOf[DeltaCatalog] =>
        GlutenDeltaLeafV2CommandExec(rtas)
      case other => other
    }
  }
}

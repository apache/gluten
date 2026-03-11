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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.enumerated.RasOffload
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2._

import org.apache.iceberg.spark.source.IcebergWriteUtil.supportsWrite

case class OffloadIcebergAppend() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case a: AppendDataExec if supportsWrite(a.write) =>
      VeloxIcebergAppendDataExec(a)
    case other => other
  }
}

case class OffloadIcebergReplaceData() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case r: ReplaceDataExec if supportsWrite(r.write) =>
      VeloxIcebergReplaceDataExec(r)
    case other => other
  }
}

case class OffloadIcebergOverwrite() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case r: OverwriteByExpressionExec if supportsWrite(r.write) =>
      VeloxIcebergOverwriteByExpressionExec(r)
    case other => other
  }
}

case class OffloadIcebergOverwritePartitionsDynamic() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case r: OverwritePartitionsDynamicExec if supportsWrite(r.write) =>
      VeloxIcebergOverwritePartitionsDynamicExec(r)
    case other => other
  }
}

case class OffloadIcebergWriteToDataSourceV2() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case r: WriteToDataSourceV2Exec =>
      VeloxIcebergWriteToDataSourceV2Exec(r).getOrElse(r)
    case other => other
  }
}

object OffloadIcebergWrite {
  def inject(injector: Injector): Unit = {
    // Inject legacy rule.
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(
          OffloadIcebergAppend(),
          OffloadIcebergReplaceData(),
          OffloadIcebergOverwrite(),
          OffloadIcebergOverwritePartitionsDynamic(),
          OffloadIcebergWriteToDataSourceV2()
        )
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload
        )
    }

    val offloads: Seq[RasOffload] = Seq(
      RasOffload.from[AppendDataExec](OffloadIcebergAppend()),
      RasOffload.from[ReplaceDataExec](OffloadIcebergReplaceData()),
      RasOffload.from[OverwriteByExpressionExec](OffloadIcebergOverwrite()),
      RasOffload.from[OverwritePartitionsDynamicExec](OffloadIcebergOverwritePartitionsDynamic()),
      RasOffload.from[WriteToDataSourceV2Exec](OffloadIcebergWriteToDataSourceV2())
    )
    offloads.foreach(
      offload =>
        injector.gluten.ras.injectRasRule(
          c => RasOffload.Rule(offload, Validators.newValidator(new GlutenConfig(c.sqlConf)), Nil)))
  }
}

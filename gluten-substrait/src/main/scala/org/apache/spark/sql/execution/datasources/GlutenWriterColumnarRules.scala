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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.ColumnarToRowExecBase
import org.apache.gluten.execution.datasource.GlutenFormatFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable}
import org.apache.spark.sql.sources.DataSourceRegister

object GlutenWriterColumnarRules {
  // TODO: support ctas in Spark3.4, see https://github.com/apache/spark/pull/39220
  // TODO: support dynamic partition and bucket write
  //  1. pull out `Empty2Null` and required ordering to `WriteFilesExec`, see Spark3.4 `V1Writes`
  //  2. support detect partition value, partition path, bucket value, bucket path at native side,
  //     see `BaseDynamicPartitionDataWriter`
  private val formatMapping = Map(
    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat" -> "orc",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" -> "parquet"
  )
  private def getNativeFormat(cmd: DataWritingCommand): Option[String] = {
    cmd match {
      case command: CreateDataSourceTableAsSelectCommand
          if !BackendsApiManager.getSettings.skipNativeCtas(command) =>
        command.table.provider.filter(GlutenFormatFactory.isRegistered)
      case command: InsertIntoHadoopFsRelationCommand
          if !BackendsApiManager.getSettings.skipNativeInsertInto(command) =>
        command.fileFormat match {
          case register: DataSourceRegister
              if GlutenFormatFactory.isRegistered(register.shortName()) =>
            Some(register.shortName())
          case _ => None
        }
      case command: InsertIntoHiveDirCommand =>
        command.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
      case command: InsertIntoHiveTable =>
        command.table.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
      case command: CreateHiveTableAsSelectCommand =>
        command.tableDesc.storage.outputFormat
          .flatMap(formatMapping.get)
          .filter(GlutenFormatFactory.isRegistered)
      case _ =>
        None
    }
  }

  private[datasources] def injectFakeRowAdaptor(command: SparkPlan, child: SparkPlan): SparkPlan = {
    child match {
      // if the child is columnar, we can just wrap & transfer the columnar data
      case c2r: ColumnarToRowExecBase =>
        command.withNewChildren(
          Array(BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToCarrierRow(c2r.child)))
      // If the child is aqe, we make aqe "support columnar",
      // then aqe itself will guarantee to generate columnar outputs.
      // So FakeRowAdaptor will always consumes columnar data,
      // thus avoiding the case of c2r->aqe->r2c->writer
      case aqe: AdaptiveSparkPlanExec =>
        command.withNewChildren(
          Array(
            BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToCarrierRow(
              AdaptiveSparkPlanExec(
                aqe.inputPlan,
                aqe.context,
                aqe.preprocessingRules,
                aqe.isSubquery,
                supportsColumnar = true
              ))))
      case other =>
        command.withNewChildren(
          Array(BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToCarrierRow(other)))
    }
  }

  case class NativeWritePostRule(session: SparkSession) extends Rule[SparkPlan] {

    override def apply(p: SparkPlan): SparkPlan = p match {
      case rc @ DataWritingCommandExec(cmd, child) =>
        // The same thread can set these properties in the last query submission.
        val format =
          if (BackendsApiManager.getSettings.enableNativeWriteFiles()) {
            getNativeFormat(cmd)
          } else {
            None
          }
        val numStaticPartitions: Option[Int] = cmd match {
          case cmd: InsertIntoHadoopFsRelationCommand =>
            Some(cmd.staticPartitions.size)
          case _ =>
            None
        }
        injectSparkLocalProperty(session, format, numStaticPartitions)
        format match {
          case Some(_) =>
            injectFakeRowAdaptor(rc, child)
          case None =>
            rc.withNewChildren(rc.children.map(apply))
        }

      case plan: SparkPlan => plan.withNewChildren(plan.children.map(apply))
    }
  }

  // TODO: This makes FileFormatWriter#write caller-sensitive.
  //  Remove this workaround once we have a better solution.
  def injectSparkLocalProperty(
      spark: SparkSession,
      format: Option[String],
      numStaticPartitions: Option[Int]): Unit = {
    if (format.isDefined) {
      spark.sparkContext.setLocalProperty("isNativeApplicable", true.toString)
      spark.sparkContext.setLocalProperty("nativeFormat", format.get)
      // The flag for static-only write is not used by Velox backend where
      // the static partition write is already supported.
      spark.sparkContext.setLocalProperty(
        "staticPartitionWriteOnly",
        BackendsApiManager.getSettings.staticPartitionWriteOnly().toString)
      spark.sparkContext.setLocalProperty(
        "numStaticPartitionCols",
        numStaticPartitions.getOrElse(0).toString)
    } else {
      spark.sparkContext.setLocalProperty("isNativeApplicable", null)
      spark.sparkContext.setLocalProperty("nativeFormat", null)
      spark.sparkContext.setLocalProperty("staticPartitionWriteOnly", null)
      spark.sparkContext.setLocalProperty("numStaticPartitionCols", null)
    }
  }
}

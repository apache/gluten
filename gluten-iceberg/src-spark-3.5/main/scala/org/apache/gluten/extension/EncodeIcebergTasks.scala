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

import org.apache.gluten.config.GlutenIcebergConfig
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{InputFileBlockLength, InputFileBlockStart, InputFileName, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Command, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.datasources.v2.{CompactIcebergStrategy, DataSourceV2Relation, DeferredIcebergScanExec}

import org.apache.iceberg.spark.source.{CompactIcebergTable, SparkTable}

case class EncodeIcebergTasks(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !GlutenIcebergConfig.get.enableNativeRead ||
      !GlutenIcebergConfig.get.getConf(GlutenIcebergConfig.EARLY_TASK_ENCODING_ENABLED)
    ) {
      return plan
    }
    if (
      plan.exists {
        case _: Join | _: Command => true
        case node => node.expressions.exists(_.exists {
            case _: SubqueryExpression | _: InputFileName | _: InputFileBlockStart |
                _: InputFileBlockLength => true
            case _ => false
          })
      }
    ) {
      return plan
    }
    plan.transformDown {
      case relation: DataSourceV2Relation
          if !relation.isStreaming && relation.table.getClass == classOf[SparkTable] =>
        val table = relation.table.asInstanceOf[SparkTable]
        if (table.branch() == null) {
          relation.copy(table = new CompactIcebergTable(spark, table))
        } else {
          relation
        }
    }
  }
}

object EncodeIcebergTasks {
  def inject(injector: Injector): Unit = {
    injector.spark.injectPreCBORule(EncodeIcebergTasks.apply)
    injector.spark.injectPlannerStrategy(_ => CompactIcebergStrategy)
    injector.gluten.legacy.injectPre(
      _ =>
        plan =>
          plan.transformDown {
            case deferred: DeferredIcebergScanExec =>
              ProjectExec(deferred.output, deferred.scanExec)
          })
  }
}

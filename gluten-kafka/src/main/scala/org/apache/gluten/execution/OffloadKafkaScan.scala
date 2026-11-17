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
package org.apache.gluten.execution

import org.apache.gluten.config.{ConfigEntry, ConfigRegistry, GlutenConfig}
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.internal.SQLConf

case class OffloadKafkaScan(enabled: Boolean =
  OffloadKafkaScan.get.getConf(OffloadKafkaScan.ENABLED))
  extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: MicroBatchScanExec
        if enabled && MicroBatchScanExecTransformer.supportsBatchScan(scan.scan) =>
      MicroBatchScanExecTransformer(scan)
    case other => other
  }
}

object OffloadKafkaScan extends ConfigRegistry {
  override def get: GlutenConfig = new GlutenConfig(SQLConf.get)

  val ENABLED: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.kafkaScan")
      .doc("Enable native Kafka micro-batch scans. When disabled, Kafka scans use Spark while " +
        "downstream operators remain eligible for native execution.")
      .booleanConf
      .createWithDefault(true)

  def inject(injector: Injector): Unit = {
    // Inject legacy rule.
    injector.gluten.legacy.injectTransform {
      c =>
        val conf = new GlutenConfig(c.sqlConf)
        val offload = Seq(OffloadKafkaScan(conf.getConf(ENABLED)))
        HeuristicTransform.Simple(
          Validators.newValidator(conf, offload),
          offload
        )
    }
  }
}
